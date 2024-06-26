// Copyright 2023 The NativeLink Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Borrow;
use std::cmp::Eq;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::{DerefMut, RangeBounds};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_lock::Mutex;
use async_trait::async_trait;
use lru::LruCache;
use nativelink_config::stores::EvictionPolicy;
use serde::{Deserialize, Serialize};
use tracing::{event, Level};

use crate::metrics_utils::{CollectorState, Counter, CounterWithTime, MetricsComponent};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SerializedLRU<K> {
    pub data: Vec<(K, i32)>,
    pub anchor_time: u64,
}

/// Wrapper used to abstract away which underlying Instant impl we are using.
/// This is needed for testing.
pub trait InstantWrapper: 'static {
    fn from_secs(secs: u64) -> Self;
    fn unix_timestamp(&self) -> u64;
    fn elapsed(&self) -> Duration;
}

impl InstantWrapper for SystemTime {
    fn from_secs(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_secs(secs))
            .unwrap()
    }

    fn unix_timestamp(&self) -> u64 {
        self.duration_since(UNIX_EPOCH).unwrap().as_secs()
    }

    fn elapsed(&self) -> Duration {
        <SystemTime>::elapsed(self).unwrap()
    }
}

#[derive(Debug)]
struct EvictionItem<T: LenEntry + Debug> {
    seconds_since_anchor: i32,
    data: T,
}

#[async_trait]
impl<T: LenEntry + Debug> LenEntry for EvictionItem<T> {
    #[inline]
    fn len(&self) -> usize {
        return self.data.len();
    }

    #[inline]
    fn is_empty(&self) -> bool {
        return self.data.is_empty();
    }

    #[inline]
    async fn unref(&self) {
        std::mem::drop(&self.data);
    }
}

#[async_trait]
pub trait LenEntry: 'static + Send + Sync {
    /// Length of referenced data.
    fn len(&self) -> usize;

    /// Returns `true` if `self` has zero length.
    fn is_empty(&self) -> bool;

    /// Called when an entry is touched.  On failure, will remove the entry
    /// from the map.
    #[inline]
    async fn touch(&self) -> bool {
        true
    }

    /// This will be called when object is removed from map.
    /// Note: There may still be a reference to it held somewhere else, which
    /// is why it can't be mutable. This is a good place to mark the item
    /// to be deleted and then in the Drop call actually do the deleting.
    /// This will ensure nowhere else in the program still holds a reference
    /// to this object.
    /// You should not rely only on the Drop trait. Doing so might result in the
    /// program safely shutting down and calling the Drop method on each object,
    /// which if you are deleting items you may not want to do.
    /// It is undefined behavior to have `unref()` called more than once.
    /// During the execution of `unref()` no items can be added or removed to/from
    /// the EvictionMap globally (including inside `unref()`).
    #[inline]
    async fn unref(&self) {}
}

#[async_trait]
impl<T: LenEntry + Send + Sync> LenEntry for Arc<T> {
    #[inline]
    fn len(&self) -> usize {
        T::len(self.as_ref())
    }

    #[inline]
    fn is_empty(&self) -> bool {
        T::is_empty(self.as_ref())
    }

    #[inline]
    async fn touch(&self) -> bool {
        self.as_ref().touch().await
    }

    #[inline]
    async fn unref(&self) {
        self.as_ref().unref().await;
    }
}

struct State<K: Ord + Hash + Eq + Clone + Debug, T: LenEntry + Debug> {
    lru: LruCache<K, EvictionItem<T>>,
    btree: Option<BTreeSet<K>>,
    sum_store_size: u64,

    // Metrics.
    evicted_bytes: Counter,
    evicted_items: CounterWithTime,
    replaced_bytes: Counter,
    replaced_items: CounterWithTime,
    removed_bytes: Counter,
    removed_items: CounterWithTime,
    lifetime_inserted_bytes: Counter,
}

impl<K: Ord + Hash + Eq + Clone + Debug, T: LenEntry + Debug + Sync> State<K, T> {
    /// Removes an item from the cache.
    async fn remove<Q>(&mut self, key: &Q, eviction_item: &EvictionItem<T>, replaced: bool)
    where
        K: Borrow<Q>,
        Q: Ord + Hash + Eq + Debug,
    {
        if let Some(btree) = &mut self.btree {
            btree.remove(key.borrow());
        }
        self.sum_store_size -= eviction_item.data.len() as u64;
        if replaced {
            self.replaced_items.inc();
            self.replaced_bytes.add(eviction_item.data.len() as u64);
        } else {
            self.evicted_items.inc();
            self.evicted_bytes.add(eviction_item.data.len() as u64);
        }
        // Note: See comment in `unref()` requring global lock of insert/remove.
        eviction_item.data.unref().await;
    }

    /// Inserts a new item into the cache. If the key already exists, the old item is returned.
    async fn put(&mut self, key: K, eviction_item: EvictionItem<T>) -> Option<T> {
        // If we are maintaining a btree index, we need to update it.
        if let Some(btree) = &mut self.btree {
            btree.insert(key.clone());
        }
        if let Some(old_item) = self.lru.put(key.clone(), eviction_item) {
            self.remove(&key, &old_item, true).await;
            return Some(old_item.data);
        }
        None
    }
}

pub struct EvictingMap<K: Ord + Hash + Eq + Clone + Debug, T: LenEntry + Debug, I: InstantWrapper> {
    state: Mutex<State<K, T>>,
    anchor_time: I,
    max_bytes: u64,
    evict_bytes: u64,
    max_seconds: i32,
    max_count: u64,
}

impl<K, T, I> EvictingMap<K, T, I>
where
    K: Ord + Hash + Eq + Clone + Debug,
    T: LenEntry + Debug + Clone + Send + Sync,
    I: InstantWrapper,
{
    pub fn new(config: &EvictionPolicy, anchor_time: I) -> Self {
        EvictingMap {
            // We use unbounded because if we use the bounded version we can't call the delete
            // function on the LenEntry properly.
            state: Mutex::new(State {
                lru: LruCache::unbounded(),
                btree: None,
                sum_store_size: 0,
                evicted_bytes: Counter::default(),
                evicted_items: CounterWithTime::default(),
                replaced_bytes: Counter::default(),
                replaced_items: CounterWithTime::default(),
                removed_bytes: Counter::default(),
                removed_items: CounterWithTime::default(),
                lifetime_inserted_bytes: Counter::default(),
            }),
            anchor_time,
            max_bytes: config.max_bytes as u64,
            evict_bytes: config.evict_bytes as u64,
            max_seconds: config.max_seconds as i32,
            max_count: config.max_count,
        }
    }

    pub async fn enable_filtering(&self) {
        let mut state = self.state.lock().await;
        if state.btree.is_none() {
            Self::rebuild_btree_index(&mut state);
        }
    }

    fn rebuild_btree_index(state: &mut State<K, T>) {
        state.btree = Some(state.lru.iter().map(|(k, _)| k).cloned().collect());
    }

    /// Run the `handler` function on each key-value pair that matches the `prefix_range`
    /// and return the number of items that were processed.
    /// The `handler` function should return `true` to continue processing the next item
    /// or `false` to stop processing.
    pub async fn range<F, Q>(&self, prefix_range: impl RangeBounds<Q>, mut handler: F) -> usize
    where
        F: FnMut(&K, &T) -> bool,
        K: Borrow<Q> + Ord,
        Q: Ord + Hash + Eq + Debug,
    {
        let mut state = self.state.lock().await;
        let btree = match state.btree {
            Some(ref btree) => btree,
            None => {
                Self::rebuild_btree_index(&mut state);
                state.btree.as_ref().unwrap()
            }
        };
        let mut continue_count = 0;
        for key in btree.range(prefix_range) {
            let value = &state.lru.peek(key.borrow()).unwrap().data;
            let should_continue = handler(key, value);
            if !should_continue {
                break;
            }
            continue_count += 1;
        }
        continue_count
    }

    /// Returns the number of key-value pairs that are currently in the the cache.
    /// Function is not for production code paths.
    pub async fn len_for_test(&self) -> usize {
        self.state.lock().await.lru.len()
    }

    fn should_evict(
        &self,
        lru_len: usize,
        peek_entry: &EvictionItem<T>,
        sum_store_size: u64,
        max_bytes: u64,
    ) -> bool {
        let is_over_size = max_bytes != 0 && sum_store_size >= max_bytes;

        let evict_older_than_seconds =
            (self.anchor_time.elapsed().as_secs() as i32) - self.max_seconds;
        let old_item_exists =
            self.max_seconds != 0 && peek_entry.seconds_since_anchor < evict_older_than_seconds;

        let is_over_count = self.max_count != 0 && (lru_len as u64) > self.max_count;

        is_over_size || old_item_exists || is_over_count
    }

    async fn evict_items(&self, state: &mut State<K, T>) {
        let Some((_, mut peek_entry)) = state.lru.peek_lru() else {
            return;
        };

        let max_bytes = if self.max_bytes != 0
            && self.evict_bytes != 0
            && self.should_evict(
                state.lru.len(),
                peek_entry,
                state.sum_store_size,
                self.max_bytes,
            ) {
            if self.max_bytes > self.evict_bytes {
                self.max_bytes - self.evict_bytes
            } else {
                0
            }
        } else {
            self.max_bytes
        };

        while self.should_evict(state.lru.len(), peek_entry, state.sum_store_size, max_bytes) {
            let (key, eviction_item) = state
                .lru
                .pop_lru()
                .expect("Tried to peek() then pop() but failed");
            print!("Evicting item -- ");
            event!(Level::INFO, ?key, "Evicting",);
            state.remove(&key, &eviction_item, false).await;

            peek_entry = if let Some((_, entry)) = state.lru.peek_lru() {
                entry
            } else {
                return;
            };
        }
    }

    /// Return the size of a `key`, if not found `None` is returned.
    pub async fn size_for_key<Q>(&self, key: &Q) -> Option<usize>
    where
        K: Borrow<Q>,
        Q: Ord + Hash + Eq + Debug,
    {
        let mut results = [None];
        self.sizes_for_keys([key], &mut results[..]).await;
        results[0]
    }

    /// Return the sizes of a collection of `keys`. Expects `results` collection
    /// to be provided for storing the resulting key sizes. Each index value in
    /// `keys` maps directly to the size value for the key in `results`.
    /// If no key is found in the internal map, `None` is filled in its place.
    pub async fn sizes_for_keys<It, Q, R>(&self, keys: It, results: &mut [Option<usize>])
    where
        It: IntoIterator<Item = R>,
        // This may look strange, but what we are doing is saying:
        // * `K` must be able to borrow `Q`
        // * `R` (the input stream item type) must also be able to borrow `Q`
        // Note: That K and R do not need to be the same type, they just both need
        // to be able to borrow a `Q`.
        K: Borrow<Q>,
        R: Borrow<Q>,
        Q: Ord + Hash + Eq + Debug,
    {
        let mut state = self.state.lock().await;

        let lru_len = state.lru.len();
        for (key, result) in keys.into_iter().zip(results.iter_mut()) {
            match state.lru.get(key.borrow()) {
                Some(entry) => {
                    // Since we are not inserting anythign we don't need to evict based
                    // on the size of the store.
                    // Note: We need to check eviction because the item might be expired
                    // based on the current time. In such case, we remove the item while
                    // we are here.
                    let should_evict = self.should_evict(lru_len, entry, 0, u64::MAX);
                    if !should_evict && entry.data.touch().await {
                        *result = Some(entry.data.len());
                    } else {
                        *result = None;
                        if let Some((key, eviction_item)) = state.lru.pop_entry(key.borrow()) {
                            if should_evict {
                                event!(Level::INFO, ?key, "Item expired, evicting");
                            } else {
                                event!(Level::INFO, ?key, "Touch failed, evicting");
                            }
                            state.remove(key.borrow(), &eviction_item, false).await;
                        }
                    }
                }
                None => *result = None,
            }
        }
    }

    pub async fn get<Q>(&self, key: &Q) -> Option<T>
    where
        K: Borrow<Q>,
        Q: Ord + Hash + Eq + Debug,
    {
        let mut state = self.state.lock().await;
        self.evict_items(state.deref_mut()).await;

        let entry = state.lru.get_mut(key.borrow())?;

        if entry.data.touch().await {
            return Some(entry.data.clone());
        }

        let (key, eviction_item) = state.lru.pop_entry(key.borrow())?;
        event!(Level::INFO, ?key, "Touch failed, evicting");
        state.remove(key.borrow(), &eviction_item, false).await;
        None
    }

    /// Returns the replaced item if any.
    pub async fn insert(&self, key: K, data: T) -> Option<T> {
        self.insert_with_time(key, data, self.anchor_time.elapsed().as_secs() as i32)
            .await
    }

    /// Returns the replaced item if any.
    pub async fn insert_with_time(&self, key: K, data: T, seconds_since_anchor: i32) -> Option<T> {
        let mut state = self.state.lock().await;
        let results = self
            .inner_insert_many(&mut state, [(key, data)], seconds_since_anchor)
            .await;
        results.into_iter().next()
    }

    /// Same as insert(), but optimized for multiple inserts.
    /// Returns the replaced items if any.
    pub async fn insert_many(&self, inserts: impl IntoIterator<Item = (K, T)>) -> Vec<T> {
        let mut inserts = inserts.into_iter().peekable();
        // Shortcut for cases where there are no inserts, so we don't need to lock.
        if inserts.peek().is_none() {
            return Vec::new();
        }
        let state = &mut self.state.lock().await;
        self.inner_insert_many(state, inserts, self.anchor_time.elapsed().as_secs() as i32)
            .await
    }

    async fn inner_insert_many(
        &self,
        mut state: &mut State<K, T>,
        inserts: impl IntoIterator<Item = (K, T)>,
        seconds_since_anchor: i32,
    ) -> Vec<T> {
        let mut replaced_items = Vec::new();
        for (key, data) in inserts.into_iter() {
            let new_item_size = data.len() as u64;
            let eviction_item = EvictionItem {
                seconds_since_anchor,
                data,
            };

            if let Some(old_item) = state.put(key, eviction_item).await {
                replaced_items.push(old_item);
            }
            state.sum_store_size += new_item_size;
            state.lifetime_inserted_bytes.add(new_item_size);
            self.evict_items(state.deref_mut()).await;
        }
        replaced_items
    }

    pub async fn remove<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + Hash + Eq + Debug,
    {
        let mut state = self.state.lock().await;
        self.inner_remove(&mut state, key).await
    }

    async fn inner_remove<Q>(&self, mut state: &mut State<K, T>, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + Hash + Eq + Debug,
    {
        self.evict_items(state.deref_mut()).await;
        if let Some(entry) = state.lru.pop(key.borrow()) {
            state.remove(key, &entry, false).await;
            return true;
        }
        false
    }

    /// Same as remove(), but allows for a conditional to be applied to the entry before removal
    /// in an atomic fashion.
    pub async fn remove_if<Q, F: FnOnce(&T) -> bool>(&self, key: &Q, cond: F) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + Hash + Eq + Debug,
    {
        let mut state = self.state.lock().await;
        if let Some(entry) = state.lru.get(key.borrow()) {
            if !cond(&entry.data) {
                return false;
            }
            return self.inner_remove(&mut state, key).await;
        }
        false
    }
}

impl<K: Ord + Hash + Eq + Clone + Debug, T: LenEntry + Debug, I: InstantWrapper> MetricsComponent
    for EvictingMap<K, T, I>
{
    fn gather_metrics(&self, c: &mut CollectorState) {
        c.publish(
            "max_bytes",
            &self.max_bytes,
            "Maximum size of the store in bytes",
        );
        c.publish(
            "evict_bytes",
            &self.evict_bytes,
            "Number of bytes to evict when the store is full",
        );
        c.publish(
            "anchor_time_timestamp",
            &self.anchor_time.unix_timestamp(),
            "Anchor time for the store",
        );
        c.publish(
            "max_seconds",
            &self.max_seconds,
            "Maximum number of seconds to keep an item in the store",
        );
        c.publish(
            "max_count",
            &self.max_count,
            "Maximum number of items to keep in the store",
        );
        futures::executor::block_on(async move {
            let state = self.state.lock().await;
            c.publish(
                "sum_store_size_bytes",
                &state.sum_store_size,
                "Total size of all items in the store",
            );
            c.publish(
                "items_in_store_total",
                &state.lru.len(),
                "Number of items in the store",
            );
            c.publish(
                "oldest_item_timestamp",
                &state
                    .lru
                    .peek_lru()
                    .map(|(_, v)| {
                        self.anchor_time.unix_timestamp() as i64 - v.seconds_since_anchor as i64
                    })
                    .unwrap_or(-1),
                "Timestamp of the oldest item in the store",
            );
            c.publish(
                "newest_item_timestamp",
                &state
                    .lru
                    .iter()
                    .next()
                    .map(|(_, v)| {
                        self.anchor_time.unix_timestamp() as i64 - v.seconds_since_anchor as i64
                    })
                    .unwrap_or(-1),
                "Timestamp of the newest item in the store",
            );
            c.publish(
                "evicted_items_total",
                &state.evicted_items,
                "Number of items evicted from the store",
            );
            c.publish(
                "evicted_bytes",
                &state.evicted_bytes,
                "Number of bytes evicted from the store",
            );
            c.publish(
                "lifetime_inserted_bytes",
                &state.lifetime_inserted_bytes,
                "Number of bytes inserted into the store since it was created",
            );
            c.publish(
                "replaced_bytes",
                &state.replaced_bytes,
                "Number of bytes replaced in the store",
            );
            c.publish(
                "replaced_items_total",
                &state.replaced_items,
                "Number of items replaced in the store",
            );
            c.publish(
                "removed_bytes",
                &state.removed_bytes,
                "Number of bytes explicitly removed from the store",
            );
            c.publish(
                "removed_items_total",
                &state.removed_items,
                "Number of items explicitly removed from the store",
            );
            c.publish_stats(
                "item_size_bytes",
                state.lru.iter().take(1_000_000).map(|(_, v)| v.data.len()),
                "Stats about the first 1_000_000 items in the store (these are newest items in the store)",
            );
        });
    }
}
