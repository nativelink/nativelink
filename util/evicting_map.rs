// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::ops::DerefMut;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fast_async_mutex::mutex::Mutex;
use lru::LruCache;
use serde::{Deserialize, Serialize};

use common::{DigestInfo, SerializableDigestInfo};
use config::backends::EvictionPolicy;

type LastTouchSinceAnchor = u32;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SerializedLRU {
    pub data: Vec<(SerializableDigestInfo, LastTouchSinceAnchor)>,
    pub anchor_time: u64,
}

/// Wrapper used to abstract away which underlying Instant impl we are using.
/// This is needed for testing.
pub trait InstantWrapper {
    fn from_secs(secs: u64) -> Self;
    fn unix_timestamp(&self) -> u64;
    fn elapsed(&self) -> Duration;
}

impl InstantWrapper for SystemTime {
    fn from_secs(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(secs)).unwrap()
    }

    fn unix_timestamp(&self) -> u64 {
        self.duration_since(UNIX_EPOCH).unwrap().as_secs()
    }

    fn elapsed(&self) -> Duration {
        <SystemTime>::elapsed(self).unwrap()
    }
}

struct EvictionItem<T: LenEntry> {
    seconds_since_anchor: u32,
    data: T,
}

pub trait LenEntry {
    // Length of referenced data.
    fn len(&self) -> usize;

    // This will be called when object is removed from map.
    // Note: There may still be a reference to it held somewhere else, which
    // is why it can't be mutable. This is a good place to mark the item
    // to be deleted and then in the Drop call actually do the deleting.
    // This will ensure nowhere else in the program still holds a reference
    // to this object.
    // Note: You should not rely only on the Drop trait. Doing so might
    // result in the program safely shutting down and calling the Drop method
    // on each object, which if you are deleting items you may not want to do.
    fn unref(&self) {}
}

impl<T: AsRef<[u8]>> LenEntry for T {
    fn len(&self) -> usize {
        <[u8]>::len(self.as_ref())
    }
}

struct State<T: LenEntry + Clone> {
    lru: LruCache<DigestInfo, EvictionItem<T>>,
    sum_store_size: u64,
}

pub struct EvictingMap<T: LenEntry + Clone, I: InstantWrapper> {
    state: Mutex<State<T>>,
    anchor_time: I,
    max_bytes: u64,
    max_seconds: u32,
    max_count: u64,
}

impl<T: LenEntry + Clone, I: InstantWrapper> EvictingMap<T, I> {
    pub fn new(config: &EvictionPolicy, anchor_time: I) -> Self {
        EvictingMap {
            // We use unbounded because if we use the bounded version we can't call the delete
            // function on the LenEntry properly.
            state: Mutex::new(State {
                lru: LruCache::unbounded(),
                sum_store_size: 0,
            }),
            anchor_time,
            max_bytes: config.max_bytes as u64,
            max_seconds: config.max_seconds,
            max_count: config.max_count,
        }
    }

    pub async fn build_lru_index(&self) -> SerializedLRU {
        let state = self.state.lock().await;
        let mut serialized_lru = SerializedLRU {
            data: Vec::with_capacity(state.lru.len()),
            anchor_time: self.anchor_time.unix_timestamp(),
        };
        for (digest, eviction_item) in state.lru.iter() {
            let serialized_digest = SerializableDigestInfo {
                hash: digest.packed_hash,
                size_bytes: digest.size_bytes as u64,
            };
            serialized_lru
                .data
                .push((serialized_digest, eviction_item.seconds_since_anchor));
        }
        serialized_lru
    }

    pub async fn restore_lru(&mut self, seiralized_lru: SerializedLRU, entry_builder: impl Fn(&DigestInfo) -> T) {
        let mut state = self.state.lock().await;
        self.anchor_time = I::from_secs(seiralized_lru.anchor_time);
        state.lru.clear();
        for (digest, seconds_since_anchor) in seiralized_lru.data {
            let digest: DigestInfo = digest.into();
            let entry = entry_builder(&digest);
            state.lru.put(
                digest,
                EvictionItem {
                    seconds_since_anchor,
                    data: entry,
                },
            );
        }
        // Just in case we allow for some cleanup (eg: old items).
        self.evict_items(state.deref_mut());
    }

    fn should_evict(&self, lru_len: usize, peek_entry: &EvictionItem<T>, sum_store_size: u64) -> bool {
        let is_over_size = self.max_bytes != 0 && sum_store_size >= self.max_bytes;

        let evict_before_seconds =
            (self.anchor_time.elapsed().as_secs() as u32).max(self.max_seconds) - self.max_seconds;
        let old_item_exists = self.max_seconds != 0 && peek_entry.seconds_since_anchor < evict_before_seconds;

        let is_over_count = self.max_count != 0 && (lru_len as u64) > self.max_count;

        if is_over_size || old_item_exists || is_over_count {
            return true;
        }
        false
    }

    fn evict_items(&self, state: &mut State<T>) {
        let mut peek_entry = if let Some((_, entry)) = state.lru.peek_lru() {
            entry
        } else {
            return;
        };
        while self.should_evict(state.lru.len(), peek_entry, state.sum_store_size) {
            let (_, eviction_item) = state.lru.pop_lru().expect("Tried to peek() then pop() but failed");
            state.sum_store_size -= eviction_item.data.len() as u64;
            eviction_item.data.unref();

            peek_entry = if let Some((_, entry)) = state.lru.peek_lru() {
                entry
            } else {
                return;
            };
        }
    }

    pub async fn size_for_key(&self, digest: &DigestInfo) -> Option<usize> {
        let mut state = self.state.lock().await;
        if let Some(mut entry) = state.lru.get_mut(digest) {
            entry.seconds_since_anchor = self.anchor_time.elapsed().as_secs() as u32;
            return Some(entry.data.len());
        }
        None
    }

    pub async fn get(&self, digest: &DigestInfo) -> Option<T> {
        let mut state = self.state.lock().await;
        if let Some(mut entry) = state.lru.get_mut(digest) {
            entry.seconds_since_anchor = self.anchor_time.elapsed().as_secs() as u32;
            return Some(entry.data.clone());
        }
        None
    }

    pub async fn insert(&self, digest: DigestInfo, data: T) {
        let new_item_size = data.len() as u64;
        let eviction_item = EvictionItem {
            seconds_since_anchor: self.anchor_time.elapsed().as_secs() as u32,
            data,
        };
        let mut state = self.state.lock().await;
        if let Some(old_item) = state.lru.put(digest.into(), eviction_item) {
            state.sum_store_size -= old_item.data.len() as u64;
            old_item.data.unref();
        }
        state.sum_store_size += new_item_size;
        self.evict_items(state.deref_mut());
    }

    pub async fn remove(&self, digest: &DigestInfo) -> bool {
        let mut state = self.state.lock().await;
        if let Some(entry) = state.lru.pop(digest) {
            state.sum_store_size -= entry.data.len() as u64;
            drop(state);
            entry.data.unref();
            return true;
        }
        false
    }
}
