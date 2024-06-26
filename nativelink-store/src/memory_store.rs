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

use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Bound;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use nativelink_error::{make_err, Code, Error, ResultExt};
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::evicting_map::{EvictingMap, LenEntry};
use nativelink_util::health_utils::{default_health_status_indicator, HealthStatusIndicator};
use nativelink_util::metrics_utils::{Collector, CollectorState, MetricsComponent, Registry};
use nativelink_util::store_trait::{
    StoreDriver, StoreKey, StoreOptimizations, StoreSubscription, StoreSubscriptionItem,
    UploadSizeInfo,
};
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use tokio::sync::watch;
use tracing::{event, Level};

use crate::cas_utils::is_zero_digest;

#[derive(Clone)]
pub struct BytesWrapper(Bytes);

impl BytesWrapper {
    pub fn from_string(s: &str) -> Self {
        BytesWrapper(Bytes::copy_from_slice(s.as_bytes()))
    }
}

impl Debug for BytesWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BytesWrapper { -- Binary data -- }")
    }
}

impl Drop for BytesWrapper {
    fn drop(&mut self) {
        print!(
            "BytesWrapper dropped {}\n",
            std::str::from_utf8(&self.0).unwrap()
        );
        std::mem::drop(&self.0);
    }
}

impl LenEntry for BytesWrapper {
    #[inline]
    fn len(&self) -> usize {
        Bytes::len(&self.0)
    }

    #[inline]
    fn is_empty(&self) -> bool {
        Bytes::is_empty(&self.0)
    }
}

/// A subscription to a key in the MemoryStore.
struct MemoryStoreSubscription {
    store: Weak<MemoryStore>,
    key: StoreKey<'static>,
    receiver: Option<watch::Receiver<Result<Arc<dyn StoreSubscriptionItem>, Error>>>,
}

#[async_trait]
impl StoreSubscription for MemoryStoreSubscription {
    fn peek(&self) -> Result<Arc<dyn StoreSubscriptionItem>, Error> {
        self.receiver.as_ref().unwrap().borrow().clone()
    }

    async fn changed(&mut self) -> Result<Arc<dyn StoreSubscriptionItem>, Error> {
        self.receiver
            .as_mut()
            .unwrap()
            .changed()
            .await
            .map_err(|e| {
                make_err!(
                    Code::Internal,
                    "Sender dropped in DefaultStoreSubscription::changed - {e:?}"
                )
            })?;
        self.receiver.as_ref().unwrap().borrow().clone()
    }
}

/// When the subscription is dropped, we need to remove the subscription from the store
/// to prevent memory leaks.
impl Drop for MemoryStoreSubscription {
    fn drop(&mut self) {
        // Make sure we manually drop receiver first.
        self.receiver = None;
        let Some(store) = self.store.upgrade() else {
            return;
        };
        store.remove_dropped_subscription(self.key.borrow().into_owned());
    }
}

struct MemoryStoreSubscriptionItem {
    store: Weak<MemoryStore>,
    key: StoreKey<'static>,
}

#[async_trait]
impl StoreSubscriptionItem for MemoryStoreSubscriptionItem {
    async fn get_key<'a>(&'a self) -> Result<StoreKey<'a>, Error> {
        Ok(self.key.borrow())
    }

    async fn get_part(
        &self,
        writer: &mut DropCloserWriteHalf,
        offset: usize,
        length: Option<usize>,
    ) -> Result<(), Error> {
        let store = self
            .store
            .upgrade()
            .err_tip(|| "Store dropped in MemoryStoreSubscriptionItem::get_part")?;
        Pin::new(store.as_ref())
            .get_part(self.key.borrow(), writer, offset, length)
            .await
    }
}

type SubscriptionSender = watch::Sender<Result<Arc<dyn StoreSubscriptionItem>, Error>>;
pub struct MemoryStore {
    weak_self: Weak<Self>,
    evicting_map: EvictingMap<StoreKey<'static>, BytesWrapper, SystemTime>,
    subscriptions: RwLock<HashMap<StoreKey<'static>, SubscriptionSender>>,
}

impl MemoryStore {
    pub fn new(config: &nativelink_config::stores::MemoryStore) -> Arc<Self> {
        let empty_policy = nativelink_config::stores::EvictionPolicy::default();
        let eviction_policy = config.eviction_policy.as_ref().unwrap_or(&empty_policy);
        Arc::new_cyclic(|weak_self| MemoryStore {
            weak_self: weak_self.clone(),
            evicting_map: EvictingMap::new(eviction_policy, SystemTime::now()),
            subscriptions: RwLock::new(HashMap::new()),
        })
    }

    /// Returns the number of key-value pairs that are currently in the the cache.
    /// Function is not for production code paths.
    pub async fn len_for_test(&self) -> usize {
        self.evicting_map.len_for_test().await
    }

    pub async fn remove_entry(&self, key: StoreKey<'_>) -> bool {
        self.evicting_map.remove(&key.into_owned()).await
    }

    pub async fn insert(&self, key: StoreKey<'static>, data: BytesWrapper) -> Option<BytesWrapper> {
        return self.evicting_map.insert(key, data).await;
    }

    /// Tells the store that a subscription has been dropped and gives an opportunity to clean up.
    fn remove_dropped_subscription(&self, key: StoreKey<'static>) {
        let mut subscriptions = self.subscriptions.write();
        let Some(sender) = subscriptions.get(&key) else {
            event!(
                Level::ERROR,
                "Subscription dropped for key '{key:?}' but not found in subscriptions"
            );
            return;
        };
        if sender.receiver_count() == 0 {
            subscriptions.remove(&key);
        }
    }
}

#[async_trait]
impl StoreDriver for MemoryStore {
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<usize>],
    ) -> Result<(), Error> {
        // TODO(allada): This is a dirty hack to get around the lifetime issues with the
        // evicting map.
        let digests: Vec<_> = keys.iter().map(|key| key.borrow().into_owned()).collect();
        self.evicting_map.sizes_for_keys(digests, results).await;
        // We need to do a special pass to ensure our zero digest exist.
        keys.iter()
            .zip(results.iter_mut())
            .for_each(|(key, result)| {
                if is_zero_digest(key.borrow()) {
                    *result = Some(0);
                }
            });
        Ok(())
    }

    async fn list(
        self: Pin<&Self>,
        range: (Bound<StoreKey<'_>>, Bound<StoreKey<'_>>),
        handler: &mut (dyn for<'a> FnMut(&'a StoreKey) -> bool + Send + Sync + '_),
    ) -> Result<usize, Error> {
        let range = (
            range.0.map(|v| v.into_owned()),
            range.1.map(|v| v.into_owned()),
        );
        let iterations = self
            .evicting_map
            .range(range, move |key, _value| handler(key))
            .await;
        Ok(iterations)
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        // Internally Bytes might hold a reference to more data than just our data. To prevent
        // this potential case, we make a full copy of our data for long-term storage.
        let final_buffer = {
            let buffer = reader
                .consume(None)
                .await
                .err_tip(|| "Failed to collect all bytes from reader in memory_store::update")?;
            let mut new_buffer = BytesMut::with_capacity(buffer.len());
            new_buffer.extend_from_slice(&buffer[..]);
            new_buffer.freeze()
        };

        self.evicting_map
            .insert(key.borrow().into_owned(), BytesWrapper(final_buffer))
            .await;
        {
            // Notify all subscribers that the key has been modified.
            let subscription_lock = self.subscriptions.read();
            if let Some(sender) = subscription_lock.get(&key.into_owned()) {
                sender.send_modify(|_| { /* We just need to flag it modified. */ });
            }
        }
        Ok(())
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: usize,
        length: Option<usize>,
    ) -> Result<(), Error> {
        if is_zero_digest(key.borrow()) {
            writer
                .send_eof()
                .err_tip(|| "Failed to send zero EOF in filesystem store get_part")?;
            return Ok(());
        }

        let value = self
            .evicting_map
            .get(&key.borrow().into_owned())
            .await
            .err_tip_with_code(|_| (Code::NotFound, format!("Key {key:?} not found")))?;
        let default_len = value.len() - offset;
        let length = length.unwrap_or(default_len).min(default_len);
        if length > 0 {
            writer
                .send(value.0.slice(offset..(offset + length)))
                .await
                .err_tip(|| "Failed to write data in memory store")?;
        }
        writer
            .send_eof()
            .err_tip(|| "Failed to write EOF in memory store get_part")?;
        Ok(())
    }

    fn optimized_for(&self, optimization: StoreOptimizations) -> bool {
        optimization == StoreOptimizations::SubscribeChanges
    }

    async fn subscribe(self: Arc<Self>, key: StoreKey<'_>) -> Box<dyn StoreSubscription> {
        let key = key.into_owned();
        let subscription_lock = self.subscriptions.upgradable_read();
        if let Some(watch_sender) = subscription_lock.get(&key) {
            let mut rx = watch_sender.subscribe();
            rx.mark_changed();
            return Box::new(MemoryStoreSubscription {
                store: self.weak_self.clone(),
                key: key.borrow().into_owned(),
                receiver: Some(rx),
            });
        }
        {
            let mut subscription_lock = RwLockUpgradableReadGuard::upgrade(subscription_lock);
            let subscription_item: Arc<dyn StoreSubscriptionItem> =
                Arc::new(MemoryStoreSubscriptionItem {
                    store: self.weak_self.clone(),
                    key: key.borrow().into_owned(),
                });
            let (tx, mut rx) = watch::channel(Ok(subscription_item));
            subscription_lock.insert(key.borrow().into_owned(), tx.clone());
            rx.mark_changed();
            Box::new(MemoryStoreSubscription {
                store: self.weak_self.clone(),
                key,
                receiver: Some(rx),
            })
        }
    }

    fn inner_store(&self, _digest: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any<'a>(&'a self) -> &'a (dyn std::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_metrics(self: Arc<Self>, registry: &mut Registry) {
        registry.register_collector(Box::new(Collector::new(&self)));
    }
}

impl MetricsComponent for MemoryStore {
    fn gather_metrics(&self, c: &mut CollectorState) {
        c.publish("evicting_map", &self.evicting_map, "");
    }
}

default_health_status_indicator!(MemoryStore);
