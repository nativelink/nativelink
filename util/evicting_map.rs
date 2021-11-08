// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::convert::TryInto;
use std::sync::Arc;
use std::time::{Duration, Instant};

use lru::LruCache;

use common::DigestInfo;
use config::backends::EvictionPolicy;

/// Wrapper used to abstract away which underlying Instant impl we are using.
/// This is needed for testing.
pub trait InstantWrapper {
    fn elapsed(&self) -> Duration;
}

impl InstantWrapper for Instant {
    fn elapsed(&self) -> Duration {
        self.elapsed()
    }
}

struct EvictionItem {
    seconds_since_anchor: u32,
    data: Arc<Vec<u8>>,
}

pub struct EvictingMap<T: InstantWrapper> {
    lru: LruCache<DigestInfo, EvictionItem>,
    anchor_time: T,
    sum_store_size: usize,
    max_bytes: usize,
    max_seconds: u32,
}

impl<T: InstantWrapper> EvictingMap<T> {
    pub fn new(config: &EvictionPolicy, anchor_time: T) -> Self {
        let mut lru = LruCache::unbounded();
        if config.max_count != 0 {
            lru = LruCache::new(config.max_count.try_into().expect("Could not convert max_count to u64"));
        }
        EvictingMap {
            lru,
            anchor_time: anchor_time,
            sum_store_size: 0,
            max_bytes: config.max_bytes,
            max_seconds: config.max_seconds,
        }
    }

    fn evict_items(&mut self) {
        // Remove items from map until size is less than max_bytes.
        while self.max_bytes != 0 && self.sum_store_size >= self.max_bytes {
            let (_, entry) = self.lru.pop_lru().expect("LRU became out of sync with sum_store_size");
            self.sum_store_size -= entry.data.len();
        }

        // Zero means never evict based on time.
        if self.max_seconds == 0 {
            return;
        }

        // Remove items that are evicted based on time.
        let evict_before_seconds =
            (self.anchor_time.elapsed().as_secs() as u32).max(self.max_seconds) - self.max_seconds;
        while let Some((_, entry)) = self.lru.peek_lru() {
            if entry.seconds_since_anchor >= evict_before_seconds {
                break;
            }
            let (_, eviction_item) = self.lru.pop_lru().expect("Tried to peek() then pop() but failed");
            self.sum_store_size -= eviction_item.data.len();
        }
    }

    pub fn contains_key(&mut self, hash: &DigestInfo) -> bool {
        if let Some(mut entry) = self.lru.get_mut(hash) {
            entry.seconds_since_anchor = self.anchor_time.elapsed().as_secs() as u32;
            return true;
        }
        false
    }

    pub fn get<'a>(&'a mut self, hash: &DigestInfo) -> Option<&'a Arc<Vec<u8>>> {
        if let Some(mut entry) = self.lru.get_mut(hash) {
            entry.seconds_since_anchor = self.anchor_time.elapsed().as_secs() as u32;
            return Some(&entry.data);
        }
        None
    }

    pub fn insert(&mut self, hash: DigestInfo, data: Arc<Vec<u8>>) {
        let new_item_size = data.len();
        let eviction_item = EvictionItem {
            seconds_since_anchor: self.anchor_time.elapsed().as_secs() as u32,
            data,
        };
        if let Some(old_item) = self.lru.put(hash, eviction_item) {
            self.sum_store_size -= old_item.data.len();
        }
        self.sum_store_size += new_item_size;
        self.evict_items();
    }

    pub fn remove(&mut self, hash: &DigestInfo) -> bool {
        self.lru.pop(hash).is_some()
    }
}
