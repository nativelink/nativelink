// Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

pub use traits::{StoreTrait as Store, UploadSizeInfo};

pub struct StoreManager {
    stores: RwLock<HashMap<String, Arc<dyn Store>>>,
}

impl StoreManager {
    pub fn new() -> StoreManager {
        StoreManager {
            stores: RwLock::new(HashMap::new()),
        }
    }

    pub fn add_store(&self, name: &str, store: Arc<dyn Store>) {
        let mut stores = self.stores.write().expect("Failed to lock mutex in add_store()");
        stores.insert(name.to_string(), store);
    }

    pub fn get_store(&self, name: &str) -> Option<Arc<dyn Store>> {
        let stores = self.stores.read().expect("Failed to lock read mutex in get_store()");
        if let Some(store) = stores.get(name) {
            return Some(store.clone());
        }
        return None;
    }
}
