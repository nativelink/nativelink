// Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::cell::UnsafeCell;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;

use buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use common::DigestInfo;
use config;
use error::{make_err, make_input_err, Code, Error};
use store::StoreManager;
use traits::{StoreTrait, UploadSizeInfo};

#[repr(C, align(8))]
struct AlignedStoreCell(UnsafeCell<Option<Arc<dyn StoreTrait>>>);

struct StoreReference {
    cell: AlignedStoreCell,
    mux: Mutex<()>,
}

unsafe impl Sync for StoreReference {}

pub struct RefStore {
    ref_store_name: String,
    store_manager: Arc<StoreManager>,
    ref_store: StoreReference,
}

impl RefStore {
    pub fn new(config: &config::backends::RefStore, store_manager: Arc<StoreManager>) -> Self {
        RefStore {
            ref_store_name: config.name.clone(),
            store_manager,
            ref_store: StoreReference {
                mux: Mutex::new(()),
                cell: AlignedStoreCell(UnsafeCell::new(None)),
            },
        }
    }

    // This will get the store or populate it if needed. It is designed to be quite fast on the
    // common path, but slow on the uncommon path. It does use some unsafe functions because we
    // wanted it to be fast. It is technically possible on some platforms for this function to
    // create a data race here is the reason I do not believe it is an issue:
    // 1. It would only happen on the very first call of the function (after first call we are safe)
    // 2. It should only happen on platforms that are < 64 bit address space
    // 3. It is likely that the internals of how Option work protect us anyway.
    #[inline]
    fn get_store(&self) -> Result<&Arc<dyn StoreTrait>, Error> {
        let ref_store = self.ref_store.cell.0.get();
        unsafe {
            if let Some(ref store) = *ref_store {
                return Ok(store);
            }
        }
        // This should protect us against multiple writers writing the same location at the same
        // time.
        let _lock = self
            .ref_store
            .mux
            .lock()
            .map_err(|e| make_err!(Code::Internal, "Failed to lock mutex in ref_store : {:?}", e))?;
        if let Some(store) = self.store_manager.get_store(&self.ref_store_name) {
            unsafe {
                *ref_store = Some(store.clone());
                return Ok(&(*ref_store).as_ref().unwrap());
            }
        }
        return Err(make_input_err!(
            "Failed to find store '{}' in StoreManager in RefStore",
            self.ref_store_name
        ));
    }
}

#[async_trait]
impl StoreTrait for RefStore {
    async fn has(self: Pin<&Self>, digest: DigestInfo) -> Result<Option<usize>, Error> {
        let store = self.get_store()?;
        Pin::new(store.as_ref()).has(digest).await
    }

    async fn update(
        self: Pin<&Self>,
        digest: DigestInfo,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        let store = self.get_store()?;
        Pin::new(store.as_ref()).update(digest, reader, size_info).await
    }

    async fn get_part(
        self: Pin<&Self>,
        digest: DigestInfo,
        writer: DropCloserWriteHalf,
        offset: usize,
        length: Option<usize>,
    ) -> Result<(), Error> {
        let store = self.get_store()?;
        Pin::new(store.as_ref()).get_part(digest, writer, offset, length).await
    }

    fn as_any(self: Arc<Self>) -> Box<dyn std::any::Any> {
        Box::new(self)
    }
}
