// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::cmp;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bincode::{
    self,
    config::{FixintEncoding, WithOtherIntEncoding},
    DefaultOptions, Options,
};
use futures::stream::{self, StreamExt};
use futures::{future::try_join_all, FutureExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::codec::FramedRead;

use buf_channel::{DropCloserReadHalf, DropCloserWriteHalf, StreamReader};
use common::{log, DigestInfo, JoinHandleDropGuard, SerializableDigestInfo};
use config;
use error::{make_err, Code, Error, ResultExt};
use fastcdc::FastCDC;
use traits::{StoreTrait, UploadSizeInfo};

// NOTE: If these change update the comments in `backends.rs` to reflect
// the new defaults.
const DEFAULT_MIN_SIZE: usize = 64 * 1024;
const DEFAULT_NORM_SIZE: usize = 256 * 1024;
const DEFAULT_MAX_SIZE: usize = 512 * 1024;
const DEFAULT_MAX_CONCURRENT_FETCH_PER_GET: usize = 10;

#[derive(Serialize, Deserialize, PartialEq, Debug, Default, Clone)]
pub struct DedupIndex {
    pub entries: Vec<SerializableDigestInfo>,
}

pub struct DedupStore {
    index_store: Arc<dyn StoreTrait>,
    content_store: Arc<dyn StoreTrait>,
    fast_cdc_decoder: FastCDC,
    max_concurrent_fetch_per_get: usize,
    upload_normal_size: usize,
    bincode_options: WithOtherIntEncoding<DefaultOptions, FixintEncoding>,
}

impl DedupStore {
    pub fn new(
        config: &config::backends::DedupStore,
        index_store: Arc<dyn StoreTrait>,
        content_store: Arc<dyn StoreTrait>,
    ) -> Self {
        let min_size = if config.min_size == 0 {
            DEFAULT_MIN_SIZE
        } else {
            config.min_size as usize
        };
        let normal_size = if config.normal_size == 0 {
            DEFAULT_NORM_SIZE
        } else {
            config.normal_size as usize
        };
        let max_size = if config.max_size == 0 {
            DEFAULT_MAX_SIZE
        } else {
            config.max_size as usize
        };
        let max_concurrent_fetch_per_get = if config.max_concurrent_fetch_per_get == 0 {
            DEFAULT_MAX_CONCURRENT_FETCH_PER_GET
        } else {
            config.max_concurrent_fetch_per_get as usize
        };
        DedupStore {
            index_store,
            content_store,
            fast_cdc_decoder: FastCDC::new(min_size, normal_size, max_size),
            max_concurrent_fetch_per_get,
            // We add 30% because the normal_size is not super accurate and we'd prefer to
            // over estimate than under estimate.
            upload_normal_size: (normal_size as f64 * 1.3) as usize,
            bincode_options: DefaultOptions::new().with_fixint_encoding(),
        }
    }

    fn pin_index_store<'a>(&'a self) -> Pin<&'a dyn StoreTrait> {
        Pin::new(self.index_store.as_ref())
    }
}

#[async_trait]
impl StoreTrait for DedupStore {
    async fn has(self: Pin<&Self>, digest: DigestInfo) -> Result<Option<usize>, Error> {
        // First we need to load the index that contains where the individual parts actually
        // can be fetched from.
        let index_entries = {
            let maybe_data = self
                .pin_index_store()
                .get_part_unchunked(digest.clone(), 0, None, Some(self.upload_normal_size))
                .await
                .err_tip(|| "Failed to read index store in dedup store");
            let data = match maybe_data {
                Err(e) => {
                    if e.code == Code::NotFound {
                        return Ok(None);
                    }
                    return Err(e);
                }
                Ok(data) => data,
            };

            match self.bincode_options.deserialize::<DedupIndex>(&data) {
                Err(e) => {
                    log::warn!(
                        "Failed to deserialize index in dedup store : {} - {:?}",
                        digest.str(),
                        e
                    );
                    // We return the equivalent of NotFound here so the client is happy.
                    return Ok(None);
                }
                Ok(v) => v,
            }
        };

        let mut stream = stream::iter(index_entries.entries)
            .map(move |index_entry| {
                let content_store = self.content_store.clone();
                async move {
                    let digest = DigestInfo::new(index_entry.hash, index_entry.size_bytes as i64);
                    Pin::new(content_store.as_ref())
                        .has(digest)
                        .await
                        .err_tip(|| "Failed to check .has() on content_store in dedup store")
                }
            })
            .buffer_unordered(self.max_concurrent_fetch_per_get);

        let mut sum = 0;
        while let Some(result) = stream.next().await {
            let maybe_size = result?;
            if let Some(size) = maybe_size {
                sum += size;
                continue;
            }
            // A part is missing so return None meaning not-found.
            // This will abort all in-flight queries related to this request.
            return Ok(None);
        }
        Ok(Some(sum))
    }

    async fn update(
        self: Pin<&Self>,
        digest: DigestInfo,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        let input_max_size = match size_info {
            UploadSizeInfo::ExactSize(sz) => sz,
            UploadSizeInfo::MaxSize(sz) => sz,
        };
        let est_spawns = (input_max_size / self.upload_normal_size) + 1;
        let mut spawns = Vec::with_capacity(est_spawns);
        let mut bytes_reader = StreamReader::new(reader);
        let mut frame_reader = FramedRead::new(&mut bytes_reader, self.fast_cdc_decoder.clone());
        while let Some(frame) = frame_reader.next().await {
            let frame = frame.err_tip(|| "Failed to decode frame from fast_cdc")?;
            let content_store = self.content_store.clone();
            // Create a new spawn here so we do the sha256 on possibly a new thread (when needed).
            spawns.push(
                JoinHandleDropGuard::new(tokio::spawn(async move {
                    let hash = Sha256::digest(&frame[..]);

                    let frame_len = frame.len();
                    let index_entry = SerializableDigestInfo {
                        hash: hash.into(),
                        size_bytes: frame_len as u64,
                    };

                    let content_store_pin = Pin::new(content_store.as_ref());
                    let digest = DigestInfo::new(hash.clone().into(), frame.len() as i64);
                    if content_store_pin.has(digest.clone()).await?.is_some() {
                        // If our store has this digest, we don't need to upload it.
                        return Ok(index_entry);
                    }
                    content_store_pin
                        .update_oneshot(digest, frame)
                        .await
                        .err_tip(|| "Failed to update content store in dedup_store")?;
                    Ok(index_entry)
                }))
                .map(|result| match result.err_tip(|| "Failed to run dedup get spawn") {
                    Ok(inner_result) => inner_result,
                    Err(e) => Err(e),
                }),
            );
        }

        // Wait for all data to finish uploading to content_store.
        let index_entries = try_join_all(spawns).await?;

        let serialized_index = self
            .bincode_options
            .serialize(&DedupIndex { entries: index_entries })
            .map_err(|e| make_err!(Code::Internal, "Failed to serialize index in dedup_store : {:?}", e))?;

        self.pin_index_store()
            .update_oneshot(digest, serialized_index.into())
            .await
            .err_tip(|| "Failed to insert our index entry to index_store in dedup_store")?;

        Ok(())
    }

    async fn get_part(
        self: Pin<&Self>,
        digest: DigestInfo,
        mut writer: DropCloserWriteHalf,
        offset: usize,
        length: Option<usize>,
    ) -> Result<(), Error> {
        // First we need to download the index that contains where the individual parts actually
        // can be fetched from.
        let index_entries = {
            let data = self
                .pin_index_store()
                .get_part_unchunked(digest, 0, None, Some(self.upload_normal_size))
                .await
                .err_tip(|| "Failed to read index store in dedup store")?;

            self.bincode_options.deserialize::<DedupIndex>(&data).map_err(|e| {
                make_err!(
                    Code::Internal,
                    "Failed to deserialize index in dedup_store::get_part : {:?}",
                    e
                )
            })?
        };

        let mut start_byte_in_stream: usize = 0;
        let entries = {
            if offset == 0 && length.is_none() {
                index_entries.entries
            } else {
                let mut current_entries_sum = 0;
                let mut entries = Vec::with_capacity(index_entries.entries.len());
                for entry in index_entries.entries {
                    let first_byte = current_entries_sum;
                    current_entries_sum += entry.size_bytes as usize;
                    // Filter any items who's end byte is before the first requested byte.
                    if length.is_some() && current_entries_sum < offset {
                        start_byte_in_stream += entry.size_bytes as usize;
                        continue;
                    }
                    // Filter any items who's start byte is after the last requested byte.
                    if length.is_some() && first_byte > offset + length.unwrap() {
                        continue;
                    }
                    entries.push(entry);
                }
                entries
            }
        };

        // Second we we create a stream of futures for each chunk, but buffer/limit them so only
        // `max_concurrent_fetch_per_get` will be executed at a time.
        // The results will be streamed out in the same order they are in the entries table.
        // The results will execute in a "window-like" fashion, meaning that if we limit to
        // 5 requests at a time, and request 3 is stalled, request 1 & 2 can be output and
        // request 4 & 5 can be executing (or finished) while waiting for 3 to finish.
        // Note: We will buffer our data here up to:
        // `config.max_size * config.max_concurrent_fetch_per_get` per `get_part()` request.
        let mut entries_stream = stream::iter(entries)
            .map(move |index_entry| {
                let content_store = self.content_store.clone();

                async move {
                    let digest = DigestInfo::new(index_entry.hash, index_entry.size_bytes as i64);

                    let data = Pin::new(content_store.as_ref())
                        .get_part_unchunked(digest, 0, None, Some(index_entry.size_bytes as usize))
                        .await
                        .err_tip(|| "Failed to get_part in content_store in dedup_store")?;

                    Ok(data)
                }
            })
            .buffered(self.max_concurrent_fetch_per_get);

        // Stream out the buffered data one at a time and write the data to our writer stream.
        // In the event any of these error, we will abort early and abandon all the rest of the
        // streamed data.
        // Note: Need to take special care to ensure we send the proper slice of data requested.
        let mut bytes_to_skip = offset - start_byte_in_stream;
        let mut bytes_to_send = length.unwrap_or(usize::MAX);
        while let Some(result) = entries_stream.next().await {
            match result {
                Err(err) => return Err(err),
                Ok(mut data) => {
                    assert!(
                        bytes_to_skip <= data.len(),
                        "Formula above must be wrong, {} > {}",
                        bytes_to_skip,
                        data.len()
                    );
                    let end_pos = cmp::min(data.len(), bytes_to_send + bytes_to_skip);
                    if bytes_to_skip != 0 || data.len() > bytes_to_send {
                        data = data.slice(bytes_to_skip..end_pos);
                    }
                    writer
                        .send(data)
                        .await
                        .err_tip(|| "Failed to write data to get_part dedup")?;
                    bytes_to_send -= end_pos - bytes_to_skip;
                    bytes_to_skip -= bytes_to_skip;
                }
            }
        }

        // Finish our stream by writing our EOF and shutdown the stream.
        writer
            .send_eof()
            .await
            .err_tip(|| "Failed to write EOF out from get_part dedup")?;
        Ok(())
    }

    fn as_any(self: Arc<Self>) -> Box<dyn std::any::Any> {
        Box::new(self)
    }
}
