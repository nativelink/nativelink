// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use serde::{Deserialize, Serialize};

#[allow(non_camel_case_types)]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum StoreConfig {
    /// Memory store will store all data in a hashmap in memory.
    memory(MemoryStore),

    /// S3 store will use Amazon's S3 service as a backend to store
    /// the files. This configuration can be used to share files
    /// across multiple instances.
    ///
    /// This configuration will never delete files, so you are
    /// responsible for purging old files in other ways.
    s3_store(S3Store),

    /// Verify store is used to apply verifications to an underlying
    /// store implementation. It is strongly encouraged to validate
    /// as much data as you can before accepting data from a client,
    /// failing to do so may cause the data in the store to be
    /// populated with invalid data causing all kinds of problems.
    ///
    /// The suggested configuration is to have the CAS validate the
    /// hash and size and the AC validate nothing.
    verify(Box<VerifyStore>),

    /// A compression store that will compress the data inbound and
    /// outbound. There will be a non-trivial cost to compress and
    /// decompress the data, but in many cases if the final store is
    /// a store that requires network transport and/or storage space
    /// is a concern it is often faster and more efficient to use this
    /// store before those stores.
    compression(Box<CompressionStore>),

    /// A dedup store will take the inputs and run a rolling hash
    /// algorithm on them to slice the input into smaller parts then
    /// run a sha256 algorithm on the slice and if the object doesn't
    /// already exist, upload the slice to the `content_store` using
    /// a new digest of just the slice. Once all parts exist, an
    /// Action-Cache-like digest will be built and uploaded to the
    /// `index_store` which will contain a reference to each
    /// chunk/digest of the uploaded file. Downloading a request will
    /// first grab the index from the `index_store`, and forward the
    /// download content of each chunk as if it were one file.
    ///
    /// This store is exceptionally good when the following conditions
    /// are met:
    /// * Content is mostly the same (inserts, updates, deletes are ok)
    /// * Content is not compressed or encrypted
    /// * Uploading or downloading from `content_store` is the bottleneck.
    ///
    /// Note: This store pairs well when used with CompressionStore as
    /// the `content_store`, but never put DedupStore as the backend of
    /// CompressionStore as it will negate all the gains.
    ///
    /// Note: When running `.has()` on this store, it will only check
    /// to see if the entry exists in the `index_store` and not check
    /// if the individual chunks exist in the `content_store`.
    dedup(Box<DedupStore>),
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct MemoryStore {
    /// Policy used to evict items out of the store. Failure to set this
    /// value will cause items to never be removed from the store causing
    /// infinite memory usage.
    pub eviction_policy: Option<EvictionPolicy>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DedupStore {
    /// Store used to store the index of each dedup slice. This store
    /// should generally be fast and small.
    pub index_store: StoreConfig,

    /// The store where the individual chunks will be uploaded. This
    /// store should generally be the slower & larger store.
    pub content_store: StoreConfig,

    /// Minimum size that a chunk will be when slicing up the content.
    /// Note: This setting can be increased to improve performance
    /// because it will actually not check this number of bytes when
    /// deciding where to partition the data.
    ///
    /// Default: 4096 (4k)
    #[serde(default)]
    pub min_size: u32,

    /// A best-effort attempt will be made to keep the average size
    /// of the chunks to this number. It is not a guarantee, but a
    /// slight attempt will be made.
    ///
    /// Default: 16384 (16k)
    #[serde(default)]
    pub normal_size: u32,

    /// Maximum size a chunk is allowed to be.
    ///
    /// Default: 65536 (64k)
    #[serde(default)]
    pub max_size: u32,

    /// Due to implementation detail, we want to prefer to download
    /// the first chunks of the file so we can stream the content
    /// out and free up some of our buffers. This configuration
    /// will be used to to restrict the number of concurrent chunk
    /// downloads at a time per `get()` request.
    ///
    /// This setting will also affect how much memory might be used
    /// per `get()` request. Estimated worst case memory per `get()`
    /// request is: `max_concurrent_fetch_per_get * max_size`.
    ///
    /// Default: 10
    #[serde(default)]
    pub max_concurrent_fetch_per_get: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VerifyStore {
    /// The underlying store wrap around. All content will first flow
    /// through self before forwarding to backend. In the event there
    /// is an error detected in self, the connection to the backend
    /// will be terminated, and early termination should always cause
    /// updates to fail on the backend.
    pub backend: StoreConfig,

    /// If set the store will verify the size of the data before accepting
    /// an upload of data.
    ///
    /// This should be set to false for AC, but true for CAS stores.
    #[serde(default)]
    pub verify_size: bool,

    /// If set this store will hash the contents and verify it matches the
    /// digest hash before writing the entry to underlying store.
    ///
    /// This should be set to false for AC, but true for CAS stores.
    #[serde(default)]
    pub verify_hash: bool,
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Clone, Copy)]
pub struct Lz4Config {
    /// Size of the blocks to compress.
    /// Higher values require more ram, but might yield slightly better
    /// compression ratios.
    ///
    /// Default: 65536 (64k).
    #[serde(default)]
    pub block_size: u32,

    /// Maximum size allowed to attempt to deserialize data into.
    /// This is needed because the block_size is embedded into the data
    /// so if there was a bad actor, they could upload an extremely large
    /// block_size'ed entry and we'd allocate a large amount of memory
    /// when retrieving the data. To prevent this from happening, we
    /// allow you to specify the maximum that we'll attempt deserialize.
    ///
    /// Default: value in `block_size`.
    #[serde(default)]
    pub max_decode_block_size: u32,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum CompressionAlgorithm {
    /// LZ4 compression algorithm is extremely fast for compression and
    /// decompression, however does not perform very well in compression
    /// ratio. In most cases build artifacts are highly compressible, however
    /// lz4 is quite good at aborting early if the data is not deemed very
    /// compressible.
    ///
    /// see: https://lz4.github.io/lz4/
    LZ4(Lz4Config),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompressionStore {
    /// The underlying store wrap around. All content will first flow
    /// through self before forwarding to backend. In the event there
    /// is an error detected in self, the connection to the backend
    /// will be terminated, and early termination should always cause
    /// updates to fail on the backend.
    pub backend: StoreConfig,

    /// The compression algorithm to use.
    pub compression_algorithm: CompressionAlgorithm,
}

/// Eviction policy always works on LRU (Least Recently Used). Any time an entry
/// is touched it updates the timestamp. Inserts and updates will execute the
/// eviction policy removing any expired entries and/or the oldest entries
/// until the store size becomes smaller than max_bytes.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct EvictionPolicy {
    /// Maximum number of bytes before eviction takes place.
    /// Default: 0. Zero means never evict based on size.
    #[serde(default)]
    pub max_bytes: usize,

    /// Maximum number of seconds for an entry to live before an eviction.
    /// Default: 0. Zero means never evict based on time.
    #[serde(default)]
    pub max_seconds: u32,

    /// Maximum size of the store before an eviction takes place.
    /// Default: 0. Zero means never evict based on count.
    #[serde(default)]
    pub max_count: u64,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct S3Store {
    /// S3 region. Usually us-east-1, us-west-2, af-south-1, exc...
    #[serde(default)]
    pub region: String,

    /// Bucket name to use as the backend.
    #[serde(default)]
    pub bucket: String,

    /// If you wish to prefix the location on s3. If None, no prefix will be used.
    #[serde(default)]
    pub key_prefix: Option<String>,

    /// Retry configuration to use when a network request fails.
    #[serde(default)]
    pub retry: Retry,

    /// The number of buffer objects available to this store. The default value is 5MB
    /// for each entry. Due to the way S3Store buffers it's data and can process multiple
    /// uploads and downloads at a time (even for the same request), it might be possible
    /// for localhost to send data much faster than S3 can receive the data. If we do not
    /// use a pool of buffer objects we might end up with a significant amount of data
    /// queued up for upload in memory. This value will help curb this event from happening
    /// by throttling a request from being able to read/write more data until a previous
    /// pooled object is released.
    ///
    /// Default: 50 - This is arbitrary and no research was performed to choose this number.
    #[serde(default)]
    pub buffer_pool_size: usize,
}

/// Retry configuration. This configuration is exponential and each iteration
/// a jitter as a percentage is applied of the calculated delay. For example:
/// ```
/// Retry{
///   max_retries: 7
///   delay: .1,
///   jitter: .5
/// }
/// ```
/// will result in:
/// Attempt - Delay
/// 1         0ms
/// 2         75ms - 125ms
/// 3         150ms - 250ms
/// 4         300ms - 500ms
/// 5         600ms - 1s
/// 6         1.2s - 2s
/// 7         2.4s - 4s
/// 8         4.8s - 8s
/// Remember that to get total results is additive, meaning the above results
/// would mean a single request would have a total delay of 9.525s - 15.875s.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Retry {
    /// Maximum number of retries until retrying stops.
    /// Setting this to zero will always attempt 1 time, but not retry.
    #[serde(default)]
    pub max_retries: usize,

    /// Delay in seconds for exponential back off.
    #[serde(default)]
    pub delay: f32,

    /// Amount of jitter to add as a percentage in decimal form. This will
    /// change the formula like:
    /// ```
    /// random(
    ///    2 ^ {attempt_number} * {delay}) * (1 - (jitter / 2)),
    ///    2 ^ {attempt_number} * {delay}) * (1 + (jitter / 2)),
    /// )
    /// ```
    #[serde(default)]
    pub jitter: f32,
}
