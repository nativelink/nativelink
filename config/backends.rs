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

    /// FastSlow store will first try to fetch the data from the `fast`
    /// store and then if it does not exist try the `slow` store.
    /// When the object does exist in the `slow` store, it will copy
    /// the data to the `fast` store while returning the data.
    /// This store should be thought of as a store that "buffers"
    /// the data to the `fast` store.
    /// On uploads it will mirror data to both `fast` and `slow` stores.
    ///
    /// WARNING: If you need data to always exist in the `slow` store
    /// for something like remote execution, be careful because this
    /// store will never check to see if the objects exist in the
    /// `slow` store if it exists in the `fast` store (ie: it assumes
    /// that if an object exists `fast` store it will exist in `slow`
    /// store).
    fast_slow(Box<FastSlowStore>),

    /// Stores the data on the filesystem. This store is designed for
    /// local persistent storage. Restarts of this program should restore
    /// the previous state, meaning anything uploaded will be persistent
    /// as long as the filesystem integrity holds. This store uses the
    /// filesystem's `atime` (access time) to hold the last touched time
    /// of the file(s). We also use the RENAME_EXCHANGE flag to atomically
    /// swap files if a file is being replaced. Only filesystems that
    /// support `atime` and the RENAME_EXCHANGE flag are supported.
    filesystem(FilesystemStore),

    /// Store used to reference a store in the root store manager.
    /// This is useful for cases when you want to share a store in different
    /// nested stores. Example, you may want to share the same memory store
    /// used for the action cache, but use a FastSlowStore and have the fast
    /// store also share the memory store for efficiency.
    ref_store(RefStore),

    /// Uses the size field of the digest to separate which store to send the
    /// data. This is useful for cases when you'd like to put small objects
    /// in one store and large objects in another store. This should only be
    /// used if the size field is the real size of the content, in other
    /// words, don't use on AC (Action Cache) stores. Any store where you can
    /// safely use VerifyStore.verify_size = true, this store should be safe
    /// to use (ie: CAS stores).
    size_partitioning(Box<SizePartitioningStore>),

    /// This store will pass-through calls to another GRPC store. This store
    /// is not designed to be used as a sub-store of another store, but it
    /// does satisfy the interface and will likely work.
    ///
    /// One major GOTCHA is that some stores use a special function on this
    /// store to get the size of the underlying object, which is only reliable
    /// when this store is serving the a CAS store, not an AC store. If using
    /// this store directly without being a child of any store there are no
    /// side effects and is the most efficient way to use it.
    grpc(GrpcStore),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SizePartitioningStore {
    /// Size to partition the data on.
    pub size: u64,

    /// Store to send data when object is < (less than) size.
    pub lower_store: StoreConfig,

    /// Store to send data when object is >= (less than eq) size.
    pub upper_store: StoreConfig,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct RefStore {
    /// Name of the store under the root "stores" config object.
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct FilesystemStore {
    /// Path on the system where to store the actual content. This is where
    /// the bulk of the data will be placed.
    /// On service bootup this folder will be scanned and all files will be
    /// added to the cache. In the event one of the files doesn't match the
    /// criteria, the file will be deleted.
    pub content_path: String,

    /// A temporary location of where files that are being uploaded or
    /// deleted will be placed while the content cannot be guaranteed to be
    /// accurate. This location must be on the same block device as
    /// `content_path` so atomic moves can happen (ie: move without copy).
    /// All files in this folder will be deleted on every startup.
    pub temp_path: String,

    /// Buffer size to use when reading files. Generally this should be left
    /// to the default value except for testing.
    /// Default: 32k.
    #[serde(default)]
    pub read_buffer_size: u32,

    /// Policy used to evict items out of the store. Failure to set this
    /// value will cause items to never be removed from the store causing
    /// infinite memory usage.
    pub eviction_policy: Option<EvictionPolicy>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FastSlowStore {
    /// Fast store that will be attempted to be contacted before reaching
    /// out to the `slow` store.
    pub fast: StoreConfig,

    /// If the object does not exist in the `fast` store it will try to
    /// get it from this store.
    pub slow: StoreConfig,
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
    /// Default: 65536 (64k)
    #[serde(default)]
    pub min_size: u32,

    /// A best-effort attempt will be made to keep the average size
    /// of the chunks to this number. It is not a guarantee, but a
    /// slight attempt will be made.
    ///
    /// This value will also be about the threshold used to determine
    /// if we should even attempt to dedup the entry or just forward
    /// it directly to the content_store without an index. The actual
    /// value will be about `normal_size * 1.3` due to implementation
    /// details.
    ///
    /// Default: 262144 (256k)
    #[serde(default)]
    pub normal_size: u32,

    /// Maximum size a chunk is allowed to be.
    ///
    /// Default: 524288 (512k)
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

    /// Additional number of active requests this store will add to the total
    /// amount of permitted concurrent requests to s3. Instead of limiting the
    /// number of requests to s3 per store, we instead have a central counter
    /// that is global to all s3 stores. This means that if one store has this
    /// value set to 5 and another set to 0, both stores can use a maximum of 5.
    /// It is done this way because it is rare that a user would want to limit
    /// a certain store to a certain number of concurrent requests, but instead
    /// would want to globally limit it, but because s3_store has no global
    /// settings we do it on the individual store level to apply globally.
    ///
    /// Default: 20.
    #[serde(default)]
    pub additional_max_concurrent_requests: usize,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct GrpcStore {
    /// Instance name for GRPC calls. Proxy calls will have the instance_name changed to this.
    #[serde(default)]
    pub instance_name: String,

    /// The endpoint of the grpc connection.
    #[serde(default)]
    pub endpoints: Vec<String>,
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
