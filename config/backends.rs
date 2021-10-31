// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use serde::Deserialize;

#[allow(non_camel_case_types)]
#[derive(Deserialize, Debug)]
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
}

#[derive(Deserialize, Debug, Default)]
pub struct MemoryStore {
    /// Policy used to evict items out of the store. Failure to set this
    /// value will cause items to never be removed from the store causing
    /// infinite memory usage.
    pub eviction_policy: Option<EvictionPolicy>,
}

#[derive(Deserialize, Debug)]
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

/// Eviction policy always works on LRU (Least Recently Used). Any time an entry
/// is touched it updates the timestamp. Inserts and updates will execute the
/// eviction policy removing any expired entries and/or the oldest entries
/// until the store size becomes smaller than max_bytes.
#[derive(Deserialize, Debug, Default)]
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

#[derive(Deserialize, Debug, Default)]
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
#[derive(Deserialize, Clone, Debug, Default)]
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
