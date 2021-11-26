// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::{Duration, SystemTime};

use common::DigestInfo;
use error::Error;
use platform_property_manager::PlatformProperties;
use prost::Message;
use prost_types::Any;
use proto::build::bazel::remote::execution::v2::{
    execution_stage, ActionResult as ProtoActionResult, ExecuteOperationMetadata, ExecuteRequest, ExecuteResponse,
    ExecutedActionMetadata, FileNode, LogFile, NodeProperties, OutputDirectory, OutputFile, OutputSymlink, SymlinkNode,
};
use proto::google::longrunning::{operation::Result as LongRunningResult, Operation};

/// Information needed to execute an action. This struct is used over bazel's proto `Action`
/// for simplicity and offers a `salt`, which is useful to ensure during hashing (for dicts)
/// to ensure we never match against another `ActionInfo` (when a task should never be cached).
/// This struct must be 100% compatible with `ExecuteRequest` struct in remote_execution.proto.
#[derive(Clone, Debug)]
pub struct ActionInfo {
    /// Instance name used to send the request.
    pub instance_name: String,
    /// Digest of the underlying `Action`.
    pub digest: DigestInfo,
    /// Digest of the underlying `Command`.
    pub command_digest: DigestInfo,
    /// Digest of the underlying `Directory`.
    pub input_root_digest: DigestInfo,
    /// Timeout of the action.
    pub timeout: Duration,
    /// The properties rules that must be applied when finding a worker that can run this action.
    pub platform_properties: PlatformProperties,
    /// The priority of the action. Higher value means it should execute faster.
    pub priority: i64,
    /// When this action was created.
    pub insert_timestamp: SystemTime,
    /// Salt that can be filled with a random number to ensure no `ActionInfo` will be a match
    /// to another `ActionInfo` in the scheduler. When caching is wanted this value is usually
    /// zero.
    pub salt: u64,
}

impl Into<ExecuteRequest> for &ActionInfo {
    fn into(self) -> ExecuteRequest {
        ExecuteRequest {
            instance_name: self.instance_name.clone(),
            action_digest: Some(self.digest.clone().into()),
            skip_cache_lookup: true,    // The worker should never cache lookup.
            execution_policy: None,     // Not used in the worker.
            results_cache_policy: None, // Not used in the worker.
        }
    }
}

// Note: Hashing, Eq, and Ord matching on this struct is unique. Normally these functions
// must play well with each other, but in our case the following rules apply:
// * Hash - Hashing must be unique on the exact command being run and must never match
//          when do_not_cache is enabled, but must be consistent between identical data
//          hashes.
// * Eq   - Same as hash.
// * Ord  - Used when sorting `ActionInfo` together. The only major sorting is priority and
//          insert_timestamp, everything else is undefined, but must be deterministic.
impl Hash for ActionInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Digest is unique, so hashing it is all we need.
        self.digest.hash(state);
        self.salt.hash(state);
    }
}

impl PartialEq for ActionInfo {
    fn eq(&self, other: &Self) -> bool {
        self.digest == other.digest && self.salt == other.salt
    }
}

impl Eq for ActionInfo {}

impl Ord for ActionInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| self.insert_timestamp.cmp(&other.insert_timestamp))
            .then_with(|| self.salt.cmp(&other.salt))
            .then_with(|| self.digest.size_bytes.cmp(&other.digest.size_bytes))
            .then_with(|| self.digest.packed_hash.cmp(&other.digest.packed_hash))
    }
}

impl PartialOrd for ActionInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let cmp = self
            .priority
            .cmp(&other.priority)
            .then_with(|| self.insert_timestamp.cmp(&other.insert_timestamp))
            .then_with(|| self.salt.cmp(&other.salt));
        if cmp == Ordering::Equal {
            return None;
        }
        Some(cmp)
    }
}

/// Simple utility struct to determine if a string is representing a full path or
/// just the name of the file.
/// This is in order to be able to reuse the same struct instead of building different
/// structs when converting `FileInfo` -> {`OutputFile`, `FileNode`} and other simlar
/// structs.
#[derive(Eq, PartialEq, Debug, Clone)]
pub enum NameOrPath {
    Name(String),
    Path(String),
}

/// Represents an individual file and associated metadata.
/// This struct must be 100% compatible with `OutputFile` and `FileNode` structs in
/// remote_execution.proto.
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct FileInfo {
    pub name_or_path: NameOrPath,
    pub digest: DigestInfo,
    pub is_executable: bool,
    pub mtime: SystemTime,
    pub permissions: u32,
}

impl Into<FileNode> for &FileInfo {
    fn into(self) -> FileNode {
        let name = if let NameOrPath::Name(name) = &self.name_or_path {
            name.clone()
        } else {
            panic!("Cannot return a FileInfo that uses a NameOrPath::Path(), it must be a NameOrPath::Name()");
        };
        FileNode {
            name,
            digest: Some((&self.digest).into()),
            is_executable: self.is_executable,
            node_properties: Some(NodeProperties {
                properties: Default::default(),
                mtime: Some(self.mtime.into()),
                unix_mode: Some(self.permissions),
            }),
        }
    }
}

impl Into<OutputFile> for &FileInfo {
    fn into(self) -> OutputFile {
        let path = if let NameOrPath::Path(path) = &self.name_or_path {
            path.clone()
        } else {
            panic!("Cannot return a FileInfo that uses a NameOrPath::Name(), it must be a NameOrPath::Path()");
        };
        OutputFile {
            path,
            digest: Some((&self.digest).into()),
            is_executable: self.is_executable,
            contents: Default::default(),
            node_properties: Some(NodeProperties {
                properties: Default::default(),
                mtime: Some(self.mtime.into()),
                unix_mode: Some(self.permissions),
            }),
        }
    }
}

/// Represents an individual symlink file and associated metadata.
/// This struct must be 100% compatible with `SymlinkNode` and `OutputSymlink`.
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct SymlinkInfo {
    pub name_or_path: NameOrPath,
    pub target: String,
    pub mtime: SystemTime,
    pub permissions: u32,
}

impl Into<SymlinkNode> for &SymlinkInfo {
    fn into(self) -> SymlinkNode {
        let name = if let NameOrPath::Name(name) = &self.name_or_path {
            name.clone()
        } else {
            panic!("Cannot return a SymlinkInfo that uses a NameOrPath::Path(), it must be a NameOrPath::Name()");
        };
        SymlinkNode {
            name,
            target: self.target.clone(),
            node_properties: Some(NodeProperties {
                properties: Default::default(),
                mtime: Some(self.mtime.into()),
                unix_mode: Some(self.permissions),
            }),
        }
    }
}

impl Into<OutputSymlink> for &SymlinkInfo {
    fn into(self) -> OutputSymlink {
        let path = if let NameOrPath::Path(path) = &self.name_or_path {
            path.clone()
        } else {
            panic!("Cannot return a SymlinkInfo that uses a NameOrPath::Path(), it must be a NameOrPath::Name()");
        };
        OutputSymlink {
            path,
            target: self.target.clone(),
            node_properties: Some(NodeProperties {
                properties: Default::default(),
                mtime: Some(self.mtime.into()),
                unix_mode: Some(self.permissions),
            }),
        }
    }
}

/// Represents an individual directory file and associated metadata.
/// This struct must be 100% compatible with `SymlinkNode` and `OutputSymlink`.
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct DirectoryInfo {
    pub path: String,
    pub tree_digest: DigestInfo,
}

impl Into<OutputDirectory> for &DirectoryInfo {
    fn into(self) -> OutputDirectory {
        OutputDirectory {
            path: self.path.clone(),
            tree_digest: Some((&self.tree_digest).into()),
        }
    }
}

/// Represents the metadata associated with the execution result.
/// This struct must be 100% compatible with `ExecutedActionMetadata`.
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct ExecutionMetadata {
    pub worker: String,
    pub queued_timestamp: SystemTime,
    pub worker_start_timestamp: SystemTime,
    pub worker_completed_timestamp: SystemTime,
    pub input_fetch_start_timestamp: SystemTime,
    pub input_fetch_completed_timestamp: SystemTime,
    pub execution_start_timestamp: SystemTime,
    pub execution_completed_timestamp: SystemTime,
    pub output_upload_start_timestamp: SystemTime,
    pub output_upload_completed_timestamp: SystemTime,
}

impl Into<ExecutedActionMetadata> for &ExecutionMetadata {
    fn into(self) -> ExecutedActionMetadata {
        ExecutedActionMetadata {
            worker: self.worker.clone(),
            queued_timestamp: Some(self.queued_timestamp.into()),
            worker_start_timestamp: Some(self.worker_start_timestamp.into()),
            worker_completed_timestamp: Some(self.worker_completed_timestamp.into()),
            input_fetch_start_timestamp: Some(self.input_fetch_start_timestamp.into()),
            input_fetch_completed_timestamp: Some(self.input_fetch_completed_timestamp.into()),
            execution_start_timestamp: Some(self.execution_start_timestamp.into()),
            execution_completed_timestamp: Some(self.execution_completed_timestamp.into()),
            output_upload_start_timestamp: Some(self.output_upload_start_timestamp.into()),
            output_upload_completed_timestamp: Some(self.output_upload_completed_timestamp.into()),
            auxiliary_metadata: Default::default(),
        }
    }
}

/// Represents the results of an execution.
/// This struct must be 100% compatible with `ActionResult` in remote_execution.proto.
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct ActionResult {
    pub output_files: Vec<FileInfo>,
    pub output_folders: Vec<DirectoryInfo>,
    pub output_symlinks: Vec<SymlinkInfo>,
    pub exit_code: i32,
    pub stdout_digest: DigestInfo,
    pub stderr_digest: DigestInfo,
    pub execution_metadata: ExecutionMetadata,
    pub server_logs: HashMap<String, DigestInfo>,
}

impl Into<ExecuteResponse> for &ActionResult {
    fn into(self) -> ExecuteResponse {
        let mut server_logs = HashMap::with_capacity(self.server_logs.len());
        for (k, v) in &self.server_logs {
            server_logs.insert(
                k.clone(),
                LogFile {
                    digest: Some(v.into()),
                    human_readable: false,
                },
            );
        }

        ExecuteResponse {
            result: Some(ProtoActionResult {
                output_files: self.output_files.iter().map(|v| v.into()).collect(),
                output_symlinks: self.output_symlinks.iter().map(|v| v.into()).collect(),
                output_directories: self.output_folders.iter().map(|v| v.into()).collect(),
                exit_code: self.exit_code,
                stdout_digest: Some((&self.stdout_digest).into()),
                stderr_digest: Some((&self.stderr_digest).into()),
                execution_metadata: Some((&self.execution_metadata).into()),
                output_directory_symlinks: Default::default(),
                output_file_symlinks: Default::default(),
                stdout_raw: Default::default(),
                stderr_raw: Default::default(),
            }),
            cached_result: Default::default(), // This is filled later.
            status: Default::default(),        // This is filled later.
            server_logs,
            message: "TODO(blaise.bruer) We should put a reference something like bb_browser".to_string(),
        }
    }
}

/// The execution status/stage. This should match ExecutionStage::Value in remote_execution.proto.
#[derive(Eq, PartialEq, Debug, Clone)]
pub enum ActionStage {
    /// Stage is unknown.
    Unknown,
    /// Checking the cache to see if action exists.
    CacheCheck,
    /// Action has been accepted and waiting for worker to take it.
    Queued,
    /// Worker is executing the action.
    Executing,
    /// Worker completed the work with result.
    Completed(ActionResult),
    /// Result was found from cache.
    CompletedFromCache(ActionResult),
    /// Error or action failed with an exit code on the worker.
    /// This means that the job might have finished executing, but returned a non-zero
    /// exit code (for example a test failing or file not compilable). The `ActionResult`
    /// may contain better results than the `Error` message.
    Error((Error, ActionResult)),
}

/// Current state of the action.
/// This must be 100% compatible with `Operation` in `google/longrunning/operations.proto`.
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct ActionState {
    pub name: String,
    pub action_digest: DigestInfo,
    pub stage: ActionStage,
}

impl Into<Operation> for &ActionState {
    fn into(self) -> Operation {
        let (stage, maybe_execute_response) = match &self.stage {
            ActionStage::Unknown => (execution_stage::Value::Unknown, None),
            ActionStage::CacheCheck => (execution_stage::Value::CacheCheck, None),
            ActionStage::Queued => (execution_stage::Value::Queued, None),
            ActionStage::Executing => (execution_stage::Value::Executing, None),
            ActionStage::Completed(action_results) => (execution_stage::Value::Completed, Some(action_results.into())),
            ActionStage::CompletedFromCache(action_results) => {
                let mut action_result = Into::<ExecuteResponse>::into(action_results);
                action_result.cached_result = true;
                (execution_stage::Value::Completed, Some(action_result))
            }
            ActionStage::Error((err, action_results)) => {
                let mut action_result = Into::<ExecuteResponse>::into(action_results);
                action_result.status = Some(err.into());
                (execution_stage::Value::Completed, Some(action_result))
            }
        };

        let (serialized_response, done) = if let Some(execute_response) = maybe_execute_response {
            execute_response.encode_to_vec();
            (execute_response.encode_to_vec(), true)
        } else {
            (vec![], false)
        };

        let metadata = ExecuteOperationMetadata {
            stage: stage.into(),
            action_digest: Some((&self.action_digest).into()),
            // TODO(blaise.bruer) We should support stderr/stdout streaming.
            stdout_stream_name: Default::default(),
            stderr_stream_name: Default::default(),
        };

        Operation {
            name: self.name.clone(),
            metadata: Some(Any {
                type_url: "build.bazel.remote.execution.v2.ExecuteOperationMetadata".to_string(),
                value: metadata.encode_to_vec(),
            }),
            done,
            result: Some(LongRunningResult::Response(Any {
                type_url: "build.bazel.remote.execution.v2.ExecuteResponse".to_string(),
                value: serialized_response,
            })),
        }
    }
}
