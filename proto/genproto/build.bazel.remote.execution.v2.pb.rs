// Copyright 2020 Nathan (Blaise) Bruer.  All rights reserved.
/// An `Action` captures all the information about an execution which is required
/// to reproduce it.
///
/// `Action`s are the core component of the \[Execution\] service. A single
/// `Action` represents a repeatable action that can be performed by the
/// execution service. `Action`s can be succinctly identified by the digest of
/// their wire format encoding and, once an `Action` has been executed, will be
/// cached in the action cache. Future requests can then use the cached result
/// rather than needing to run afresh.
///
/// When a server completes execution of an
/// \[Action][build.bazel.remote.execution.v2.Action\], it MAY choose to
/// cache the \[result][build.bazel.remote.execution.v2.ActionResult\] in
/// the \[ActionCache][build.bazel.remote.execution.v2.ActionCache\] unless
/// `do_not_cache` is `true`. Clients SHOULD expect the server to do so. By
/// default, future calls to
/// \[Execute][build.bazel.remote.execution.v2.Execution.Execute\] the same
/// `Action` will also serve their results from the cache. Clients must take care
/// to understand the caching behaviour. Ideally, all `Action`s will be
/// reproducible so that serving a result from cache is always desirable and
/// correct.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Action {
    /// The digest of the \[Command][build.bazel.remote.execution.v2.Command\]
    /// to run, which MUST be present in the
    /// \[ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage\].
    #[prost(message, optional, tag="1")]
    pub command_digest: ::core::option::Option<Digest>,
    /// The digest of the root
    /// \[Directory][build.bazel.remote.execution.v2.Directory\] for the input
    /// files. The files in the directory tree are available in the correct
    /// location on the build machine before the command is executed. The root
    /// directory, as well as every subdirectory and content blob referred to, MUST
    /// be in the
    /// \[ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage\].
    #[prost(message, optional, tag="2")]
    pub input_root_digest: ::core::option::Option<Digest>,
    /// A timeout after which the execution should be killed. If the timeout is
    /// absent, then the client is specifying that the execution should continue
    /// as long as the server will let it. The server SHOULD impose a timeout if
    /// the client does not specify one, however, if the client does specify a
    /// timeout that is longer than the server's maximum timeout, the server MUST
    /// reject the request.
    ///
    /// The timeout is a part of the
    /// \[Action][build.bazel.remote.execution.v2.Action\] message, and
    /// therefore two `Actions` with different timeouts are different, even if they
    /// are otherwise identical. This is because, if they were not, running an
    /// `Action` with a lower timeout than is required might result in a cache hit
    /// from an execution run with a longer timeout, hiding the fact that the
    /// timeout is too short. By encoding it directly in the `Action`, a lower
    /// timeout will result in a cache miss and the execution timeout will fail
    /// immediately, rather than whenever the cache entry gets evicted.
    #[prost(message, optional, tag="6")]
    pub timeout: ::core::option::Option<::prost_types::Duration>,
    /// If true, then the `Action`'s result cannot be cached, and in-flight
    /// requests for the same `Action` may not be merged.
    #[prost(bool, tag="7")]
    pub do_not_cache: bool,
    /// An optional additional salt value used to place this `Action` into a
    /// separate cache namespace from other instances having the same field
    /// contents. This salt typically comes from operational configuration
    /// specific to sources such as repo and service configuration,
    /// and allows disowning an entire set of ActionResults that might have been
    /// poisoned by buggy software or tool failures.
    #[prost(bytes="bytes", tag="9")]
    pub salt: ::prost::bytes::Bytes,
    /// The optional platform requirements for the execution environment. The
    /// server MAY choose to execute the action on any worker satisfying the
    /// requirements, so the client SHOULD ensure that running the action on any
    /// such worker will have the same result.  A detailed lexicon for this can be
    /// found in the accompanying platform.md.
    /// New in version 2.2: clients SHOULD set these platform properties as well
    /// as those in the \[Command][build.bazel.remote.execution.v2.Command\]. Servers
    /// SHOULD prefer those set here.
    #[prost(message, optional, tag="10")]
    pub platform: ::core::option::Option<Platform>,
}
/// A `Command` is the actual command executed by a worker running an
/// \[Action][build.bazel.remote.execution.v2.Action\] and specifications of its
/// environment.
///
/// Except as otherwise required, the environment (such as which system
/// libraries or binaries are available, and what filesystems are mounted where)
/// is defined by and specific to the implementation of the remote execution API.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Command {
    /// The arguments to the command. The first argument must be the path to the
    /// executable, which must be either a relative path, in which case it is
    /// evaluated with respect to the input root, or an absolute path.
    #[prost(string, repeated, tag="1")]
    pub arguments: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// The environment variables to set when running the program. The worker may
    /// provide its own default environment variables; these defaults can be
    /// overridden using this field. Additional variables can also be specified.
    ///
    /// In order to ensure that equivalent
    /// \[Command][build.bazel.remote.execution.v2.Command\]s always hash to the same
    /// value, the environment variables MUST be lexicographically sorted by name.
    /// Sorting of strings is done by code point, equivalently, by the UTF-8 bytes.
    #[prost(message, repeated, tag="2")]
    pub environment_variables: ::prost::alloc::vec::Vec<command::EnvironmentVariable>,
    /// A list of the output files that the client expects to retrieve from the
    /// action. Only the listed files, as well as directories listed in
    /// `output_directories`, will be returned to the client as output.
    /// Other files or directories that may be created during command execution
    /// are discarded.
    ///
    /// The paths are relative to the working directory of the action execution.
    /// The paths are specified using a single forward slash (`/`) as a path
    /// separator, even if the execution platform natively uses a different
    /// separator. The path MUST NOT include a trailing slash, nor a leading slash,
    /// being a relative path.
    ///
    /// In order to ensure consistent hashing of the same Action, the output paths
    /// MUST be sorted lexicographically by code point (or, equivalently, by UTF-8
    /// bytes).
    ///
    /// An output file cannot be duplicated, be a parent of another output file, or
    /// have the same path as any of the listed output directories.
    ///
    /// Directories leading up to the output files are created by the worker prior
    /// to execution, even if they are not explicitly part of the input root.
    ///
    /// DEPRECATED since v2.1: Use `output_paths` instead.
    #[prost(string, repeated, tag="3")]
    pub output_files: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// A list of the output directories that the client expects to retrieve from
    /// the action. Only the listed directories will be returned (an entire
    /// directory structure will be returned as a
    /// \[Tree][build.bazel.remote.execution.v2.Tree\] message digest, see
    /// \[OutputDirectory][build.bazel.remote.execution.v2.OutputDirectory\]), as
    /// well as files listed in `output_files`. Other files or directories that
    /// may be created during command execution are discarded.
    ///
    /// The paths are relative to the working directory of the action execution.
    /// The paths are specified using a single forward slash (`/`) as a path
    /// separator, even if the execution platform natively uses a different
    /// separator. The path MUST NOT include a trailing slash, nor a leading slash,
    /// being a relative path. The special value of empty string is allowed,
    /// although not recommended, and can be used to capture the entire working
    /// directory tree, including inputs.
    ///
    /// In order to ensure consistent hashing of the same Action, the output paths
    /// MUST be sorted lexicographically by code point (or, equivalently, by UTF-8
    /// bytes).
    ///
    /// An output directory cannot be duplicated or have the same path as any of
    /// the listed output files. An output directory is allowed to be a parent of
    /// another output directory.
    ///
    /// Directories leading up to the output directories (but not the output
    /// directories themselves) are created by the worker prior to execution, even
    /// if they are not explicitly part of the input root.
    ///
    /// DEPRECATED since 2.1: Use `output_paths` instead.
    #[prost(string, repeated, tag="4")]
    pub output_directories: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// A list of the output paths that the client expects to retrieve from the
    /// action. Only the listed paths will be returned to the client as output.
    /// The type of the output (file or directory) is not specified, and will be
    /// determined by the server after action execution. If the resulting path is
    /// a file, it will be returned in an
    /// \[OutputFile][build.bazel.remote.execution.v2.OutputFile\]) typed field.
    /// If the path is a directory, the entire directory structure will be returned
    /// as a \[Tree][build.bazel.remote.execution.v2.Tree\] message digest, see
    /// \[OutputDirectory][build.bazel.remote.execution.v2.OutputDirectory\])
    /// Other files or directories that may be created during command execution
    /// are discarded.
    ///
    /// The paths are relative to the working directory of the action execution.
    /// The paths are specified using a single forward slash (`/`) as a path
    /// separator, even if the execution platform natively uses a different
    /// separator. The path MUST NOT include a trailing slash, nor a leading slash,
    /// being a relative path.
    ///
    /// In order to ensure consistent hashing of the same Action, the output paths
    /// MUST be deduplicated and sorted lexicographically by code point (or,
    /// equivalently, by UTF-8 bytes).
    ///
    /// Directories leading up to the output paths are created by the worker prior
    /// to execution, even if they are not explicitly part of the input root.
    ///
    /// New in v2.1: this field supersedes the DEPRECATED `output_files` and
    /// `output_directories` fields. If `output_paths` is used, `output_files` and
    /// `output_directories` will be ignored!
    #[prost(string, repeated, tag="7")]
    pub output_paths: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// The platform requirements for the execution environment. The server MAY
    /// choose to execute the action on any worker satisfying the requirements, so
    /// the client SHOULD ensure that running the action on any such worker will
    /// have the same result.  A detailed lexicon for this can be found in the
    /// accompanying platform.md.
    /// DEPRECATED as of v2.2: platform properties are now specified directly in
    /// the action. See documentation note in the
    /// \[Action][build.bazel.remote.execution.v2.Action\] for migration.
    #[prost(message, optional, tag="5")]
    pub platform: ::core::option::Option<Platform>,
    /// The working directory, relative to the input root, for the command to run
    /// in. It must be a directory which exists in the input tree. If it is left
    /// empty, then the action is run in the input root.
    #[prost(string, tag="6")]
    pub working_directory: ::prost::alloc::string::String,
    /// A list of keys for node properties the client expects to retrieve for
    /// output files and directories. Keys are either names of string-based
    /// \[NodeProperty][build.bazel.remote.execution.v2.NodeProperty\] or
    /// names of fields in \[NodeProperties][build.bazel.remote.execution.v2.NodeProperties\].
    /// In order to ensure that equivalent `Action`s always hash to the same
    /// value, the node properties MUST be lexicographically sorted by name.
    /// Sorting of strings is done by code point, equivalently, by the UTF-8 bytes.
    ///
    /// The interpretation of string-based properties is server-dependent. If a
    /// property is not recognized by the server, the server will return an
    /// `INVALID_ARGUMENT`.
    #[prost(string, repeated, tag="8")]
    pub output_node_properties: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// Nested message and enum types in `Command`.
pub mod command {
    /// An `EnvironmentVariable` is one variable to set in the running program's
    /// environment.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct EnvironmentVariable {
        /// The variable name.
        #[prost(string, tag="1")]
        pub name: ::prost::alloc::string::String,
        /// The variable value.
        #[prost(string, tag="2")]
        pub value: ::prost::alloc::string::String,
    }
}
/// A `Platform` is a set of requirements, such as hardware, operating system, or
/// compiler toolchain, for an
/// \[Action][build.bazel.remote.execution.v2.Action\]'s execution
/// environment. A `Platform` is represented as a series of key-value pairs
/// representing the properties that are required of the platform.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Platform {
    /// The properties that make up this platform. In order to ensure that
    /// equivalent `Platform`s always hash to the same value, the properties MUST
    /// be lexicographically sorted by name, and then by value. Sorting of strings
    /// is done by code point, equivalently, by the UTF-8 bytes.
    #[prost(message, repeated, tag="1")]
    pub properties: ::prost::alloc::vec::Vec<platform::Property>,
}
/// Nested message and enum types in `Platform`.
pub mod platform {
    /// A single property for the environment. The server is responsible for
    /// specifying the property `name`s that it accepts. If an unknown `name` is
    /// provided in the requirements for an
    /// \[Action][build.bazel.remote.execution.v2.Action\], the server SHOULD
    /// reject the execution request. If permitted by the server, the same `name`
    /// may occur multiple times.
    ///
    /// The server is also responsible for specifying the interpretation of
    /// property `value`s. For instance, a property describing how much RAM must be
    /// available may be interpreted as allowing a worker with 16GB to fulfill a
    /// request for 8GB, while a property describing the OS environment on which
    /// the action must be performed may require an exact match with the worker's
    /// OS.
    ///
    /// The server MAY use the `value` of one or more properties to determine how
    /// it sets up the execution environment, such as by making specific system
    /// files available to the worker.
    ///
    /// Both names and values are typically case-sensitive. Note that the platform
    /// is implicitly part of the action digest, so even tiny changes in the names
    /// or values (like changing case) may result in different action cache
    /// entries.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Property {
        /// The property name.
        #[prost(string, tag="1")]
        pub name: ::prost::alloc::string::String,
        /// The property value.
        #[prost(string, tag="2")]
        pub value: ::prost::alloc::string::String,
    }
}
/// A `Directory` represents a directory node in a file tree, containing zero or
/// more children \[FileNodes][build.bazel.remote.execution.v2.FileNode\],
/// \[DirectoryNodes][build.bazel.remote.execution.v2.DirectoryNode\] and
/// \[SymlinkNodes][build.bazel.remote.execution.v2.SymlinkNode\].
/// Each `Node` contains its name in the directory, either the digest of its
/// content (either a file blob or a `Directory` proto) or a symlink target, as
/// well as possibly some metadata about the file or directory.
///
/// In order to ensure that two equivalent directory trees hash to the same
/// value, the following restrictions MUST be obeyed when constructing a
/// a `Directory`:
///
/// * Every child in the directory must have a path of exactly one segment.
///   Multiple levels of directory hierarchy may not be collapsed.
/// * Each child in the directory must have a unique path segment (file name).
///   Note that while the API itself is case-sensitive, the environment where
///   the Action is executed may or may not be case-sensitive. That is, it is
///   legal to call the API with a Directory that has both "Foo" and "foo" as
///   children, but the Action may be rejected by the remote system upon
///   execution.
/// * The files, directories and symlinks in the directory must each be sorted
///   in lexicographical order by path. The path strings must be sorted by code
///   point, equivalently, by UTF-8 bytes.
/// * The \[NodeProperties][build.bazel.remote.execution.v2.NodeProperty\] of files,
///   directories, and symlinks must be sorted in lexicographical order by
///   property name.
///
/// A `Directory` that obeys the restrictions is said to be in canonical form.
///
/// As an example, the following could be used for a file named `bar` and a
/// directory named `foo` with an executable file named `baz` (hashes shortened
/// for readability):
///
/// ```json
/// // (Directory proto)
/// {
///   files: [
///     {
///       name: "bar",
///       digest: {
///         hash: "4a73bc9d03...",
///         size: 65534
///       },
///       node_properties: [
///         {
///           "name": "MTime",
///           "value": "2017-01-15T01:30:15.01Z"
///         }
///       ]
///     }
///   ],
///   directories: [
///     {
///       name: "foo",
///       digest: {
///         hash: "4cf2eda940...",
///         size: 43
///       }
///     }
///   ]
/// }
///
/// // (Directory proto with hash "4cf2eda940..." and size 43)
/// {
///   files: [
///     {
///       name: "baz",
///       digest: {
///         hash: "b2c941073e...",
///         size: 1294,
///       },
///       is_executable: true
///     }
///   ]
/// }
/// ```
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Directory {
    /// The files in the directory.
    #[prost(message, repeated, tag="1")]
    pub files: ::prost::alloc::vec::Vec<FileNode>,
    /// The subdirectories in the directory.
    #[prost(message, repeated, tag="2")]
    pub directories: ::prost::alloc::vec::Vec<DirectoryNode>,
    /// The symlinks in the directory.
    #[prost(message, repeated, tag="3")]
    pub symlinks: ::prost::alloc::vec::Vec<SymlinkNode>,
    #[prost(message, optional, tag="5")]
    pub node_properties: ::core::option::Option<NodeProperties>,
}
/// A single property for \[FileNodes][build.bazel.remote.execution.v2.FileNode\],
/// \[DirectoryNodes][build.bazel.remote.execution.v2.DirectoryNode\], and
/// \[SymlinkNodes][build.bazel.remote.execution.v2.SymlinkNode\]. The server is
/// responsible for specifying the property `name`s that it accepts. If
/// permitted by the server, the same `name` may occur multiple times.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeProperty {
    /// The property name.
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    /// The property value.
    #[prost(string, tag="2")]
    pub value: ::prost::alloc::string::String,
}
/// Node properties for \[FileNodes][build.bazel.remote.execution.v2.FileNode\],
/// \[DirectoryNodes][build.bazel.remote.execution.v2.DirectoryNode\], and
/// \[SymlinkNodes][build.bazel.remote.execution.v2.SymlinkNode\]. The server is
/// responsible for specifying the properties that it accepts.
///
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeProperties {
    /// A list of string-based
    /// \[NodeProperties][build.bazel.remote.execution.v2.NodeProperty\].
    #[prost(message, repeated, tag="1")]
    pub properties: ::prost::alloc::vec::Vec<NodeProperty>,
    /// The file's last modification timestamp.
    #[prost(message, optional, tag="2")]
    pub mtime: ::core::option::Option<::prost_types::Timestamp>,
    /// The UNIX file mode, e.g., 0755.
    #[prost(message, optional, tag="3")]
    pub unix_mode: ::core::option::Option<u32>,
}
/// A `FileNode` represents a single file and associated metadata.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileNode {
    /// The name of the file.
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    /// The digest of the file's content.
    #[prost(message, optional, tag="2")]
    pub digest: ::core::option::Option<Digest>,
    /// True if file is executable, false otherwise.
    #[prost(bool, tag="4")]
    pub is_executable: bool,
    #[prost(message, optional, tag="6")]
    pub node_properties: ::core::option::Option<NodeProperties>,
}
/// A `DirectoryNode` represents a child of a
/// \[Directory][build.bazel.remote.execution.v2.Directory\] which is itself
/// a `Directory` and its associated metadata.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DirectoryNode {
    /// The name of the directory.
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    /// The digest of the
    /// \[Directory][build.bazel.remote.execution.v2.Directory\] object
    /// represented. See \[Digest][build.bazel.remote.execution.v2.Digest\]
    /// for information about how to take the digest of a proto message.
    #[prost(message, optional, tag="2")]
    pub digest: ::core::option::Option<Digest>,
}
/// A `SymlinkNode` represents a symbolic link.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SymlinkNode {
    /// The name of the symlink.
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    /// The target path of the symlink. The path separator is a forward slash `/`.
    /// The target path can be relative to the parent directory of the symlink or
    /// it can be an absolute path starting with `/`. Support for absolute paths
    /// can be checked using the \[Capabilities][build.bazel.remote.execution.v2.Capabilities\]
    /// API. `..` components are allowed anywhere in the target path as logical
    /// canonicalization may lead to different behavior in the presence of
    /// directory symlinks (e.g. `foo/../bar` may not be the same as `bar`).
    /// To reduce potential cache misses, canonicalization is still recommended
    /// where this is possible without impacting correctness.
    #[prost(string, tag="2")]
    pub target: ::prost::alloc::string::String,
    #[prost(message, optional, tag="4")]
    pub node_properties: ::core::option::Option<NodeProperties>,
}
/// A content digest. A digest for a given blob consists of the size of the blob
/// and its hash. The hash algorithm to use is defined by the server.
///
/// The size is considered to be an integral part of the digest and cannot be
/// separated. That is, even if the `hash` field is correctly specified but
/// `size_bytes` is not, the server MUST reject the request.
///
/// The reason for including the size in the digest is as follows: in a great
/// many cases, the server needs to know the size of the blob it is about to work
/// with prior to starting an operation with it, such as flattening Merkle tree
/// structures or streaming it to a worker. Technically, the server could
/// implement a separate metadata store, but this results in a significantly more
/// complicated implementation as opposed to having the client specify the size
/// up-front (or storing the size along with the digest in every message where
/// digests are embedded). This does mean that the API leaks some implementation
/// details of (what we consider to be) a reasonable server implementation, but
/// we consider this to be a worthwhile tradeoff.
///
/// When a `Digest` is used to refer to a proto message, it always refers to the
/// message in binary encoded form. To ensure consistent hashing, clients and
/// servers MUST ensure that they serialize messages according to the following
/// rules, even if there are alternate valid encodings for the same message:
///
/// * Fields are serialized in tag order.
/// * There are no unknown fields.
/// * There are no duplicate fields.
/// * Fields are serialized according to the default semantics for their type.
///
/// Most protocol buffer implementations will always follow these rules when
/// serializing, but care should be taken to avoid shortcuts. For instance,
/// concatenating two messages to merge them may produce duplicate fields.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Digest {
    /// The hash. In the case of SHA-256, it will always be a lowercase hex string
    /// exactly 64 characters long.
    #[prost(string, tag="1")]
    pub hash: ::prost::alloc::string::String,
    /// The size of the blob, in bytes.
    #[prost(int64, tag="2")]
    pub size_bytes: i64,
}
/// ExecutedActionMetadata contains details about a completed execution.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutedActionMetadata {
    /// The name of the worker which ran the execution.
    #[prost(string, tag="1")]
    pub worker: ::prost::alloc::string::String,
    /// When was the action added to the queue.
    #[prost(message, optional, tag="2")]
    pub queued_timestamp: ::core::option::Option<::prost_types::Timestamp>,
    /// When the worker received the action.
    #[prost(message, optional, tag="3")]
    pub worker_start_timestamp: ::core::option::Option<::prost_types::Timestamp>,
    /// When the worker completed the action, including all stages.
    #[prost(message, optional, tag="4")]
    pub worker_completed_timestamp: ::core::option::Option<::prost_types::Timestamp>,
    /// When the worker started fetching action inputs.
    #[prost(message, optional, tag="5")]
    pub input_fetch_start_timestamp: ::core::option::Option<::prost_types::Timestamp>,
    /// When the worker finished fetching action inputs.
    #[prost(message, optional, tag="6")]
    pub input_fetch_completed_timestamp: ::core::option::Option<::prost_types::Timestamp>,
    /// When the worker started executing the action command.
    #[prost(message, optional, tag="7")]
    pub execution_start_timestamp: ::core::option::Option<::prost_types::Timestamp>,
    /// When the worker completed executing the action command.
    #[prost(message, optional, tag="8")]
    pub execution_completed_timestamp: ::core::option::Option<::prost_types::Timestamp>,
    /// When the worker started uploading action outputs.
    #[prost(message, optional, tag="9")]
    pub output_upload_start_timestamp: ::core::option::Option<::prost_types::Timestamp>,
    /// When the worker finished uploading action outputs.
    #[prost(message, optional, tag="10")]
    pub output_upload_completed_timestamp: ::core::option::Option<::prost_types::Timestamp>,
    /// Details that are specific to the kind of worker used. For example,
    /// on POSIX-like systems this could contain a message with
    /// getrusage(2) statistics.
    #[prost(message, repeated, tag="11")]
    pub auxiliary_metadata: ::prost::alloc::vec::Vec<::prost_types::Any>,
}
/// An ActionResult represents the result of an
/// \[Action][build.bazel.remote.execution.v2.Action\] being run.
///
/// It is advised that at least one field (for example
/// `ActionResult.execution_metadata.Worker`) have a non-default value, to
/// ensure that the serialized value is non-empty, which can then be used
/// as a basic data sanity check.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionResult {
    /// The output files of the action. For each output file requested in the
    /// `output_files` or `output_paths` field of the Action, if the corresponding
    /// file existed after the action completed, a single entry will be present
    /// either in this field, or the `output_file_symlinks` field if the file was
    /// a symbolic link to another file (`output_symlinks` field after v2.1).
    ///
    /// If an output listed in `output_files` was found, but was a directory rather
    /// than a regular file, the server will return a FAILED_PRECONDITION.
    /// If the action does not produce the requested output, then that output
    /// will be omitted from the list. The server is free to arrange the output
    /// list as desired; clients MUST NOT assume that the output list is sorted.
    #[prost(message, repeated, tag="2")]
    pub output_files: ::prost::alloc::vec::Vec<OutputFile>,
    /// The output files of the action that are symbolic links to other files. Those
    /// may be links to other output files, or input files, or even absolute paths
    /// outside of the working directory, if the server supports
    /// \[SymlinkAbsolutePathStrategy.ALLOWED][build.bazel.remote.execution.v2.CacheCapabilities.SymlinkAbsolutePathStrategy\].
    /// For each output file requested in the `output_files` or `output_paths`
    /// field of the Action, if the corresponding file existed after
    /// the action completed, a single entry will be present either in this field,
    /// or in the `output_files` field, if the file was not a symbolic link.
    ///
    /// If an output symbolic link of the same name as listed in `output_files` of
    /// the Command was found, but its target type was not a regular file, the
    /// server will return a FAILED_PRECONDITION.
    /// If the action does not produce the requested output, then that output
    /// will be omitted from the list. The server is free to arrange the output
    /// list as desired; clients MUST NOT assume that the output list is sorted.
    ///
    /// DEPRECATED as of v2.1. Servers that wish to be compatible with v2.0 API
    /// should still populate this field in addition to `output_symlinks`.
    #[prost(message, repeated, tag="10")]
    pub output_file_symlinks: ::prost::alloc::vec::Vec<OutputSymlink>,
    /// New in v2.1: this field will only be populated if the command
    /// `output_paths` field was used, and not the pre v2.1 `output_files` or
    /// `output_directories` fields.
    /// The output paths of the action that are symbolic links to other paths. Those
    /// may be links to other outputs, or inputs, or even absolute paths
    /// outside of the working directory, if the server supports
    /// \[SymlinkAbsolutePathStrategy.ALLOWED][build.bazel.remote.execution.v2.CacheCapabilities.SymlinkAbsolutePathStrategy\].
    /// A single entry for each output requested in `output_paths`
    /// field of the Action, if the corresponding path existed after
    /// the action completed and was a symbolic link.
    ///
    /// If the action does not produce a requested output, then that output
    /// will be omitted from the list. The server is free to arrange the output
    /// list as desired; clients MUST NOT assume that the output list is sorted.
    #[prost(message, repeated, tag="12")]
    pub output_symlinks: ::prost::alloc::vec::Vec<OutputSymlink>,
    /// The output directories of the action. For each output directory requested
    /// in the `output_directories` or `output_paths` field of the Action, if the
    /// corresponding directory existed after the action completed, a single entry
    /// will be present in the output list, which will contain the digest of a
    /// \[Tree][build.bazel.remote.execution.v2.Tree\] message containing the
    /// directory tree, and the path equal exactly to the corresponding Action
    /// output_directories member.
    ///
    /// As an example, suppose the Action had an output directory `a/b/dir` and the
    /// execution produced the following contents in `a/b/dir`: a file named `bar`
    /// and a directory named `foo` with an executable file named `baz`. Then,
    /// output_directory will contain (hashes shortened for readability):
    ///
    /// ```json
    /// // OutputDirectory proto:
    /// {
    ///   path: "a/b/dir"
    ///   tree_digest: {
    ///     hash: "4a73bc9d03...",
    ///     size: 55
    ///   }
    /// }
    /// // Tree proto with hash "4a73bc9d03..." and size 55:
    /// {
    ///   root: {
    ///     files: [
    ///       {
    ///         name: "bar",
    ///         digest: {
    ///           hash: "4a73bc9d03...",
    ///           size: 65534
    ///         }
    ///       }
    ///     ],
    ///     directories: [
    ///       {
    ///         name: "foo",
    ///         digest: {
    ///           hash: "4cf2eda940...",
    ///           size: 43
    ///         }
    ///       }
    ///     ]
    ///   }
    ///   children : {
    ///     // (Directory proto with hash "4cf2eda940..." and size 43)
    ///     files: [
    ///       {
    ///         name: "baz",
    ///         digest: {
    ///           hash: "b2c941073e...",
    ///           size: 1294,
    ///         },
    ///         is_executable: true
    ///       }
    ///     ]
    ///   }
    /// }
    /// ```
    /// If an output of the same name as listed in `output_files` of
    /// the Command was found in `output_directories`, but was not a directory, the
    /// server will return a FAILED_PRECONDITION.
    #[prost(message, repeated, tag="3")]
    pub output_directories: ::prost::alloc::vec::Vec<OutputDirectory>,
    /// The output directories of the action that are symbolic links to other
    /// directories. Those may be links to other output directories, or input
    /// directories, or even absolute paths outside of the working directory,
    /// if the server supports
    /// \[SymlinkAbsolutePathStrategy.ALLOWED][build.bazel.remote.execution.v2.CacheCapabilities.SymlinkAbsolutePathStrategy\].
    /// For each output directory requested in the `output_directories` field of
    /// the Action, if the directory existed after the action completed, a
    /// single entry will be present either in this field, or in the
    /// `output_directories` field, if the directory was not a symbolic link.
    ///
    /// If an output of the same name was found, but was a symbolic link to a file
    /// instead of a directory, the server will return a FAILED_PRECONDITION.
    /// If the action does not produce the requested output, then that output
    /// will be omitted from the list. The server is free to arrange the output
    /// list as desired; clients MUST NOT assume that the output list is sorted.
    ///
    /// DEPRECATED as of v2.1. Servers that wish to be compatible with v2.0 API
    /// should still populate this field in addition to `output_symlinks`.
    #[prost(message, repeated, tag="11")]
    pub output_directory_symlinks: ::prost::alloc::vec::Vec<OutputSymlink>,
    /// The exit code of the command.
    #[prost(int32, tag="4")]
    pub exit_code: i32,
    /// The standard output buffer of the action. The server SHOULD NOT inline
    /// stdout unless requested by the client in the
    /// \[GetActionResultRequest][build.bazel.remote.execution.v2.GetActionResultRequest\]
    /// message. The server MAY omit inlining, even if requested, and MUST do so if inlining
    /// would cause the response to exceed message size limits.
    #[prost(bytes="bytes", tag="5")]
    pub stdout_raw: ::prost::bytes::Bytes,
    /// The digest for a blob containing the standard output of the action, which
    /// can be retrieved from the
    /// \[ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage\].
    #[prost(message, optional, tag="6")]
    pub stdout_digest: ::core::option::Option<Digest>,
    /// The standard error buffer of the action. The server SHOULD NOT inline
    /// stderr unless requested by the client in the
    /// \[GetActionResultRequest][build.bazel.remote.execution.v2.GetActionResultRequest\]
    /// message. The server MAY omit inlining, even if requested, and MUST do so if inlining
    /// would cause the response to exceed message size limits.
    #[prost(bytes="bytes", tag="7")]
    pub stderr_raw: ::prost::bytes::Bytes,
    /// The digest for a blob containing the standard error of the action, which
    /// can be retrieved from the
    /// \[ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage\].
    #[prost(message, optional, tag="8")]
    pub stderr_digest: ::core::option::Option<Digest>,
    /// The details of the execution that originally produced this result.
    #[prost(message, optional, tag="9")]
    pub execution_metadata: ::core::option::Option<ExecutedActionMetadata>,
}
/// An `OutputFile` is similar to a
/// \[FileNode][build.bazel.remote.execution.v2.FileNode\], but it is used as an
/// output in an `ActionResult`. It allows a full file path rather than
/// only a name.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OutputFile {
    /// The full path of the file relative to the working directory, including the
    /// filename. The path separator is a forward slash `/`. Since this is a
    /// relative path, it MUST NOT begin with a leading forward slash.
    #[prost(string, tag="1")]
    pub path: ::prost::alloc::string::String,
    /// The digest of the file's content.
    #[prost(message, optional, tag="2")]
    pub digest: ::core::option::Option<Digest>,
    /// True if file is executable, false otherwise.
    #[prost(bool, tag="4")]
    pub is_executable: bool,
    /// The contents of the file if inlining was requested. The server SHOULD NOT inline
    /// file contents unless requested by the client in the
    /// \[GetActionResultRequest][build.bazel.remote.execution.v2.GetActionResultRequest\]
    /// message. The server MAY omit inlining, even if requested, and MUST do so if inlining
    /// would cause the response to exceed message size limits.
    #[prost(bytes="bytes", tag="5")]
    pub contents: ::prost::bytes::Bytes,
    #[prost(message, optional, tag="7")]
    pub node_properties: ::core::option::Option<NodeProperties>,
}
/// A `Tree` contains all the
/// \[Directory][build.bazel.remote.execution.v2.Directory\] protos in a
/// single directory Merkle tree, compressed into one message.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Tree {
    /// The root directory in the tree.
    #[prost(message, optional, tag="1")]
    pub root: ::core::option::Option<Directory>,
    /// All the child directories: the directories referred to by the root and,
    /// recursively, all its children. In order to reconstruct the directory tree,
    /// the client must take the digests of each of the child directories and then
    /// build up a tree starting from the `root`.
    #[prost(message, repeated, tag="2")]
    pub children: ::prost::alloc::vec::Vec<Directory>,
}
/// An `OutputDirectory` is the output in an `ActionResult` corresponding to a
/// directory's full contents rather than a single file.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OutputDirectory {
    /// The full path of the directory relative to the working directory. The path
    /// separator is a forward slash `/`. Since this is a relative path, it MUST
    /// NOT begin with a leading forward slash. The empty string value is allowed,
    /// and it denotes the entire working directory.
    #[prost(string, tag="1")]
    pub path: ::prost::alloc::string::String,
    /// The digest of the encoded
    /// \[Tree][build.bazel.remote.execution.v2.Tree\] proto containing the
    /// directory's contents.
    #[prost(message, optional, tag="3")]
    pub tree_digest: ::core::option::Option<Digest>,
}
/// An `OutputSymlink` is similar to a
/// \[Symlink][build.bazel.remote.execution.v2.SymlinkNode\], but it is used as an
/// output in an `ActionResult`.
///
/// `OutputSymlink` is binary-compatible with `SymlinkNode`.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OutputSymlink {
    /// The full path of the symlink relative to the working directory, including the
    /// filename. The path separator is a forward slash `/`. Since this is a
    /// relative path, it MUST NOT begin with a leading forward slash.
    #[prost(string, tag="1")]
    pub path: ::prost::alloc::string::String,
    /// The target path of the symlink. The path separator is a forward slash `/`.
    /// The target path can be relative to the parent directory of the symlink or
    /// it can be an absolute path starting with `/`. Support for absolute paths
    /// can be checked using the \[Capabilities][build.bazel.remote.execution.v2.Capabilities\]
    /// API. `..` components are allowed anywhere in the target path.
    #[prost(string, tag="2")]
    pub target: ::prost::alloc::string::String,
    #[prost(message, optional, tag="4")]
    pub node_properties: ::core::option::Option<NodeProperties>,
}
/// An `ExecutionPolicy` can be used to control the scheduling of the action.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutionPolicy {
    /// The priority (relative importance) of this action. Generally, a lower value
    /// means that the action should be run sooner than actions having a greater
    /// priority value, but the interpretation of a given value is server-
    /// dependent. A priority of 0 means the *default* priority. Priorities may be
    /// positive or negative, and such actions should run later or sooner than
    /// actions having the default priority, respectively. The particular semantics
    /// of this field is up to the server. In particular, every server will have
    /// their own supported range of priorities, and will decide how these map into
    /// scheduling policy.
    #[prost(int32, tag="1")]
    pub priority: i32,
}
/// A `ResultsCachePolicy` is used for fine-grained control over how action
/// outputs are stored in the CAS and Action Cache.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResultsCachePolicy {
    /// The priority (relative importance) of this content in the overall cache.
    /// Generally, a lower value means a longer retention time or other advantage,
    /// but the interpretation of a given value is server-dependent. A priority of
    /// 0 means a *default* value, decided by the server.
    ///
    /// The particular semantics of this field is up to the server. In particular,
    /// every server will have their own supported range of priorities, and will
    /// decide how these map into retention/eviction policy.
    #[prost(int32, tag="1")]
    pub priority: i32,
}
/// A request message for
/// \[Execution.Execute][build.bazel.remote.execution.v2.Execution.Execute\].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteRequest {
    /// The instance of the execution system to operate against. A server may
    /// support multiple instances of the execution system (with their own workers,
    /// storage, caches, etc.). The server MAY require use of this field to select
    /// between them in an implementation-defined fashion, otherwise it can be
    /// omitted.
    #[prost(string, tag="1")]
    pub instance_name: ::prost::alloc::string::String,
    /// If true, the action will be executed even if its result is already
    /// present in the \[ActionCache][build.bazel.remote.execution.v2.ActionCache\].
    /// The execution is still allowed to be merged with other in-flight executions
    /// of the same action, however - semantically, the service MUST only guarantee
    /// that the results of an execution with this field set were not visible
    /// before the corresponding execution request was sent.
    /// Note that actions from execution requests setting this field set are still
    /// eligible to be entered into the action cache upon completion, and services
    /// SHOULD overwrite any existing entries that may exist. This allows
    /// skip_cache_lookup requests to be used as a mechanism for replacing action
    /// cache entries that reference outputs no longer available or that are
    /// poisoned in any way.
    /// If false, the result may be served from the action cache.
    #[prost(bool, tag="3")]
    pub skip_cache_lookup: bool,
    /// The digest of the \[Action][build.bazel.remote.execution.v2.Action\] to
    /// execute.
    #[prost(message, optional, tag="6")]
    pub action_digest: ::core::option::Option<Digest>,
    /// An optional policy for execution of the action.
    /// The server will have a default policy if this is not provided.
    #[prost(message, optional, tag="7")]
    pub execution_policy: ::core::option::Option<ExecutionPolicy>,
    /// An optional policy for the results of this execution in the remote cache.
    /// The server will have a default policy if this is not provided.
    /// This may be applied to both the ActionResult and the associated blobs.
    #[prost(message, optional, tag="8")]
    pub results_cache_policy: ::core::option::Option<ResultsCachePolicy>,
}
/// A `LogFile` is a log stored in the CAS.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LogFile {
    /// The digest of the log contents.
    #[prost(message, optional, tag="1")]
    pub digest: ::core::option::Option<Digest>,
    /// This is a hint as to the purpose of the log, and is set to true if the log
    /// is human-readable text that can be usefully displayed to a user, and false
    /// otherwise. For instance, if a command-line client wishes to print the
    /// server logs to the terminal for a failed action, this allows it to avoid
    /// displaying a binary file.
    #[prost(bool, tag="2")]
    pub human_readable: bool,
}
/// The response message for
/// \[Execution.Execute][build.bazel.remote.execution.v2.Execution.Execute\],
/// which will be contained in the [response
/// field]\[google.longrunning.Operation.response\] of the
/// \[Operation][google.longrunning.Operation\].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteResponse {
    /// The result of the action.
    #[prost(message, optional, tag="1")]
    pub result: ::core::option::Option<ActionResult>,
    /// True if the result was served from cache, false if it was executed.
    #[prost(bool, tag="2")]
    pub cached_result: bool,
    /// If the status has a code other than `OK`, it indicates that the action did
    /// not finish execution. For example, if the operation times out during
    /// execution, the status will have a `DEADLINE_EXCEEDED` code. Servers MUST
    /// use this field for errors in execution, rather than the error field on the
    /// `Operation` object.
    ///
    /// If the status code is other than `OK`, then the result MUST NOT be cached.
    /// For an error status, the `result` field is optional; the server may
    /// populate the output-, stdout-, and stderr-related fields if it has any
    /// information available, such as the stdout and stderr of a timed-out action.
    #[prost(message, optional, tag="3")]
    pub status: ::core::option::Option<super::super::super::super::super::google::rpc::Status>,
    /// An optional list of additional log outputs the server wishes to provide. A
    /// server can use this to return execution-specific logs however it wishes.
    /// This is intended primarily to make it easier for users to debug issues that
    /// may be outside of the actual job execution, such as by identifying the
    /// worker executing the action or by providing logs from the worker's setup
    /// phase. The keys SHOULD be human readable so that a client can display them
    /// to a user.
    #[prost(map="string, message", tag="4")]
    pub server_logs: ::std::collections::HashMap<::prost::alloc::string::String, LogFile>,
    /// Freeform informational message with details on the execution of the action
    /// that may be displayed to the user upon failure or when requested explicitly.
    #[prost(string, tag="5")]
    pub message: ::prost::alloc::string::String,
}
/// The current stage of action execution.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutionStage {
}
/// Nested message and enum types in `ExecutionStage`.
pub mod execution_stage {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Value {
        /// Invalid value.
        Unknown = 0,
        /// Checking the result against the cache.
        CacheCheck = 1,
        /// Currently idle, awaiting a free machine to execute.
        Queued = 2,
        /// Currently being executed by a worker.
        Executing = 3,
        /// Finished execution.
        Completed = 4,
    }
}
/// Metadata about an ongoing
/// \[execution][build.bazel.remote.execution.v2.Execution.Execute\], which
/// will be contained in the [metadata
/// field]\[google.longrunning.Operation.response\] of the
/// \[Operation][google.longrunning.Operation\].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteOperationMetadata {
    /// The current stage of execution.
    #[prost(enumeration="execution_stage::Value", tag="1")]
    pub stage: i32,
    /// The digest of the \[Action][build.bazel.remote.execution.v2.Action\]
    /// being executed.
    #[prost(message, optional, tag="2")]
    pub action_digest: ::core::option::Option<Digest>,
    /// If set, the client can use this resource name with
    /// \[ByteStream.Read][google.bytestream.ByteStream.Read\] to stream the
    /// standard output from the endpoint hosting streamed responses.
    #[prost(string, tag="3")]
    pub stdout_stream_name: ::prost::alloc::string::String,
    /// If set, the client can use this resource name with
    /// \[ByteStream.Read][google.bytestream.ByteStream.Read\] to stream the
    /// standard error from the endpoint hosting streamed responses.
    #[prost(string, tag="4")]
    pub stderr_stream_name: ::prost::alloc::string::String,
}
/// A request message for
/// \[WaitExecution][build.bazel.remote.execution.v2.Execution.WaitExecution\].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WaitExecutionRequest {
    /// The name of the \[Operation][google.longrunning.Operation\]
    /// returned by \[Execute][build.bazel.remote.execution.v2.Execution.Execute\].
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
}
/// A request message for
/// \[ActionCache.GetActionResult][build.bazel.remote.execution.v2.ActionCache.GetActionResult\].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetActionResultRequest {
    /// The instance of the execution system to operate against. A server may
    /// support multiple instances of the execution system (with their own workers,
    /// storage, caches, etc.). The server MAY require use of this field to select
    /// between them in an implementation-defined fashion, otherwise it can be
    /// omitted.
    #[prost(string, tag="1")]
    pub instance_name: ::prost::alloc::string::String,
    /// The digest of the \[Action][build.bazel.remote.execution.v2.Action\]
    /// whose result is requested.
    #[prost(message, optional, tag="2")]
    pub action_digest: ::core::option::Option<Digest>,
    /// A hint to the server to request inlining stdout in the
    /// \[ActionResult][build.bazel.remote.execution.v2.ActionResult\] message.
    #[prost(bool, tag="3")]
    pub inline_stdout: bool,
    /// A hint to the server to request inlining stderr in the
    /// \[ActionResult][build.bazel.remote.execution.v2.ActionResult\] message.
    #[prost(bool, tag="4")]
    pub inline_stderr: bool,
    /// A hint to the server to inline the contents of the listed output files.
    /// Each path needs to exactly match one file path in either `output_paths` or
    /// `output_files` (DEPRECATED since v2.1) in the
    /// \[Command][build.bazel.remote.execution.v2.Command\] message.
    #[prost(string, repeated, tag="5")]
    pub inline_output_files: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// A request message for
/// \[ActionCache.UpdateActionResult][build.bazel.remote.execution.v2.ActionCache.UpdateActionResult\].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateActionResultRequest {
    /// The instance of the execution system to operate against. A server may
    /// support multiple instances of the execution system (with their own workers,
    /// storage, caches, etc.). The server MAY require use of this field to select
    /// between them in an implementation-defined fashion, otherwise it can be
    /// omitted.
    #[prost(string, tag="1")]
    pub instance_name: ::prost::alloc::string::String,
    /// The digest of the \[Action][build.bazel.remote.execution.v2.Action\]
    /// whose result is being uploaded.
    #[prost(message, optional, tag="2")]
    pub action_digest: ::core::option::Option<Digest>,
    /// The \[ActionResult][build.bazel.remote.execution.v2.ActionResult\]
    /// to store in the cache.
    #[prost(message, optional, tag="3")]
    pub action_result: ::core::option::Option<ActionResult>,
    /// An optional policy for the results of this execution in the remote cache.
    /// The server will have a default policy if this is not provided.
    /// This may be applied to both the ActionResult and the associated blobs.
    #[prost(message, optional, tag="4")]
    pub results_cache_policy: ::core::option::Option<ResultsCachePolicy>,
}
/// A request message for
/// \[ContentAddressableStorage.FindMissingBlobs][build.bazel.remote.execution.v2.ContentAddressableStorage.FindMissingBlobs\].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FindMissingBlobsRequest {
    /// The instance of the execution system to operate against. A server may
    /// support multiple instances of the execution system (with their own workers,
    /// storage, caches, etc.). The server MAY require use of this field to select
    /// between them in an implementation-defined fashion, otherwise it can be
    /// omitted.
    #[prost(string, tag="1")]
    pub instance_name: ::prost::alloc::string::String,
    /// A list of the blobs to check.
    #[prost(message, repeated, tag="2")]
    pub blob_digests: ::prost::alloc::vec::Vec<Digest>,
}
/// A response message for
/// \[ContentAddressableStorage.FindMissingBlobs][build.bazel.remote.execution.v2.ContentAddressableStorage.FindMissingBlobs\].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FindMissingBlobsResponse {
    /// A list of the blobs requested *not* present in the storage.
    #[prost(message, repeated, tag="2")]
    pub missing_blob_digests: ::prost::alloc::vec::Vec<Digest>,
}
/// A request message for
/// \[ContentAddressableStorage.BatchUpdateBlobs][build.bazel.remote.execution.v2.ContentAddressableStorage.BatchUpdateBlobs\].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BatchUpdateBlobsRequest {
    /// The instance of the execution system to operate against. A server may
    /// support multiple instances of the execution system (with their own workers,
    /// storage, caches, etc.). The server MAY require use of this field to select
    /// between them in an implementation-defined fashion, otherwise it can be
    /// omitted.
    #[prost(string, tag="1")]
    pub instance_name: ::prost::alloc::string::String,
    /// The individual upload requests.
    #[prost(message, repeated, tag="2")]
    pub requests: ::prost::alloc::vec::Vec<batch_update_blobs_request::Request>,
}
/// Nested message and enum types in `BatchUpdateBlobsRequest`.
pub mod batch_update_blobs_request {
    /// A request corresponding to a single blob that the client wants to upload.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Request {
        /// The digest of the blob. This MUST be the digest of `data`.
        #[prost(message, optional, tag="1")]
        pub digest: ::core::option::Option<super::Digest>,
        /// The raw binary data.
        #[prost(bytes="bytes", tag="2")]
        pub data: ::prost::bytes::Bytes,
    }
}
/// A response message for
/// \[ContentAddressableStorage.BatchUpdateBlobs][build.bazel.remote.execution.v2.ContentAddressableStorage.BatchUpdateBlobs\].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BatchUpdateBlobsResponse {
    /// The responses to the requests.
    #[prost(message, repeated, tag="1")]
    pub responses: ::prost::alloc::vec::Vec<batch_update_blobs_response::Response>,
}
/// Nested message and enum types in `BatchUpdateBlobsResponse`.
pub mod batch_update_blobs_response {
    /// A response corresponding to a single blob that the client tried to upload.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Response {
        /// The blob digest to which this response corresponds.
        #[prost(message, optional, tag="1")]
        pub digest: ::core::option::Option<super::Digest>,
        /// The result of attempting to upload that blob.
        #[prost(message, optional, tag="2")]
        pub status: ::core::option::Option<super::super::super::super::super::super::google::rpc::Status>,
    }
}
/// A request message for
/// \[ContentAddressableStorage.BatchReadBlobs][build.bazel.remote.execution.v2.ContentAddressableStorage.BatchReadBlobs\].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BatchReadBlobsRequest {
    /// The instance of the execution system to operate against. A server may
    /// support multiple instances of the execution system (with their own workers,
    /// storage, caches, etc.). The server MAY require use of this field to select
    /// between them in an implementation-defined fashion, otherwise it can be
    /// omitted.
    #[prost(string, tag="1")]
    pub instance_name: ::prost::alloc::string::String,
    /// The individual blob digests.
    #[prost(message, repeated, tag="2")]
    pub digests: ::prost::alloc::vec::Vec<Digest>,
}
/// A response message for
/// \[ContentAddressableStorage.BatchReadBlobs][build.bazel.remote.execution.v2.ContentAddressableStorage.BatchReadBlobs\].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BatchReadBlobsResponse {
    /// The responses to the requests.
    #[prost(message, repeated, tag="1")]
    pub responses: ::prost::alloc::vec::Vec<batch_read_blobs_response::Response>,
}
/// Nested message and enum types in `BatchReadBlobsResponse`.
pub mod batch_read_blobs_response {
    /// A response corresponding to a single blob that the client tried to download.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Response {
        /// The digest to which this response corresponds.
        #[prost(message, optional, tag="1")]
        pub digest: ::core::option::Option<super::Digest>,
        /// The raw binary data.
        #[prost(bytes="bytes", tag="2")]
        pub data: ::prost::bytes::Bytes,
        /// The result of attempting to download that blob.
        #[prost(message, optional, tag="3")]
        pub status: ::core::option::Option<super::super::super::super::super::super::google::rpc::Status>,
    }
}
/// A request message for
/// \[ContentAddressableStorage.GetTree][build.bazel.remote.execution.v2.ContentAddressableStorage.GetTree\].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetTreeRequest {
    /// The instance of the execution system to operate against. A server may
    /// support multiple instances of the execution system (with their own workers,
    /// storage, caches, etc.). The server MAY require use of this field to select
    /// between them in an implementation-defined fashion, otherwise it can be
    /// omitted.
    #[prost(string, tag="1")]
    pub instance_name: ::prost::alloc::string::String,
    /// The digest of the root, which must be an encoded
    /// \[Directory][build.bazel.remote.execution.v2.Directory\] message
    /// stored in the
    /// \[ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage\].
    #[prost(message, optional, tag="2")]
    pub root_digest: ::core::option::Option<Digest>,
    /// A maximum page size to request. If present, the server will request no more
    /// than this many items. Regardless of whether a page size is specified, the
    /// server may place its own limit on the number of items to be returned and
    /// require the client to retrieve more items using a subsequent request.
    #[prost(int32, tag="3")]
    pub page_size: i32,
    /// A page token, which must be a value received in a previous
    /// \[GetTreeResponse][build.bazel.remote.execution.v2.GetTreeResponse\].
    /// If present, the server will use that token as an offset, returning only
    /// that page and the ones that succeed it.
    #[prost(string, tag="4")]
    pub page_token: ::prost::alloc::string::String,
}
/// A response message for
/// \[ContentAddressableStorage.GetTree][build.bazel.remote.execution.v2.ContentAddressableStorage.GetTree\].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetTreeResponse {
    /// The directories descended from the requested root.
    #[prost(message, repeated, tag="1")]
    pub directories: ::prost::alloc::vec::Vec<Directory>,
    /// If present, signifies that there are more results which the client can
    /// retrieve by passing this as the page_token in a subsequent
    /// \[request][build.bazel.remote.execution.v2.GetTreeRequest\].
    /// If empty, signifies that this is the last page of results.
    #[prost(string, tag="2")]
    pub next_page_token: ::prost::alloc::string::String,
}
/// A request message for
/// \[Capabilities.GetCapabilities][build.bazel.remote.execution.v2.Capabilities.GetCapabilities\].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetCapabilitiesRequest {
    /// The instance of the execution system to operate against. A server may
    /// support multiple instances of the execution system (with their own workers,
    /// storage, caches, etc.). The server MAY require use of this field to select
    /// between them in an implementation-defined fashion, otherwise it can be
    /// omitted.
    #[prost(string, tag="1")]
    pub instance_name: ::prost::alloc::string::String,
}
/// A response message for
/// \[Capabilities.GetCapabilities][build.bazel.remote.execution.v2.Capabilities.GetCapabilities\].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ServerCapabilities {
    /// Capabilities of the remote cache system.
    #[prost(message, optional, tag="1")]
    pub cache_capabilities: ::core::option::Option<CacheCapabilities>,
    /// Capabilities of the remote execution system.
    #[prost(message, optional, tag="2")]
    pub execution_capabilities: ::core::option::Option<ExecutionCapabilities>,
    /// Earliest RE API version supported, including deprecated versions.
    #[prost(message, optional, tag="3")]
    pub deprecated_api_version: ::core::option::Option<super::super::super::semver::SemVer>,
    /// Earliest non-deprecated RE API version supported.
    #[prost(message, optional, tag="4")]
    pub low_api_version: ::core::option::Option<super::super::super::semver::SemVer>,
    /// Latest RE API version supported.
    #[prost(message, optional, tag="5")]
    pub high_api_version: ::core::option::Option<super::super::super::semver::SemVer>,
}
/// The digest function used for converting values into keys for CAS and Action
/// Cache.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DigestFunction {
}
/// Nested message and enum types in `DigestFunction`.
pub mod digest_function {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Value {
        /// It is an error for the server to return this value.
        Unknown = 0,
        /// The SHA-256 digest function.
        Sha256 = 1,
        /// The SHA-1 digest function.
        Sha1 = 2,
        /// The MD5 digest function.
        Md5 = 3,
        /// The Microsoft "VSO-Hash" paged SHA256 digest function.
        /// See <https://github.com/microsoft/BuildXL/blob/master/Documentation/Specs/PagedHash.md> .
        Vso = 4,
        /// The SHA-384 digest function.
        Sha384 = 5,
        /// The SHA-512 digest function.
        Sha512 = 6,
        /// Murmur3 128-bit digest function, x64 variant. Note that this is not a
        /// cryptographic hash function and its collision properties are not strongly guaranteed.
        /// See <https://github.com/aappleby/smhasher/wiki/MurmurHash3> .
        Murmur3 = 7,
    }
}
/// Describes the server/instance capabilities for updating the action cache.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionCacheUpdateCapabilities {
    #[prost(bool, tag="1")]
    pub update_enabled: bool,
}
/// Allowed values for priority in
/// \[ResultsCachePolicy][build.bazel.remoteexecution.v2.ResultsCachePolicy\] and
/// \[ExecutionPolicy][build.bazel.remoteexecution.v2.ResultsCachePolicy\]
/// Used for querying both cache and execution valid priority ranges.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PriorityCapabilities {
    #[prost(message, repeated, tag="1")]
    pub priorities: ::prost::alloc::vec::Vec<priority_capabilities::PriorityRange>,
}
/// Nested message and enum types in `PriorityCapabilities`.
pub mod priority_capabilities {
    /// Supported range of priorities, including boundaries.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct PriorityRange {
        /// The minimum numeric value for this priority range, which represents the
        /// most urgent task or longest retained item.
        #[prost(int32, tag="1")]
        pub min_priority: i32,
        /// The maximum numeric value for this priority range, which represents the
        /// least urgent task or shortest retained item.
        #[prost(int32, tag="2")]
        pub max_priority: i32,
    }
}
/// Describes how the server treats absolute symlink targets.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SymlinkAbsolutePathStrategy {
}
/// Nested message and enum types in `SymlinkAbsolutePathStrategy`.
pub mod symlink_absolute_path_strategy {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Value {
        /// Invalid value.
        Unknown = 0,
        /// Server will return an `INVALID_ARGUMENT` on input symlinks with absolute
        /// targets.
        /// If an action tries to create an output symlink with an absolute target, a
        /// `FAILED_PRECONDITION` will be returned.
        Disallowed = 1,
        /// Server will allow symlink targets to escape the input root tree, possibly
        /// resulting in non-hermetic builds.
        Allowed = 2,
    }
}
/// Capabilities of the remote cache system.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CacheCapabilities {
    /// All the digest functions supported by the remote cache.
    /// Remote cache may support multiple digest functions simultaneously.
    #[prost(enumeration="digest_function::Value", repeated, tag="1")]
    pub digest_function: ::prost::alloc::vec::Vec<i32>,
    /// Capabilities for updating the action cache.
    #[prost(message, optional, tag="2")]
    pub action_cache_update_capabilities: ::core::option::Option<ActionCacheUpdateCapabilities>,
    /// Supported cache priority range for both CAS and ActionCache.
    #[prost(message, optional, tag="3")]
    pub cache_priority_capabilities: ::core::option::Option<PriorityCapabilities>,
    /// Maximum total size of blobs to be uploaded/downloaded using
    /// batch methods. A value of 0 means no limit is set, although
    /// in practice there will always be a message size limitation
    /// of the protocol in use, e.g. GRPC.
    #[prost(int64, tag="4")]
    pub max_batch_total_size_bytes: i64,
    /// Whether absolute symlink targets are supported.
    #[prost(enumeration="symlink_absolute_path_strategy::Value", tag="5")]
    pub symlink_absolute_path_strategy: i32,
}
/// Capabilities of the remote execution system.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutionCapabilities {
    /// Remote execution may only support a single digest function.
    #[prost(enumeration="digest_function::Value", tag="1")]
    pub digest_function: i32,
    /// Whether remote execution is enabled for the particular server/instance.
    #[prost(bool, tag="2")]
    pub exec_enabled: bool,
    /// Supported execution priority range.
    #[prost(message, optional, tag="3")]
    pub execution_priority_capabilities: ::core::option::Option<PriorityCapabilities>,
    /// Supported node properties.
    #[prost(string, repeated, tag="4")]
    pub supported_node_properties: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// Details for the tool used to call the API.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ToolDetails {
    /// Name of the tool, e.g. bazel.
    #[prost(string, tag="1")]
    pub tool_name: ::prost::alloc::string::String,
    /// Version of the tool used for the request, e.g. 5.0.3.
    #[prost(string, tag="2")]
    pub tool_version: ::prost::alloc::string::String,
}
/// An optional Metadata to attach to any RPC request to tell the server about an
/// external context of the request. The server may use this for logging or other
/// purposes. To use it, the client attaches the header to the call using the
/// canonical proto serialization:
///
/// * name: `build.bazel.remote.execution.v2.requestmetadata-bin`
/// * contents: the base64 encoded binary `RequestMetadata` message.
/// Note: the gRPC library serializes binary headers encoded in base 64 by
/// default (<https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#requests>).
/// Therefore, if the gRPC library is used to pass/retrieve this
/// metadata, the user may ignore the base64 encoding and assume it is simply
/// serialized as a binary message.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RequestMetadata {
    /// The details for the tool invoking the requests.
    #[prost(message, optional, tag="1")]
    pub tool_details: ::core::option::Option<ToolDetails>,
    /// An identifier that ties multiple requests to the same action.
    /// For example, multiple requests to the CAS, Action Cache, and Execution
    /// API are used in order to compile foo.cc.
    #[prost(string, tag="2")]
    pub action_id: ::prost::alloc::string::String,
    /// An identifier that ties multiple actions together to a final result.
    /// For example, multiple actions are required to build and run foo_test.
    #[prost(string, tag="3")]
    pub tool_invocation_id: ::prost::alloc::string::String,
    /// An identifier to tie multiple tool invocations together. For example,
    /// runs of foo_test, bar_test and baz_test on a post-submit of a given patch.
    #[prost(string, tag="4")]
    pub correlated_invocations_id: ::prost::alloc::string::String,
}
/// Generated client implementations.
pub mod execution_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// The Remote Execution API is used to execute an
    /// [Action][build.bazel.remote.execution.v2.Action] on the remote
    /// workers.
    ///
    /// As with other services in the Remote Execution API, any call may return an
    /// error with a [RetryInfo][google.rpc.RetryInfo] error detail providing
    /// information about when the client should retry the request; clients SHOULD
    /// respect the information provided.
    #[derive(Debug, Clone)]
    pub struct ExecutionClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ExecutionClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ExecutionClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Default + Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> ExecutionClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            ExecutionClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with `gzip`.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        /// Enable decompressing responses with `gzip`.
        #[must_use]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        /// Execute an action remotely.
        ///
        /// In order to execute an action, the client must first upload all of the
        /// inputs, the
        /// [Command][build.bazel.remote.execution.v2.Command] to run, and the
        /// [Action][build.bazel.remote.execution.v2.Action] into the
        /// [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage].
        /// It then calls `Execute` with an `action_digest` referring to them. The
        /// server will run the action and eventually return the result.
        ///
        /// The input `Action`'s fields MUST meet the various canonicalization
        /// requirements specified in the documentation for their types so that it has
        /// the same digest as other logically equivalent `Action`s. The server MAY
        /// enforce the requirements and return errors if a non-canonical input is
        /// received. It MAY also proceed without verifying some or all of the
        /// requirements, such as for performance reasons. If the server does not
        /// verify the requirement, then it will treat the `Action` as distinct from
        /// another logically equivalent action if they hash differently.
        ///
        /// Returns a stream of
        /// [google.longrunning.Operation][google.longrunning.Operation] messages
        /// describing the resulting execution, with eventual `response`
        /// [ExecuteResponse][build.bazel.remote.execution.v2.ExecuteResponse]. The
        /// `metadata` on the operation is of type
        /// [ExecuteOperationMetadata][build.bazel.remote.execution.v2.ExecuteOperationMetadata].
        ///
        /// If the client remains connected after the first response is returned after
        /// the server, then updates are streamed as if the client had called
        /// [WaitExecution][build.bazel.remote.execution.v2.Execution.WaitExecution]
        /// until the execution completes or the request reaches an error. The
        /// operation can also be queried using [Operations
        /// API][google.longrunning.Operations.GetOperation].
        ///
        /// The server NEED NOT implement other methods or functionality of the
        /// Operations API.
        ///
        /// Errors discovered during creation of the `Operation` will be reported
        /// as gRPC Status errors, while errors that occurred while running the
        /// action will be reported in the `status` field of the `ExecuteResponse`. The
        /// server MUST NOT set the `error` field of the `Operation` proto.
        /// The possible errors include:
        ///
        /// * `INVALID_ARGUMENT`: One or more arguments are invalid.
        /// * `FAILED_PRECONDITION`: One or more errors occurred in setting up the
        ///   action requested, such as a missing input or command or no worker being
        ///   available. The client may be able to fix the errors and retry.
        /// * `RESOURCE_EXHAUSTED`: There is insufficient quota of some resource to run
        ///   the action.
        /// * `UNAVAILABLE`: Due to a transient condition, such as all workers being
        ///   occupied (and the server does not support a queue), the action could not
        ///   be started. The client should retry.
        /// * `INTERNAL`: An internal error occurred in the execution engine or the
        ///   worker.
        /// * `DEADLINE_EXCEEDED`: The execution timed out.
        /// * `CANCELLED`: The operation was cancelled by the client. This status is
        ///   only possible if the server implements the Operations API CancelOperation
        ///   method, and it was called for the current execution.
        ///
        /// In the case of a missing input or command, the server SHOULD additionally
        /// send a [PreconditionFailure][google.rpc.PreconditionFailure] error detail
        /// where, for each requested blob not present in the CAS, there is a
        /// `Violation` with a `type` of `MISSING` and a `subject` of
        /// `"blobs/{hash}/{size}"` indicating the digest of the missing blob.
        ///
        /// The server does not need to guarantee that a call to this method leads to
        /// at most one execution of the action. The server MAY execute the action
        /// multiple times, potentially in parallel. These redundant executions MAY
        /// continue to run, even if the operation is completed.
        pub async fn execute(
            &mut self,
            request: impl tonic::IntoRequest<super::ExecuteRequest>,
        ) -> Result<
                tonic::Response<
                    tonic::codec::Streaming<
                        super::super::super::super::super::super::google::longrunning::Operation,
                    >,
                >,
                tonic::Status,
            > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/build.bazel.remote.execution.v2.Execution/Execute",
            );
            self.inner.server_streaming(request.into_request(), path, codec).await
        }
        /// Wait for an execution operation to complete. When the client initially
        /// makes the request, the server immediately responds with the current status
        /// of the execution. The server will leave the request stream open until the
        /// operation completes, and then respond with the completed operation. The
        /// server MAY choose to stream additional updates as execution progresses,
        /// such as to provide an update as to the state of the execution.
        pub async fn wait_execution(
            &mut self,
            request: impl tonic::IntoRequest<super::WaitExecutionRequest>,
        ) -> Result<
                tonic::Response<
                    tonic::codec::Streaming<
                        super::super::super::super::super::super::google::longrunning::Operation,
                    >,
                >,
                tonic::Status,
            > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/build.bazel.remote.execution.v2.Execution/WaitExecution",
            );
            self.inner.server_streaming(request.into_request(), path, codec).await
        }
    }
}
/// Generated client implementations.
pub mod action_cache_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// The action cache API is used to query whether a given action has already been
    /// performed and, if so, retrieve its result. Unlike the
    /// [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage],
    /// which addresses blobs by their own content, the action cache addresses the
    /// [ActionResult][build.bazel.remote.execution.v2.ActionResult] by a
    /// digest of the encoded [Action][build.bazel.remote.execution.v2.Action]
    /// which produced them.
    ///
    /// The lifetime of entries in the action cache is implementation-specific, but
    /// the server SHOULD assume that more recently used entries are more likely to
    /// be used again.
    ///
    /// As with other services in the Remote Execution API, any call may return an
    /// error with a [RetryInfo][google.rpc.RetryInfo] error detail providing
    /// information about when the client should retry the request; clients SHOULD
    /// respect the information provided.
    #[derive(Debug, Clone)]
    pub struct ActionCacheClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ActionCacheClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ActionCacheClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Default + Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> ActionCacheClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            ActionCacheClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with `gzip`.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        /// Enable decompressing responses with `gzip`.
        #[must_use]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        /// Retrieve a cached execution result.
        ///
        /// Implementations SHOULD ensure that any blobs referenced from the
        /// [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage]
        /// are available at the time of returning the
        /// [ActionResult][build.bazel.remote.execution.v2.ActionResult] and will be
        /// for some period of time afterwards. The lifetimes of the referenced blobs SHOULD be increased
        /// if necessary and applicable.
        ///
        /// Errors:
        ///
        /// * `NOT_FOUND`: The requested `ActionResult` is not in the cache.
        pub async fn get_action_result(
            &mut self,
            request: impl tonic::IntoRequest<super::GetActionResultRequest>,
        ) -> Result<tonic::Response<super::ActionResult>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/build.bazel.remote.execution.v2.ActionCache/GetActionResult",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Upload a new execution result.
        ///
        /// In order to allow the server to perform access control based on the type of
        /// action, and to assist with client debugging, the client MUST first upload
        /// the [Action][build.bazel.remote.execution.v2.Execution] that produced the
        /// result, along with its
        /// [Command][build.bazel.remote.execution.v2.Command], into the
        /// `ContentAddressableStorage`.
        ///
        /// Server implementations MAY modify the
        /// `UpdateActionResultRequest.action_result` and return an equivalent value.
        ///
        /// Errors:
        ///
        /// * `INVALID_ARGUMENT`: One or more arguments are invalid.
        /// * `FAILED_PRECONDITION`: One or more errors occurred in updating the
        ///   action result, such as a missing command or action.
        /// * `RESOURCE_EXHAUSTED`: There is insufficient storage space to add the
        ///   entry to the cache.
        pub async fn update_action_result(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdateActionResultRequest>,
        ) -> Result<tonic::Response<super::ActionResult>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/build.bazel.remote.execution.v2.ActionCache/UpdateActionResult",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated client implementations.
pub mod content_addressable_storage_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// The CAS (content-addressable storage) is used to store the inputs to and
    /// outputs from the execution service. Each piece of content is addressed by the
    /// digest of its binary data.
    ///
    /// Most of the binary data stored in the CAS is opaque to the execution engine,
    /// and is only used as a communication medium. In order to build an
    /// [Action][build.bazel.remote.execution.v2.Action],
    /// however, the client will need to also upload the
    /// [Command][build.bazel.remote.execution.v2.Command] and input root
    /// [Directory][build.bazel.remote.execution.v2.Directory] for the Action.
    /// The Command and Directory messages must be marshalled to wire format and then
    /// uploaded under the hash as with any other piece of content. In practice, the
    /// input root directory is likely to refer to other Directories in its
    /// hierarchy, which must also each be uploaded on their own.
    ///
    /// For small file uploads the client should group them together and call
    /// [BatchUpdateBlobs][build.bazel.remote.execution.v2.ContentAddressableStorage.BatchUpdateBlobs].
    /// For large uploads, the client must use the
    /// [Write method][google.bytestream.ByteStream.Write] of the ByteStream API. The
    /// `resource_name` is `{instance_name}/uploads/{uuid}/blobs/{hash}/{size}`,
    /// where `instance_name` is as described in the next paragraph, `uuid` is a
    /// version 4 UUID generated by the client, and `hash` and `size` are the
    /// [Digest][build.bazel.remote.execution.v2.Digest] of the blob. The
    /// `uuid` is used only to avoid collisions when multiple clients try to upload
    /// the same file (or the same client tries to upload the file multiple times at
    /// once on different threads), so the client MAY reuse the `uuid` for uploading
    /// different blobs. The `resource_name` may optionally have a trailing filename
    /// (or other metadata) for a client to use if it is storing URLs, as in
    /// `{instance}/uploads/{uuid}/blobs/{hash}/{size}/foo/bar/baz.cc`. Anything
    /// after the `size` is ignored.
    ///
    /// A single server MAY support multiple instances of the execution system, each
    /// with their own workers, storage, cache, etc. The exact relationship between
    /// instances is up to the server. If the server does, then the `instance_name`
    /// is an identifier, possibly containing multiple path segments, used to
    /// distinguish between the various instances on the server, in a manner defined
    /// by the server. For servers which do not support multiple instances, then the
    /// `instance_name` is the empty path and the leading slash is omitted, so that
    /// the `resource_name` becomes `uploads/{uuid}/blobs/{hash}/{size}`.
    /// To simplify parsing, a path segment cannot equal any of the following
    /// keywords: `blobs`, `uploads`, `actions`, `actionResults`, `operations` and
    /// `capabilities`.
    ///
    /// When attempting an upload, if another client has already completed the upload
    /// (which may occur in the middle of a single upload if another client uploads
    /// the same blob concurrently), the request will terminate immediately with
    /// a response whose `committed_size` is the full size of the uploaded file
    /// (regardless of how much data was transmitted by the client). If the client
    /// completes the upload but the
    /// [Digest][build.bazel.remote.execution.v2.Digest] does not match, an
    /// `INVALID_ARGUMENT` error will be returned. In either case, the client should
    /// not attempt to retry the upload.
    ///
    /// For downloading blobs, the client must use the
    /// [Read method][google.bytestream.ByteStream.Read] of the ByteStream API, with
    /// a `resource_name` of `"{instance_name}/blobs/{hash}/{size}"`, where
    /// `instance_name` is the instance name (see above), and `hash` and `size` are
    /// the [Digest][build.bazel.remote.execution.v2.Digest] of the blob.
    ///
    /// The lifetime of entries in the CAS is implementation specific, but it SHOULD
    /// be long enough to allow for newly-added and recently looked-up entries to be
    /// used in subsequent calls (e.g. to
    /// [Execute][build.bazel.remote.execution.v2.Execution.Execute]).
    ///
    /// Servers MUST behave as though empty blobs are always available, even if they
    /// have not been uploaded. Clients MAY optimize away the uploading or
    /// downloading of empty blobs.
    ///
    /// As with other services in the Remote Execution API, any call may return an
    /// error with a [RetryInfo][google.rpc.RetryInfo] error detail providing
    /// information about when the client should retry the request; clients SHOULD
    /// respect the information provided.
    #[derive(Debug, Clone)]
    pub struct ContentAddressableStorageClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ContentAddressableStorageClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ContentAddressableStorageClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Default + Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> ContentAddressableStorageClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            ContentAddressableStorageClient::new(
                InterceptedService::new(inner, interceptor),
            )
        }
        /// Compress requests with `gzip`.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        /// Enable decompressing responses with `gzip`.
        #[must_use]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        /// Determine if blobs are present in the CAS.
        ///
        /// Clients can use this API before uploading blobs to determine which ones are
        /// already present in the CAS and do not need to be uploaded again.
        ///
        /// Servers SHOULD increase the lifetimes of the referenced blobs if necessary and
        /// applicable.
        ///
        /// There are no method-specific errors.
        pub async fn find_missing_blobs(
            &mut self,
            request: impl tonic::IntoRequest<super::FindMissingBlobsRequest>,
        ) -> Result<tonic::Response<super::FindMissingBlobsResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Upload many blobs at once.
        ///
        /// The server may enforce a limit of the combined total size of blobs
        /// to be uploaded using this API. This limit may be obtained using the
        /// [Capabilities][build.bazel.remote.execution.v2.Capabilities] API.
        /// Requests exceeding the limit should either be split into smaller
        /// chunks or uploaded using the
        /// [ByteStream API][google.bytestream.ByteStream], as appropriate.
        ///
        /// This request is equivalent to calling a Bytestream `Write` request
        /// on each individual blob, in parallel. The requests may succeed or fail
        /// independently.
        ///
        /// Errors:
        ///
        /// * `INVALID_ARGUMENT`: The client attempted to upload more than the
        ///   server supported limit.
        ///
        /// Individual requests may return the following errors, additionally:
        ///
        /// * `RESOURCE_EXHAUSTED`: There is insufficient disk quota to store the blob.
        /// * `INVALID_ARGUMENT`: The
        /// [Digest][build.bazel.remote.execution.v2.Digest] does not match the
        /// provided data.
        pub async fn batch_update_blobs(
            &mut self,
            request: impl tonic::IntoRequest<super::BatchUpdateBlobsRequest>,
        ) -> Result<tonic::Response<super::BatchUpdateBlobsResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Download many blobs at once.
        ///
        /// The server may enforce a limit of the combined total size of blobs
        /// to be downloaded using this API. This limit may be obtained using the
        /// [Capabilities][build.bazel.remote.execution.v2.Capabilities] API.
        /// Requests exceeding the limit should either be split into smaller
        /// chunks or downloaded using the
        /// [ByteStream API][google.bytestream.ByteStream], as appropriate.
        ///
        /// This request is equivalent to calling a Bytestream `Read` request
        /// on each individual blob, in parallel. The requests may succeed or fail
        /// independently.
        ///
        /// Errors:
        ///
        /// * `INVALID_ARGUMENT`: The client attempted to read more than the
        ///   server supported limit.
        ///
        /// Every error on individual read will be returned in the corresponding digest
        /// status.
        pub async fn batch_read_blobs(
            &mut self,
            request: impl tonic::IntoRequest<super::BatchReadBlobsRequest>,
        ) -> Result<tonic::Response<super::BatchReadBlobsResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Fetch the entire directory tree rooted at a node.
        ///
        /// This request must be targeted at a
        /// [Directory][build.bazel.remote.execution.v2.Directory] stored in the
        /// [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage]
        /// (CAS). The server will enumerate the `Directory` tree recursively and
        /// return every node descended from the root.
        ///
        /// The GetTreeRequest.page_token parameter can be used to skip ahead in
        /// the stream (e.g. when retrying a partially completed and aborted request),
        /// by setting it to a value taken from GetTreeResponse.next_page_token of the
        /// last successfully processed GetTreeResponse).
        ///
        /// The exact traversal order is unspecified and, unless retrieving subsequent
        /// pages from an earlier request, is not guaranteed to be stable across
        /// multiple invocations of `GetTree`.
        ///
        /// If part of the tree is missing from the CAS, the server will return the
        /// portion present and omit the rest.
        ///
        /// Errors:
        ///
        /// * `NOT_FOUND`: The requested tree root is not present in the CAS.
        pub async fn get_tree(
            &mut self,
            request: impl tonic::IntoRequest<super::GetTreeRequest>,
        ) -> Result<
                tonic::Response<tonic::codec::Streaming<super::GetTreeResponse>>,
                tonic::Status,
            > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/build.bazel.remote.execution.v2.ContentAddressableStorage/GetTree",
            );
            self.inner.server_streaming(request.into_request(), path, codec).await
        }
    }
}
/// Generated client implementations.
pub mod capabilities_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// The Capabilities service may be used by remote execution clients to query
    /// various server properties, in order to self-configure or return meaningful
    /// error messages.
    ///
    /// The query may include a particular `instance_name`, in which case the values
    /// returned will pertain to that instance.
    #[derive(Debug, Clone)]
    pub struct CapabilitiesClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl CapabilitiesClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> CapabilitiesClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Default + Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> CapabilitiesClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            CapabilitiesClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with `gzip`.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        /// Enable decompressing responses with `gzip`.
        #[must_use]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        /// GetCapabilities returns the server capabilities configuration of the
        /// remote endpoint.
        /// Only the capabilities of the services supported by the endpoint will
        /// be returned:
        /// * Execution + CAS + Action Cache endpoints should return both
        ///   CacheCapabilities and ExecutionCapabilities.
        /// * Execution only endpoints should return ExecutionCapabilities.
        /// * CAS + Action Cache only endpoints should return CacheCapabilities.
        pub async fn get_capabilities(
            &mut self,
            request: impl tonic::IntoRequest<super::GetCapabilitiesRequest>,
        ) -> Result<tonic::Response<super::ServerCapabilities>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/build.bazel.remote.execution.v2.Capabilities/GetCapabilities",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod execution_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with ExecutionServer.
    #[async_trait]
    pub trait Execution: Send + Sync + 'static {
        ///Server streaming response type for the Execute method.
        type ExecuteStream: futures_core::Stream<
                Item = Result<
                    super::super::super::super::super::super::google::longrunning::Operation,
                    tonic::Status,
                >,
            >
            + Send
            + 'static;
        /// Execute an action remotely.
        ///
        /// In order to execute an action, the client must first upload all of the
        /// inputs, the
        /// [Command][build.bazel.remote.execution.v2.Command] to run, and the
        /// [Action][build.bazel.remote.execution.v2.Action] into the
        /// [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage].
        /// It then calls `Execute` with an `action_digest` referring to them. The
        /// server will run the action and eventually return the result.
        ///
        /// The input `Action`'s fields MUST meet the various canonicalization
        /// requirements specified in the documentation for their types so that it has
        /// the same digest as other logically equivalent `Action`s. The server MAY
        /// enforce the requirements and return errors if a non-canonical input is
        /// received. It MAY also proceed without verifying some or all of the
        /// requirements, such as for performance reasons. If the server does not
        /// verify the requirement, then it will treat the `Action` as distinct from
        /// another logically equivalent action if they hash differently.
        ///
        /// Returns a stream of
        /// [google.longrunning.Operation][google.longrunning.Operation] messages
        /// describing the resulting execution, with eventual `response`
        /// [ExecuteResponse][build.bazel.remote.execution.v2.ExecuteResponse]. The
        /// `metadata` on the operation is of type
        /// [ExecuteOperationMetadata][build.bazel.remote.execution.v2.ExecuteOperationMetadata].
        ///
        /// If the client remains connected after the first response is returned after
        /// the server, then updates are streamed as if the client had called
        /// [WaitExecution][build.bazel.remote.execution.v2.Execution.WaitExecution]
        /// until the execution completes or the request reaches an error. The
        /// operation can also be queried using [Operations
        /// API][google.longrunning.Operations.GetOperation].
        ///
        /// The server NEED NOT implement other methods or functionality of the
        /// Operations API.
        ///
        /// Errors discovered during creation of the `Operation` will be reported
        /// as gRPC Status errors, while errors that occurred while running the
        /// action will be reported in the `status` field of the `ExecuteResponse`. The
        /// server MUST NOT set the `error` field of the `Operation` proto.
        /// The possible errors include:
        ///
        /// * `INVALID_ARGUMENT`: One or more arguments are invalid.
        /// * `FAILED_PRECONDITION`: One or more errors occurred in setting up the
        ///   action requested, such as a missing input or command or no worker being
        ///   available. The client may be able to fix the errors and retry.
        /// * `RESOURCE_EXHAUSTED`: There is insufficient quota of some resource to run
        ///   the action.
        /// * `UNAVAILABLE`: Due to a transient condition, such as all workers being
        ///   occupied (and the server does not support a queue), the action could not
        ///   be started. The client should retry.
        /// * `INTERNAL`: An internal error occurred in the execution engine or the
        ///   worker.
        /// * `DEADLINE_EXCEEDED`: The execution timed out.
        /// * `CANCELLED`: The operation was cancelled by the client. This status is
        ///   only possible if the server implements the Operations API CancelOperation
        ///   method, and it was called for the current execution.
        ///
        /// In the case of a missing input or command, the server SHOULD additionally
        /// send a [PreconditionFailure][google.rpc.PreconditionFailure] error detail
        /// where, for each requested blob not present in the CAS, there is a
        /// `Violation` with a `type` of `MISSING` and a `subject` of
        /// `"blobs/{hash}/{size}"` indicating the digest of the missing blob.
        ///
        /// The server does not need to guarantee that a call to this method leads to
        /// at most one execution of the action. The server MAY execute the action
        /// multiple times, potentially in parallel. These redundant executions MAY
        /// continue to run, even if the operation is completed.
        async fn execute(
            &self,
            request: tonic::Request<super::ExecuteRequest>,
        ) -> Result<tonic::Response<Self::ExecuteStream>, tonic::Status>;
        ///Server streaming response type for the WaitExecution method.
        type WaitExecutionStream: futures_core::Stream<
                Item = Result<
                    super::super::super::super::super::super::google::longrunning::Operation,
                    tonic::Status,
                >,
            >
            + Send
            + 'static;
        /// Wait for an execution operation to complete. When the client initially
        /// makes the request, the server immediately responds with the current status
        /// of the execution. The server will leave the request stream open until the
        /// operation completes, and then respond with the completed operation. The
        /// server MAY choose to stream additional updates as execution progresses,
        /// such as to provide an update as to the state of the execution.
        async fn wait_execution(
            &self,
            request: tonic::Request<super::WaitExecutionRequest>,
        ) -> Result<tonic::Response<Self::WaitExecutionStream>, tonic::Status>;
    }
    /// The Remote Execution API is used to execute an
    /// [Action][build.bazel.remote.execution.v2.Action] on the remote
    /// workers.
    ///
    /// As with other services in the Remote Execution API, any call may return an
    /// error with a [RetryInfo][google.rpc.RetryInfo] error detail providing
    /// information about when the client should retry the request; clients SHOULD
    /// respect the information provided.
    #[derive(Debug)]
    pub struct ExecutionServer<T: Execution> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Execution> ExecutionServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ExecutionServer<T>
    where
        T: Execution,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/build.bazel.remote.execution.v2.Execution/Execute" => {
                    #[allow(non_camel_case_types)]
                    struct ExecuteSvc<T: Execution>(pub Arc<T>);
                    impl<
                        T: Execution,
                    > tonic::server::ServerStreamingService<super::ExecuteRequest>
                    for ExecuteSvc<T> {
                        type Response = super::super::super::super::super::super::google::longrunning::Operation;
                        type ResponseStream = T::ExecuteStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ExecuteRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).execute(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ExecuteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/build.bazel.remote.execution.v2.Execution/WaitExecution" => {
                    #[allow(non_camel_case_types)]
                    struct WaitExecutionSvc<T: Execution>(pub Arc<T>);
                    impl<
                        T: Execution,
                    > tonic::server::ServerStreamingService<super::WaitExecutionRequest>
                    for WaitExecutionSvc<T> {
                        type Response = super::super::super::super::super::super::google::longrunning::Operation;
                        type ResponseStream = T::WaitExecutionStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::WaitExecutionRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).wait_execution(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = WaitExecutionSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: Execution> Clone for ExecutionServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: Execution> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Execution> tonic::transport::NamedService for ExecutionServer<T> {
        const NAME: &'static str = "build.bazel.remote.execution.v2.Execution";
    }
}
/// Generated server implementations.
pub mod action_cache_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with ActionCacheServer.
    #[async_trait]
    pub trait ActionCache: Send + Sync + 'static {
        /// Retrieve a cached execution result.
        ///
        /// Implementations SHOULD ensure that any blobs referenced from the
        /// [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage]
        /// are available at the time of returning the
        /// [ActionResult][build.bazel.remote.execution.v2.ActionResult] and will be
        /// for some period of time afterwards. The lifetimes of the referenced blobs SHOULD be increased
        /// if necessary and applicable.
        ///
        /// Errors:
        ///
        /// * `NOT_FOUND`: The requested `ActionResult` is not in the cache.
        async fn get_action_result(
            &self,
            request: tonic::Request<super::GetActionResultRequest>,
        ) -> Result<tonic::Response<super::ActionResult>, tonic::Status>;
        /// Upload a new execution result.
        ///
        /// In order to allow the server to perform access control based on the type of
        /// action, and to assist with client debugging, the client MUST first upload
        /// the [Action][build.bazel.remote.execution.v2.Execution] that produced the
        /// result, along with its
        /// [Command][build.bazel.remote.execution.v2.Command], into the
        /// `ContentAddressableStorage`.
        ///
        /// Server implementations MAY modify the
        /// `UpdateActionResultRequest.action_result` and return an equivalent value.
        ///
        /// Errors:
        ///
        /// * `INVALID_ARGUMENT`: One or more arguments are invalid.
        /// * `FAILED_PRECONDITION`: One or more errors occurred in updating the
        ///   action result, such as a missing command or action.
        /// * `RESOURCE_EXHAUSTED`: There is insufficient storage space to add the
        ///   entry to the cache.
        async fn update_action_result(
            &self,
            request: tonic::Request<super::UpdateActionResultRequest>,
        ) -> Result<tonic::Response<super::ActionResult>, tonic::Status>;
    }
    /// The action cache API is used to query whether a given action has already been
    /// performed and, if so, retrieve its result. Unlike the
    /// [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage],
    /// which addresses blobs by their own content, the action cache addresses the
    /// [ActionResult][build.bazel.remote.execution.v2.ActionResult] by a
    /// digest of the encoded [Action][build.bazel.remote.execution.v2.Action]
    /// which produced them.
    ///
    /// The lifetime of entries in the action cache is implementation-specific, but
    /// the server SHOULD assume that more recently used entries are more likely to
    /// be used again.
    ///
    /// As with other services in the Remote Execution API, any call may return an
    /// error with a [RetryInfo][google.rpc.RetryInfo] error detail providing
    /// information about when the client should retry the request; clients SHOULD
    /// respect the information provided.
    #[derive(Debug)]
    pub struct ActionCacheServer<T: ActionCache> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: ActionCache> ActionCacheServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ActionCacheServer<T>
    where
        T: ActionCache,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/build.bazel.remote.execution.v2.ActionCache/GetActionResult" => {
                    #[allow(non_camel_case_types)]
                    struct GetActionResultSvc<T: ActionCache>(pub Arc<T>);
                    impl<
                        T: ActionCache,
                    > tonic::server::UnaryService<super::GetActionResultRequest>
                    for GetActionResultSvc<T> {
                        type Response = super::ActionResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetActionResultRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).get_action_result(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetActionResultSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/build.bazel.remote.execution.v2.ActionCache/UpdateActionResult" => {
                    #[allow(non_camel_case_types)]
                    struct UpdateActionResultSvc<T: ActionCache>(pub Arc<T>);
                    impl<
                        T: ActionCache,
                    > tonic::server::UnaryService<super::UpdateActionResultRequest>
                    for UpdateActionResultSvc<T> {
                        type Response = super::ActionResult;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UpdateActionResultRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).update_action_result(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UpdateActionResultSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: ActionCache> Clone for ActionCacheServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: ActionCache> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: ActionCache> tonic::transport::NamedService for ActionCacheServer<T> {
        const NAME: &'static str = "build.bazel.remote.execution.v2.ActionCache";
    }
}
/// Generated server implementations.
pub mod content_addressable_storage_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with ContentAddressableStorageServer.
    #[async_trait]
    pub trait ContentAddressableStorage: Send + Sync + 'static {
        /// Determine if blobs are present in the CAS.
        ///
        /// Clients can use this API before uploading blobs to determine which ones are
        /// already present in the CAS and do not need to be uploaded again.
        ///
        /// Servers SHOULD increase the lifetimes of the referenced blobs if necessary and
        /// applicable.
        ///
        /// There are no method-specific errors.
        async fn find_missing_blobs(
            &self,
            request: tonic::Request<super::FindMissingBlobsRequest>,
        ) -> Result<tonic::Response<super::FindMissingBlobsResponse>, tonic::Status>;
        /// Upload many blobs at once.
        ///
        /// The server may enforce a limit of the combined total size of blobs
        /// to be uploaded using this API. This limit may be obtained using the
        /// [Capabilities][build.bazel.remote.execution.v2.Capabilities] API.
        /// Requests exceeding the limit should either be split into smaller
        /// chunks or uploaded using the
        /// [ByteStream API][google.bytestream.ByteStream], as appropriate.
        ///
        /// This request is equivalent to calling a Bytestream `Write` request
        /// on each individual blob, in parallel. The requests may succeed or fail
        /// independently.
        ///
        /// Errors:
        ///
        /// * `INVALID_ARGUMENT`: The client attempted to upload more than the
        ///   server supported limit.
        ///
        /// Individual requests may return the following errors, additionally:
        ///
        /// * `RESOURCE_EXHAUSTED`: There is insufficient disk quota to store the blob.
        /// * `INVALID_ARGUMENT`: The
        /// [Digest][build.bazel.remote.execution.v2.Digest] does not match the
        /// provided data.
        async fn batch_update_blobs(
            &self,
            request: tonic::Request<super::BatchUpdateBlobsRequest>,
        ) -> Result<tonic::Response<super::BatchUpdateBlobsResponse>, tonic::Status>;
        /// Download many blobs at once.
        ///
        /// The server may enforce a limit of the combined total size of blobs
        /// to be downloaded using this API. This limit may be obtained using the
        /// [Capabilities][build.bazel.remote.execution.v2.Capabilities] API.
        /// Requests exceeding the limit should either be split into smaller
        /// chunks or downloaded using the
        /// [ByteStream API][google.bytestream.ByteStream], as appropriate.
        ///
        /// This request is equivalent to calling a Bytestream `Read` request
        /// on each individual blob, in parallel. The requests may succeed or fail
        /// independently.
        ///
        /// Errors:
        ///
        /// * `INVALID_ARGUMENT`: The client attempted to read more than the
        ///   server supported limit.
        ///
        /// Every error on individual read will be returned in the corresponding digest
        /// status.
        async fn batch_read_blobs(
            &self,
            request: tonic::Request<super::BatchReadBlobsRequest>,
        ) -> Result<tonic::Response<super::BatchReadBlobsResponse>, tonic::Status>;
        ///Server streaming response type for the GetTree method.
        type GetTreeStream: futures_core::Stream<
                Item = Result<super::GetTreeResponse, tonic::Status>,
            >
            + Send
            + 'static;
        /// Fetch the entire directory tree rooted at a node.
        ///
        /// This request must be targeted at a
        /// [Directory][build.bazel.remote.execution.v2.Directory] stored in the
        /// [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage]
        /// (CAS). The server will enumerate the `Directory` tree recursively and
        /// return every node descended from the root.
        ///
        /// The GetTreeRequest.page_token parameter can be used to skip ahead in
        /// the stream (e.g. when retrying a partially completed and aborted request),
        /// by setting it to a value taken from GetTreeResponse.next_page_token of the
        /// last successfully processed GetTreeResponse).
        ///
        /// The exact traversal order is unspecified and, unless retrieving subsequent
        /// pages from an earlier request, is not guaranteed to be stable across
        /// multiple invocations of `GetTree`.
        ///
        /// If part of the tree is missing from the CAS, the server will return the
        /// portion present and omit the rest.
        ///
        /// Errors:
        ///
        /// * `NOT_FOUND`: The requested tree root is not present in the CAS.
        async fn get_tree(
            &self,
            request: tonic::Request<super::GetTreeRequest>,
        ) -> Result<tonic::Response<Self::GetTreeStream>, tonic::Status>;
    }
    /// The CAS (content-addressable storage) is used to store the inputs to and
    /// outputs from the execution service. Each piece of content is addressed by the
    /// digest of its binary data.
    ///
    /// Most of the binary data stored in the CAS is opaque to the execution engine,
    /// and is only used as a communication medium. In order to build an
    /// [Action][build.bazel.remote.execution.v2.Action],
    /// however, the client will need to also upload the
    /// [Command][build.bazel.remote.execution.v2.Command] and input root
    /// [Directory][build.bazel.remote.execution.v2.Directory] for the Action.
    /// The Command and Directory messages must be marshalled to wire format and then
    /// uploaded under the hash as with any other piece of content. In practice, the
    /// input root directory is likely to refer to other Directories in its
    /// hierarchy, which must also each be uploaded on their own.
    ///
    /// For small file uploads the client should group them together and call
    /// [BatchUpdateBlobs][build.bazel.remote.execution.v2.ContentAddressableStorage.BatchUpdateBlobs].
    /// For large uploads, the client must use the
    /// [Write method][google.bytestream.ByteStream.Write] of the ByteStream API. The
    /// `resource_name` is `{instance_name}/uploads/{uuid}/blobs/{hash}/{size}`,
    /// where `instance_name` is as described in the next paragraph, `uuid` is a
    /// version 4 UUID generated by the client, and `hash` and `size` are the
    /// [Digest][build.bazel.remote.execution.v2.Digest] of the blob. The
    /// `uuid` is used only to avoid collisions when multiple clients try to upload
    /// the same file (or the same client tries to upload the file multiple times at
    /// once on different threads), so the client MAY reuse the `uuid` for uploading
    /// different blobs. The `resource_name` may optionally have a trailing filename
    /// (or other metadata) for a client to use if it is storing URLs, as in
    /// `{instance}/uploads/{uuid}/blobs/{hash}/{size}/foo/bar/baz.cc`. Anything
    /// after the `size` is ignored.
    ///
    /// A single server MAY support multiple instances of the execution system, each
    /// with their own workers, storage, cache, etc. The exact relationship between
    /// instances is up to the server. If the server does, then the `instance_name`
    /// is an identifier, possibly containing multiple path segments, used to
    /// distinguish between the various instances on the server, in a manner defined
    /// by the server. For servers which do not support multiple instances, then the
    /// `instance_name` is the empty path and the leading slash is omitted, so that
    /// the `resource_name` becomes `uploads/{uuid}/blobs/{hash}/{size}`.
    /// To simplify parsing, a path segment cannot equal any of the following
    /// keywords: `blobs`, `uploads`, `actions`, `actionResults`, `operations` and
    /// `capabilities`.
    ///
    /// When attempting an upload, if another client has already completed the upload
    /// (which may occur in the middle of a single upload if another client uploads
    /// the same blob concurrently), the request will terminate immediately with
    /// a response whose `committed_size` is the full size of the uploaded file
    /// (regardless of how much data was transmitted by the client). If the client
    /// completes the upload but the
    /// [Digest][build.bazel.remote.execution.v2.Digest] does not match, an
    /// `INVALID_ARGUMENT` error will be returned. In either case, the client should
    /// not attempt to retry the upload.
    ///
    /// For downloading blobs, the client must use the
    /// [Read method][google.bytestream.ByteStream.Read] of the ByteStream API, with
    /// a `resource_name` of `"{instance_name}/blobs/{hash}/{size}"`, where
    /// `instance_name` is the instance name (see above), and `hash` and `size` are
    /// the [Digest][build.bazel.remote.execution.v2.Digest] of the blob.
    ///
    /// The lifetime of entries in the CAS is implementation specific, but it SHOULD
    /// be long enough to allow for newly-added and recently looked-up entries to be
    /// used in subsequent calls (e.g. to
    /// [Execute][build.bazel.remote.execution.v2.Execution.Execute]).
    ///
    /// Servers MUST behave as though empty blobs are always available, even if they
    /// have not been uploaded. Clients MAY optimize away the uploading or
    /// downloading of empty blobs.
    ///
    /// As with other services in the Remote Execution API, any call may return an
    /// error with a [RetryInfo][google.rpc.RetryInfo] error detail providing
    /// information about when the client should retry the request; clients SHOULD
    /// respect the information provided.
    #[derive(Debug)]
    pub struct ContentAddressableStorageServer<T: ContentAddressableStorage> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: ContentAddressableStorage> ContentAddressableStorageServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>>
    for ContentAddressableStorageServer<T>
    where
        T: ContentAddressableStorage,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs" => {
                    #[allow(non_camel_case_types)]
                    struct FindMissingBlobsSvc<T: ContentAddressableStorage>(pub Arc<T>);
                    impl<
                        T: ContentAddressableStorage,
                    > tonic::server::UnaryService<super::FindMissingBlobsRequest>
                    for FindMissingBlobsSvc<T> {
                        type Response = super::FindMissingBlobsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::FindMissingBlobsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).find_missing_blobs(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = FindMissingBlobsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs" => {
                    #[allow(non_camel_case_types)]
                    struct BatchUpdateBlobsSvc<T: ContentAddressableStorage>(pub Arc<T>);
                    impl<
                        T: ContentAddressableStorage,
                    > tonic::server::UnaryService<super::BatchUpdateBlobsRequest>
                    for BatchUpdateBlobsSvc<T> {
                        type Response = super::BatchUpdateBlobsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::BatchUpdateBlobsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).batch_update_blobs(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = BatchUpdateBlobsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs" => {
                    #[allow(non_camel_case_types)]
                    struct BatchReadBlobsSvc<T: ContentAddressableStorage>(pub Arc<T>);
                    impl<
                        T: ContentAddressableStorage,
                    > tonic::server::UnaryService<super::BatchReadBlobsRequest>
                    for BatchReadBlobsSvc<T> {
                        type Response = super::BatchReadBlobsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::BatchReadBlobsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).batch_read_blobs(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = BatchReadBlobsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/build.bazel.remote.execution.v2.ContentAddressableStorage/GetTree" => {
                    #[allow(non_camel_case_types)]
                    struct GetTreeSvc<T: ContentAddressableStorage>(pub Arc<T>);
                    impl<
                        T: ContentAddressableStorage,
                    > tonic::server::ServerStreamingService<super::GetTreeRequest>
                    for GetTreeSvc<T> {
                        type Response = super::GetTreeResponse;
                        type ResponseStream = T::GetTreeStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetTreeRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get_tree(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetTreeSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: ContentAddressableStorage> Clone for ContentAddressableStorageServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: ContentAddressableStorage> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: ContentAddressableStorage> tonic::transport::NamedService
    for ContentAddressableStorageServer<T> {
        const NAME: &'static str = "build.bazel.remote.execution.v2.ContentAddressableStorage";
    }
}
/// Generated server implementations.
pub mod capabilities_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with CapabilitiesServer.
    #[async_trait]
    pub trait Capabilities: Send + Sync + 'static {
        /// GetCapabilities returns the server capabilities configuration of the
        /// remote endpoint.
        /// Only the capabilities of the services supported by the endpoint will
        /// be returned:
        /// * Execution + CAS + Action Cache endpoints should return both
        ///   CacheCapabilities and ExecutionCapabilities.
        /// * Execution only endpoints should return ExecutionCapabilities.
        /// * CAS + Action Cache only endpoints should return CacheCapabilities.
        async fn get_capabilities(
            &self,
            request: tonic::Request<super::GetCapabilitiesRequest>,
        ) -> Result<tonic::Response<super::ServerCapabilities>, tonic::Status>;
    }
    /// The Capabilities service may be used by remote execution clients to query
    /// various server properties, in order to self-configure or return meaningful
    /// error messages.
    ///
    /// The query may include a particular `instance_name`, in which case the values
    /// returned will pertain to that instance.
    #[derive(Debug)]
    pub struct CapabilitiesServer<T: Capabilities> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Capabilities> CapabilitiesServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for CapabilitiesServer<T>
    where
        T: Capabilities,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/build.bazel.remote.execution.v2.Capabilities/GetCapabilities" => {
                    #[allow(non_camel_case_types)]
                    struct GetCapabilitiesSvc<T: Capabilities>(pub Arc<T>);
                    impl<
                        T: Capabilities,
                    > tonic::server::UnaryService<super::GetCapabilitiesRequest>
                    for GetCapabilitiesSvc<T> {
                        type Response = super::ServerCapabilities;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetCapabilitiesRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).get_capabilities(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetCapabilitiesSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: Capabilities> Clone for CapabilitiesServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: Capabilities> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Capabilities> tonic::transport::NamedService for CapabilitiesServer<T> {
        const NAME: &'static str = "build.bazel.remote.execution.v2.Capabilities";
    }
}
