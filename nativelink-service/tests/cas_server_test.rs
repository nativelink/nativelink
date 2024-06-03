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

use std::pin::Pin;
use std::sync::Arc;

use maplit::hashmap;
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::content_addressable_storage_server::ContentAddressableStorage;
use nativelink_proto::build::bazel::remote::execution::v2::{compressor, digest_function, Digest};
use nativelink_proto::google::rpc::Status as GrpcStatus;
use nativelink_service::cas_server::CasServer;
use nativelink_store::ac_utils::serialize_and_upload_message;
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::common::DigestInfo;
use prometheus_client::registry::Registry;
use tonic::Request;

const INSTANCE_NAME: &str = "foo_instance_name";
const HASH1: &str = "0123456789abcdef000000000000000000000000000000000123456789abcdef";
const HASH2: &str = "9993456789abcdef000000000000000000000000000000000123456789abc999";
const HASH3: &str = "7773456789abcdef000000000000000000000000000000000123456789abc777";
const BAD_HASH: &str = "BAD_HASH";

async fn make_store_manager() -> Result<Arc<StoreManager>, Error> {
    let store_manager = Arc::new(StoreManager::new());
    store_manager.add_store(
        "main_cas",
        store_factory(
            &nativelink_config::stores::StoreConfigOptions::memory(
                nativelink_config::stores::MemoryStore::default(),
            ),
            &store_manager,
            Some(&mut <Registry>::default()),
            None,
        )
        .await?,
    );
    Ok(store_manager)
}

fn make_cas_server(store_manager: &StoreManager) -> Result<CasServer, Error> {
    CasServer::new(
        &hashmap! {
            "foo_instance_name".to_string() => nativelink_config::cas_server::CasStoreConfig{
                cas_store: "main_cas".to_string(),
            }
        },
        store_manager,
    )
}

#[cfg(test)]
mod find_missing_blobs {
    use nativelink_proto::build::bazel::remote::execution::v2::FindMissingBlobsRequest;
    use pretty_assertions::assert_eq; // Must be declared in every module.

    use super::*;

    #[nativelink_test]
    async fn empty_store() -> Result<(), Box<dyn std::error::Error>> {
        let store_manager = make_store_manager().await?;
        let cas_server = make_cas_server(&store_manager)?;

        let raw_response = cas_server
            .find_missing_blobs(Request::new(FindMissingBlobsRequest {
                instance_name: INSTANCE_NAME.to_string(),
                blob_digests: vec![Digest {
                    hash: HASH1.to_string(),
                    size_bytes: 0,
                }],
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert!(raw_response.is_ok());
        let response = raw_response.unwrap().into_inner();
        assert_eq!(response.missing_blob_digests.len(), 1);
        Ok(())
    }

    #[nativelink_test]
    async fn store_one_item_existence() -> Result<(), Box<dyn std::error::Error>> {
        let store_manager = make_store_manager().await?;
        let cas_server = make_cas_server(&store_manager)?;
        let store_owned = store_manager.get_store("main_cas").unwrap();

        const VALUE: &str = "1";

        let store = Pin::new(store_owned.as_ref());
        store
            .update_oneshot(DigestInfo::try_new(HASH1, VALUE.len())?, VALUE.into())
            .await?;
        let raw_response = cas_server
            .find_missing_blobs(Request::new(FindMissingBlobsRequest {
                instance_name: INSTANCE_NAME.to_string(),
                blob_digests: vec![Digest {
                    hash: HASH1.to_string(),
                    size_bytes: VALUE.len() as i64,
                }],
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert!(raw_response.is_ok());
        let response = raw_response.unwrap().into_inner();
        assert_eq!(response.missing_blob_digests.len(), 0); // All items should have been found.
        Ok(())
    }

    #[nativelink_test]
    async fn has_three_requests_one_bad_hash() -> Result<(), Box<dyn std::error::Error>> {
        let store_manager = make_store_manager().await?;
        let cas_server = make_cas_server(&store_manager)?;
        let store_owned = store_manager.get_store("main_cas").unwrap();

        const VALUE: &str = "1";

        let store = Pin::new(store_owned.as_ref());
        store
            .update_oneshot(DigestInfo::try_new(HASH1, VALUE.len())?, VALUE.into())
            .await?;
        let raw_response = cas_server
            .find_missing_blobs(Request::new(FindMissingBlobsRequest {
                instance_name: INSTANCE_NAME.to_string(),
                blob_digests: vec![
                    Digest {
                        hash: HASH1.to_string(),
                        size_bytes: VALUE.len() as i64,
                    },
                    Digest {
                        hash: BAD_HASH.to_string(),
                        size_bytes: VALUE.len() as i64,
                    },
                    Digest {
                        hash: HASH1.to_string(),
                        size_bytes: VALUE.len() as i64,
                    },
                ],
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        let error = raw_response.unwrap_err();
        assert!(
            error.to_string().contains("Invalid sha256 hash: BAD_HASH"),
            "'Invalid sha256 hash: BAD_HASH' not found in: {error:?}"
        );
        Ok(())
    }
}

#[cfg(test)]
mod batch_update_blobs {
    use nativelink_proto::build::bazel::remote::execution::v2::{
        batch_update_blobs_request, batch_update_blobs_response, BatchUpdateBlobsRequest,
        BatchUpdateBlobsResponse,
    };
    use pretty_assertions::assert_eq; // Must be declared in every module.

    use super::*;

    #[nativelink_test]
    async fn update_existing_item() -> Result<(), Box<dyn std::error::Error>> {
        let store_manager = make_store_manager().await?;
        let cas_server = make_cas_server(&store_manager)?;
        let store_owned = store_manager.get_store("main_cas").unwrap();

        const VALUE1: &str = "1";
        const VALUE2: &str = "2";

        let digest = Digest {
            hash: HASH1.to_string(),
            size_bytes: VALUE2.len() as i64,
        };

        let store = Pin::new(store_owned.as_ref());
        store
            .update_oneshot(DigestInfo::try_new(HASH1, VALUE1.len())?, VALUE1.into())
            .await
            .expect("Update should have succeeded");

        let raw_response = cas_server
            .batch_update_blobs(Request::new(BatchUpdateBlobsRequest {
                instance_name: INSTANCE_NAME.to_string(),
                requests: vec![batch_update_blobs_request::Request {
                    digest: Some(digest.clone()),
                    data: VALUE2.into(),
                    compressor: compressor::Value::Identity.into(),
                }],
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert!(raw_response.is_ok());
        assert_eq!(
            raw_response.unwrap().into_inner(),
            BatchUpdateBlobsResponse {
                responses: vec![batch_update_blobs_response::Response {
                    digest: Some(digest),
                    status: Some(GrpcStatus {
                        code: 0, // Status Ok.
                        message: "".to_string(),
                        details: vec![],
                    }),
                },],
            }
        );
        let new_data = store
            .get_part_unchunked(DigestInfo::try_new(HASH1, VALUE1.len())?, 0, None)
            .await
            .expect("Get should have succeeded");
        assert_eq!(
            new_data,
            VALUE2.as_bytes(),
            "Expected store to have been updated to new value"
        );
        Ok(())
    }
}

#[cfg(test)]
mod batch_read_blobs {
    use nativelink_proto::build::bazel::remote::execution::v2::{
        batch_read_blobs_response, BatchReadBlobsRequest, BatchReadBlobsResponse,
    };
    use pretty_assertions::assert_eq; // Must be declared in every module.
    use tonic::Code;

    use super::*;

    #[nativelink_test]
    async fn batch_read_blobs_read_two_blobs_success_one_fail(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let store_manager = make_store_manager().await?;
        let cas_server = make_cas_server(&store_manager)?;
        let store_owned = store_manager.get_store("main_cas").unwrap();

        const VALUE1: &str = "1";
        const VALUE2: &str = "23";

        let digest1 = Digest {
            hash: HASH1.to_string(),
            size_bytes: VALUE1.len() as i64,
        };
        let digest2 = Digest {
            hash: HASH2.to_string(),
            size_bytes: VALUE2.len() as i64,
        };
        {
            // Insert dummy data.
            let store = Pin::new(store_owned.as_ref());
            store
                .update_oneshot(DigestInfo::try_new(HASH1, VALUE1.len())?, VALUE1.into())
                .await
                .expect("Update should have succeeded");
            store
                .update_oneshot(DigestInfo::try_new(HASH2, VALUE2.len())?, VALUE2.into())
                .await
                .expect("Update should have succeeded");
        }
        {
            // Read two blobs and additional blob should come back not found.
            let digest3 = Digest {
                hash: HASH3.to_string(),
                size_bytes: 3,
            };
            let raw_response = cas_server
                .batch_read_blobs(Request::new(BatchReadBlobsRequest {
                    instance_name: INSTANCE_NAME.to_string(),
                    digests: vec![digest1.clone(), digest2.clone(), digest3.clone()],
                    acceptable_compressors: vec![compressor::Value::Identity.into()],
                    digest_function: digest_function::Value::Sha256.into(),
                }))
                .await;
            assert!(raw_response.is_ok());
            assert_eq!(
                raw_response.unwrap().into_inner(),
                BatchReadBlobsResponse {
                    responses: vec![
                        batch_read_blobs_response::Response {
                            digest: Some(digest1),
                            data: VALUE1.into(),
                            status: Some(GrpcStatus {
                                code: 0, // Status Ok.
                                message: "".to_string(),
                                details: vec![],
                            }),
                            compressor: compressor::Value::Identity.into(),
                        },
                        batch_read_blobs_response::Response {
                            digest: Some(digest2),
                            data: VALUE2.into(),
                            status: Some(GrpcStatus {
                                code: 0, // Status Ok.
                                message: "".to_string(),
                                details: vec![],
                            }),
                            compressor: compressor::Value::Identity.into(),
                        },
                        batch_read_blobs_response::Response {
                            digest: Some(digest3.clone()),
                            data: vec![].into(),
                            status: Some(GrpcStatus {
                                code: Code::NotFound as i32,
                                message: format!("Hash {} not found", digest3.hash),
                                details: vec![],
                            }),
                            compressor: compressor::Value::Identity.into(),
                        }
                    ],
                }
            );
        }
        Ok(())
    }
}

mod get_tree {
    use futures::StreamExt;
    use nativelink_proto::build::bazel::remote::execution::v2::{
        digest_function, Directory, DirectoryNode, GetTreeRequest, GetTreeResponse, NodeProperties,
    };
    use nativelink_util::digest_hasher::DigestHasherFunc;
    use nativelink_util::store_trait::Store;
    use pretty_assertions::assert_eq; // Must be declared in every module.
    use prost_types::Timestamp;

    use super::*;

    struct SetupDirectoryResult {
        root_directory: Directory,
        root_directory_digest_info: DigestInfo,
        sub_directories: Vec<Directory>,
        sub_directory_digest_infos: Vec<DigestInfo>,
    }
    async fn setup_directory_structure(
        store_pinned: Pin<&dyn Store>,
    ) -> Result<SetupDirectoryResult, Error> {
        // Set up 5 sub-directories.
        const SUB_DIRECTORIES_LENGTH: i32 = 5;
        let mut sub_directory_nodes: Vec<DirectoryNode> = vec![];
        let mut sub_directories: Vec<Directory> = vec![];
        let mut sub_directory_digest_infos: Vec<DigestInfo> = vec![];

        for i in 0..SUB_DIRECTORIES_LENGTH {
            let sub_directory: Directory = Directory {
                files: vec![],
                directories: vec![],
                symlinks: vec![],
                node_properties: Some(NodeProperties {
                    properties: vec![],
                    mtime: Some(Timestamp {
                        seconds: i as i64,
                        nanos: 0,
                    }),
                    unix_mode: Some(0o755),
                }),
            };
            let sub_directory_digest_info: DigestInfo = serialize_and_upload_message(
                &sub_directory,
                store_pinned,
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;
            sub_directory_digest_infos.push(sub_directory_digest_info);
            sub_directory_nodes.push(DirectoryNode {
                name: format!("sub_directory_{i}"),
                digest: Some(sub_directory_digest_info.into()),
            });
            sub_directories.push(sub_directory);
        }

        // Set up a root directory.
        let root_directory: Directory = Directory {
            files: vec![],
            directories: sub_directory_nodes,
            symlinks: vec![],
            node_properties: None,
        };
        let root_directory_digest_info: DigestInfo = serialize_and_upload_message(
            &root_directory,
            store_pinned,
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;

        Ok(SetupDirectoryResult {
            root_directory,
            root_directory_digest_info,
            sub_directories,
            sub_directory_digest_infos,
        })
    }

    #[nativelink_test]
    async fn get_tree_read_directories_without_paging() -> Result<(), Box<dyn std::error::Error>> {
        let store_manager = make_store_manager().await?;
        let cas_server = make_cas_server(&store_manager)?;
        let store_owned = store_manager.get_store("main_cas").unwrap();
        let store_pinned = Pin::new(store_owned.as_ref());

        // Setup directory structure.
        let SetupDirectoryResult {
            root_directory,
            root_directory_digest_info,
            sub_directories,
            sub_directory_digest_infos: _,
        } = setup_directory_structure(store_pinned).await?;

        // Must work when paging is disabled ( `page_size` is 0 ).
        // It reads all directories at once.
        let raw_response = cas_server
            .get_tree(Request::new(GetTreeRequest {
                instance_name: INSTANCE_NAME.to_string(),
                page_size: 0,
                page_token: format!(
                    "{}-{}",
                    root_directory_digest_info.hash_str(),
                    root_directory_digest_info.size_bytes
                ),
                root_digest: Some(root_directory_digest_info.into()),
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert!(raw_response.is_ok());
        assert_eq!(
            raw_response
                .unwrap()
                .into_inner()
                .filter_map(|x| async move { Some(x.unwrap()) })
                .collect::<Vec<_>>()
                .await,
            vec![GetTreeResponse {
                directories: vec![
                    root_directory.clone(),
                    sub_directories[0].clone(),
                    sub_directories[1].clone(),
                    sub_directories[2].clone(),
                    sub_directories[3].clone(),
                    sub_directories[4].clone()
                ],
                next_page_token: String::new()
            }]
        );

        Ok(())
    }

    #[nativelink_test]
    async fn get_tree_read_directories_with_paging() -> Result<(), Box<dyn std::error::Error>> {
        let store_manager = make_store_manager().await?;
        let cas_server = make_cas_server(&store_manager)?;
        let store_owned = store_manager.get_store("main_cas").unwrap();
        let store_pinned = Pin::new(store_owned.as_ref());

        // Setup directory structure.
        let SetupDirectoryResult {
            root_directory,
            root_directory_digest_info,
            sub_directories,
            sub_directory_digest_infos,
        } = setup_directory_structure(store_pinned).await?;

        // Must work when paging is enabled ( `page_size` is 2 ).
        // First, it reads `root_directory` and `sub_directory[0]`.
        // Then, it reads `sub_directory[1]` and `sub_directory[2]`.
        // Finally, it reads `sub_directory[3]` and `sub_directory[4]`.
        let raw_response = cas_server
            .get_tree(Request::new(GetTreeRequest {
                instance_name: INSTANCE_NAME.to_string(),
                page_size: 2,
                page_token: format!(
                    "{}-{}",
                    root_directory_digest_info.hash_str(),
                    root_directory_digest_info.size_bytes
                ),
                root_digest: Some(root_directory_digest_info.into()),
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert!(raw_response.is_ok());
        assert_eq!(
            raw_response
                .unwrap()
                .into_inner()
                .filter_map(|x| async move { Some(x.unwrap()) })
                .collect::<Vec<_>>()
                .await,
            vec![GetTreeResponse {
                directories: vec![root_directory.clone(), sub_directories[0].clone()],
                next_page_token: format!(
                    "{}-{}",
                    sub_directory_digest_infos[1].hash_str(),
                    sub_directory_digest_infos[1].size_bytes
                ),
            }]
        );
        let raw_response = cas_server
            .get_tree(Request::new(GetTreeRequest {
                instance_name: INSTANCE_NAME.to_string(),
                page_size: 2,
                page_token: format!(
                    "{}-{}",
                    sub_directory_digest_infos[1].hash_str(),
                    sub_directory_digest_infos[1].size_bytes
                ),
                root_digest: Some(root_directory_digest_info.into()),
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert!(raw_response.is_ok());
        assert_eq!(
            raw_response
                .unwrap()
                .into_inner()
                .filter_map(|x| async move { Some(x.unwrap()) })
                .collect::<Vec<_>>()
                .await,
            vec![GetTreeResponse {
                directories: vec![sub_directories[1].clone(), sub_directories[2].clone()],
                next_page_token: format!(
                    "{}-{}",
                    sub_directory_digest_infos[3].hash_str(),
                    sub_directory_digest_infos[3].size_bytes
                ),
            }]
        );
        let raw_response = cas_server
            .get_tree(Request::new(GetTreeRequest {
                instance_name: INSTANCE_NAME.to_string(),
                page_size: 2,
                page_token: format!(
                    "{}-{}",
                    sub_directory_digest_infos[3].hash_str(),
                    sub_directory_digest_infos[3].size_bytes
                ),
                root_digest: Some(root_directory_digest_info.into()),
                digest_function: digest_function::Value::Sha256.into(),
            }))
            .await;
        assert!(raw_response.is_ok());
        assert_eq!(
            raw_response
                .unwrap()
                .into_inner()
                .filter_map(|x| async move { Some(x.unwrap()) })
                .collect::<Vec<_>>()
                .await,
            vec![GetTreeResponse {
                directories: vec![sub_directories[3].clone(), sub_directories[4].clone()],
                next_page_token: String::new(),
            }]
        );

        Ok(())
    }
}
#[cfg(test)]
mod end_to_end {
    use nativelink_proto::build::bazel::remote::execution::v2::{
        batch_update_blobs_request, batch_update_blobs_response, BatchUpdateBlobsRequest,
        BatchUpdateBlobsResponse, FindMissingBlobsRequest,
    };
    use pretty_assertions::assert_eq; // Must be declared in every module.

    use super::*;

    #[nativelink_test]
    async fn batch_update_blobs_two_items_existence_with_third_missing(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let store_manager = make_store_manager().await?;
        let cas_server = make_cas_server(&store_manager)?;

        const VALUE1: &str = "1";
        const VALUE2: &str = "23";

        let digest1 = Digest {
            hash: HASH1.to_string(),
            size_bytes: VALUE1.len() as i64,
        };
        let digest2 = Digest {
            hash: HASH2.to_string(),
            size_bytes: VALUE2.len() as i64,
        };

        {
            // Send update to insert two entries into backend.
            let raw_response = cas_server
                .batch_update_blobs(Request::new(BatchUpdateBlobsRequest {
                    instance_name: INSTANCE_NAME.to_string(),
                    requests: vec![
                        batch_update_blobs_request::Request {
                            digest: Some(digest1.clone()),
                            data: VALUE1.into(),
                            compressor: compressor::Value::Identity.into(),
                        },
                        batch_update_blobs_request::Request {
                            digest: Some(digest2.clone()),
                            data: VALUE2.into(),
                            compressor: compressor::Value::Identity.into(),
                        },
                    ],
                    digest_function: digest_function::Value::Sha256.into(),
                }))
                .await;
            assert!(raw_response.is_ok());
            assert_eq!(
                raw_response.unwrap().into_inner(),
                BatchUpdateBlobsResponse {
                    responses: vec![
                        batch_update_blobs_response::Response {
                            digest: Some(digest1),
                            status: Some(GrpcStatus {
                                code: 0, // Status Ok.
                                message: "".to_string(),
                                details: vec![],
                            }),
                        },
                        batch_update_blobs_response::Response {
                            digest: Some(digest2),
                            status: Some(GrpcStatus {
                                code: 0, // Status Ok.
                                message: "".to_string(),
                                details: vec![],
                            }),
                        }
                    ],
                }
            );
        }
        {
            // Query the backend for inserted entries plus one that is not
            // present and ensure it only returns the one that is missing.
            let missing_digest = Digest {
                hash: HASH3.to_string(),
                size_bytes: 1,
            };
            let raw_response = cas_server
                .find_missing_blobs(Request::new(FindMissingBlobsRequest {
                    instance_name: INSTANCE_NAME.to_string(),
                    blob_digests: vec![
                        Digest {
                            hash: HASH1.to_string(),
                            size_bytes: VALUE1.len() as i64,
                        },
                        missing_digest.clone(),
                        Digest {
                            hash: HASH2.to_string(),
                            size_bytes: VALUE2.len() as i64,
                        },
                    ],
                    digest_function: digest_function::Value::Sha256.into(),
                }))
                .await;
            assert!(raw_response.is_ok());
            let response = raw_response.unwrap().into_inner();
            assert_eq!(response.missing_blob_digests, vec![missing_digest]);
        }
        Ok(())
    }
}
