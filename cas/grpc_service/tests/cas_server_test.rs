// Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::pin::Pin;
use std::sync::Arc;

use maplit::hashmap;
use tonic::Request;

use proto::build::bazel::remote::execution::v2::{
    content_addressable_storage_server::ContentAddressableStorage, Digest,
};
use proto::google::rpc::Status as GrpcStatus;

use cas_server::CasServer;
use common::DigestInfo;
use config;
use default_store_factory::store_factory;
use error::Error;
use store::StoreManager;

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
            &config::backends::StoreConfig::memory(config::backends::MemoryStore::default()),
            &store_manager,
        )
        .await?,
    );
    Ok(store_manager)
}

fn make_cas_server(store_manager: &StoreManager) -> Result<CasServer, Error> {
    CasServer::new(
        &hashmap! {
            "foo_instance_name".to_string() => config::cas_server::CasStoreConfig{
                cas_store: "main_cas".to_string(),
            }
        },
        &store_manager,
    )
}

#[cfg(test)]
mod find_missing_blobs {
    use super::*;
    use pretty_assertions::assert_eq; // Must be declared in every module.

    use proto::build::bazel::remote::execution::v2::FindMissingBlobsRequest;

    #[tokio::test]
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
            }))
            .await;
        assert!(raw_response.is_ok());
        let response = raw_response.unwrap().into_inner();
        assert_eq!(response.missing_blob_digests.len(), 1);
        Ok(())
    }

    #[tokio::test]
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
            }))
            .await;
        assert!(raw_response.is_ok());
        let response = raw_response.unwrap().into_inner();
        assert_eq!(response.missing_blob_digests.len(), 0); // All items should have been found.
        Ok(())
    }

    #[tokio::test]
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
            }))
            .await;
        let error = raw_response.unwrap_err();
        assert!(
            error.to_string().contains("Invalid sha256 hash: BAD_HASH"),
            "'Invalid sha256 hash: BAD_HASH' not found in: {:?}",
            error
        );
        Ok(())
    }
}

#[cfg(test)]
mod batch_update_blobs {
    use super::*;
    use pretty_assertions::assert_eq; // Must be declared in every module.

    use proto::build::bazel::remote::execution::v2::{
        batch_update_blobs_request, batch_update_blobs_response, BatchUpdateBlobsRequest, BatchUpdateBlobsResponse,
    };

    #[tokio::test]
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
            .update_oneshot(DigestInfo::try_new(&HASH1, VALUE1.len())?, VALUE1.into())
            .await
            .expect("Update should have succeeded");

        let raw_response = cas_server
            .batch_update_blobs(Request::new(BatchUpdateBlobsRequest {
                instance_name: INSTANCE_NAME.to_string(),
                requests: vec![batch_update_blobs_request::Request {
                    digest: Some(digest.clone()),
                    data: VALUE2.into(),
                }],
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
            .get_part_unchunked(DigestInfo::try_new(&HASH1, VALUE1.len())?, 0, None, None)
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
    use super::*;
    use pretty_assertions::assert_eq; // Must be declared in every module.

    use proto::build::bazel::remote::execution::v2::{
        batch_read_blobs_response, BatchReadBlobsRequest, BatchReadBlobsResponse,
    };
    use tonic::Code;

    #[tokio::test]
    async fn batch_read_blobs_read_two_blobs_success_one_fail() -> Result<(), Box<dyn std::error::Error>> {
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
                .update_oneshot(DigestInfo::try_new(&HASH1, VALUE1.len())?, VALUE1.into())
                .await
                .expect("Update should have succeeded");
            store
                .update_oneshot(DigestInfo::try_new(&HASH2, VALUE2.len())?, VALUE2.into())
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
                        },
                        batch_read_blobs_response::Response {
                            digest: Some(digest2),
                            data: VALUE2.into(),
                            status: Some(GrpcStatus {
                                code: 0, // Status Ok.
                                message: "".to_string(),
                                details: vec![],
                            }),
                        },
                        batch_read_blobs_response::Response {
                            digest: Some(digest3.clone()),
                            data: vec![].into(),
                            status: Some(GrpcStatus {
                                code: Code::NotFound as i32,
                                message: format!(
                                    "Error: Error {{ code: NotFound, messages: [\"Hash {} not found\"] }}",
                                    digest3.hash
                                ),
                                details: vec![],
                            }),
                        }
                    ],
                }
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod end_to_end {
    use super::*;
    use pretty_assertions::assert_eq; // Must be declared in every module.

    use proto::build::bazel::remote::execution::v2::{
        batch_update_blobs_request, batch_update_blobs_response, BatchUpdateBlobsRequest, BatchUpdateBlobsResponse,
        FindMissingBlobsRequest,
    };

    #[tokio::test]
    async fn batch_update_blobs_two_items_existence_with_third_missing() -> Result<(), Box<dyn std::error::Error>> {
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
                        },
                        batch_update_blobs_request::Request {
                            digest: Some(digest2.clone()),
                            data: VALUE2.into(),
                        },
                    ],
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
                }))
                .await;
            assert!(raw_response.is_ok());
            let response = raw_response.unwrap().into_inner();
            assert_eq!(response.missing_blob_digests, vec![missing_digest]);
        }
        Ok(())
    }
}
