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

use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::shard_store::ShardStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use nativelink_util::store_trait::Store;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

const MEGABYTE_SZ: usize = 1024 * 1024;

fn make_stores(weights: &[u32]) -> (Arc<ShardStore>, Vec<Arc<MemoryStore>>) {
    let memory_store_config = nativelink_config::stores::MemoryStore::default();
    let store_config =
        nativelink_config::stores::StoreConfigOptions::memory(memory_store_config.clone());
    let stores: Vec<_> = weights
        .iter()
        .map(|_| Arc::new(MemoryStore::new(&memory_store_config)))
        .collect();
    let stores_dyn: Vec<_> = stores
        .clone()
        .iter()
        .cloned()
        .map(|x| -> Arc<dyn Store> { x })
        .collect();

    let shard_store = Arc::new(
        ShardStore::new(
            &nativelink_config::stores::ShardStore {
                stores: weights
                    .iter()
                    .map(|weight| nativelink_config::stores::ShardConfig {
                        store: store_config.clone(),
                        weight: Some(*weight),
                    })
                    .collect(),
            },
            stores_dyn.clone(),
        )
        .unwrap(),
    );
    (shard_store, stores)
}

fn make_random_data(sz: usize) -> Vec<u8> {
    let mut value = vec![0u8; sz];
    let mut rng = SmallRng::seed_from_u64(1);
    rng.fill(&mut value[..]);
    value
}

async fn verify_weights(
    weights: &[u32],
    expected_hits: &[usize],
    rounds: u64,
    print_results: bool,
) -> Result<(), Error> {
    let (shard_store, stores) = make_stores(weights);
    let shard_store = Pin::new(shard_store.as_ref());
    let stores: Vec<_> = stores
        .iter()
        .map(|store| Pin::new(store.as_ref()))
        .collect();
    let data = make_random_data(MEGABYTE_SZ);

    for counter in 0..rounds {
        let mut hasher = DigestHasherFunc::Blake3.hasher();
        hasher.update(&counter.to_le_bytes());
        let digest = hasher.finalize_digest();
        shard_store
            .update_oneshot(digest, data.clone().into())
            .await?;
    }

    let stores_and_hits: Vec<(&Pin<&MemoryStore>, &usize)> =
        stores.iter().zip(expected_hits.iter()).collect();

    for (index, (&store, &expected_hit)) in stores_and_hits.iter().enumerate() {
        let total_hits = store.len_for_test().await;
        if print_results {
            println!("expected_hit: {expected_hit} - total_hits: {total_hits}");
        } else {
            assert_eq!(
                expected_hit, total_hits,
                "Index {index} failed with expected_hit: {expected_hit} != total_hits: {total_hits}"
            )
        }
    }
    Ok(())
}

#[cfg(test)]
mod shard_store_tests {

    use pretty_assertions::assert_eq;

    use super::*; // Must be declared in every module.

    const STORE0_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";
    const STORE1_HASH: &str = "00000000EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE";

    #[nativelink_test]
    async fn has_with_one_digest() -> Result<(), Error> {
        let (shard_store, stores) = make_stores(&[1, 1]);
        let shard_store = Pin::new(shard_store.as_ref());
        let stores: Vec<_> = stores
            .iter()
            .map(|store| Pin::new(store.as_ref()))
            .collect();

        let original_data = make_random_data(MEGABYTE_SZ);
        let digest1 = DigestInfo::try_new(STORE0_HASH, 100).unwrap();
        stores[0]
            .update_oneshot(digest1, original_data.clone().into())
            .await?;

        assert_eq!(shard_store.has(digest1).await, Ok(Some(MEGABYTE_SZ)));
        Ok(())
    }

    #[nativelink_test]
    async fn has_with_many_digests_both_missing() -> Result<(), Error> {
        let (shard_store, _stores) = make_stores(&[1, 1]);
        let shard_store = Pin::new(shard_store.as_ref());

        let missing_digest1 = DigestInfo::try_new(STORE0_HASH, 100).unwrap();
        let missing_digest2 = DigestInfo::try_new(STORE1_HASH, 100).unwrap();

        assert_eq!(
            shard_store
                .has_many(&[missing_digest1, missing_digest2])
                .await,
            Ok(vec![None, None])
        );
        Ok(())
    }

    #[nativelink_test]
    async fn has_with_many_digests_one_missing() -> Result<(), Error> {
        let (shard_store, stores) = make_stores(&[1, 1]);
        let shard_store = Pin::new(shard_store.as_ref());
        let stores: Vec<_> = stores
            .iter()
            .map(|store| Pin::new(store.as_ref()))
            .collect();

        let original_data = make_random_data(MEGABYTE_SZ);
        let digest1 = DigestInfo::try_new(STORE0_HASH, 100).unwrap();
        let missing_digest = DigestInfo::try_new(STORE1_HASH, 100).unwrap();
        stores[0]
            .update_oneshot(digest1, original_data.clone().into())
            .await?;

        assert_eq!(
            shard_store.has_many(&[digest1, missing_digest]).await,
            Ok(vec![Some(MEGABYTE_SZ), None])
        );
        Ok(())
    }

    #[nativelink_test]
    async fn has_with_many_digests_both_exist() -> Result<(), Error> {
        let (shard_store, stores) = make_stores(&[1, 1]);
        let shard_store = Pin::new(shard_store.as_ref());
        let stores: Vec<_> = stores
            .iter()
            .map(|store| Pin::new(store.as_ref()))
            .collect();

        let original_data1 = make_random_data(MEGABYTE_SZ);
        let original_data2 = make_random_data(2 * MEGABYTE_SZ);
        let digest1 = DigestInfo::try_new(STORE0_HASH, 100).unwrap();
        let digest2 = DigestInfo::try_new(STORE1_HASH, 100).unwrap();
        stores[0]
            .update_oneshot(digest1, original_data1.clone().into())
            .await?;
        stores[1]
            .update_oneshot(digest2, original_data2.clone().into())
            .await?;

        assert_eq!(
            shard_store.has_many(&[digest1, digest2]).await,
            Ok(vec![Some(original_data1.len()), Some(original_data2.len())])
        );
        Ok(())
    }

    #[nativelink_test]
    async fn get_part_reads_store0() -> Result<(), Error> {
        let (shard_store, stores) = make_stores(&[1, 1]);
        let shard_store = Pin::new(shard_store.as_ref());
        let stores: Vec<_> = stores
            .iter()
            .map(|store| Pin::new(store.as_ref()))
            .collect();

        let original_data1 = make_random_data(MEGABYTE_SZ);
        let digest1 = DigestInfo::try_new(STORE0_HASH, 100).unwrap();
        stores[0]
            .update_oneshot(digest1, original_data1.clone().into())
            .await?;

        assert_eq!(
            shard_store.get_part_unchunked(digest1, 0, None).await,
            Ok(original_data1.into())
        );
        Ok(())
    }

    #[nativelink_test]
    async fn get_part_reads_store1() -> Result<(), Error> {
        let (shard_store, stores) = make_stores(&[1, 1]);
        let shard_store = Pin::new(shard_store.as_ref());
        let stores: Vec<_> = stores
            .iter()
            .map(|store| Pin::new(store.as_ref()))
            .collect();

        let original_data1 = make_random_data(MEGABYTE_SZ);
        let digest1 = DigestInfo::try_new(STORE1_HASH, 100).unwrap();
        stores[1]
            .update_oneshot(digest1, original_data1.clone().into())
            .await?;

        assert_eq!(
            shard_store.get_part_unchunked(digest1, 0, None).await,
            Ok(original_data1.into())
        );
        Ok(())
    }

    #[nativelink_test]
    async fn upload_store0() -> Result<(), Error> {
        let (shard_store, stores) = make_stores(&[1, 1]);
        let shard_store = Pin::new(shard_store.as_ref());
        let stores: Vec<_> = stores
            .iter()
            .map(|store| Pin::new(store.as_ref()))
            .collect();

        let original_data1 = make_random_data(MEGABYTE_SZ);
        let digest1 = DigestInfo::try_new(STORE0_HASH, 100).unwrap();
        shard_store
            .update_oneshot(digest1, original_data1.clone().into())
            .await?;

        assert_eq!(
            stores[0].get_part_unchunked(digest1, 0, None).await,
            Ok(original_data1.into())
        );
        Ok(())
    }

    #[nativelink_test]
    async fn upload_store1() -> Result<(), Error> {
        let (shard_store, stores) = make_stores(&[1, 1]);
        let shard_store = Pin::new(shard_store.as_ref());
        let stores: Vec<_> = stores
            .iter()
            .map(|store| Pin::new(store.as_ref()))
            .collect();

        let original_data1 = make_random_data(MEGABYTE_SZ);
        let digest1 = DigestInfo::try_new(STORE1_HASH, 100).unwrap();
        shard_store
            .update_oneshot(digest1, original_data1.clone().into())
            .await?;

        assert_eq!(
            stores[1].get_part_unchunked(digest1, 0, None).await,
            Ok(original_data1.into())
        );
        Ok(())
    }

    #[nativelink_test]
    async fn upload_download_has_check() -> Result<(), Error> {
        let (shard_store, _stores) = make_stores(&[1, 1]);
        let shard_store = Pin::new(shard_store.as_ref());

        let original_data1 = make_random_data(MEGABYTE_SZ);
        let digest1 = DigestInfo::try_new(STORE1_HASH, 100).unwrap();

        assert_eq!(shard_store.has(digest1).await, Ok(None));
        shard_store
            .update_oneshot(digest1, original_data1.clone().into())
            .await?;
        assert_eq!(
            shard_store.get_part_unchunked(digest1, 0, None).await,
            Ok(original_data1.into())
        );
        assert_eq!(shard_store.has(digest1).await, Ok(Some(MEGABYTE_SZ)));
        Ok(())
    }

    #[nativelink_test]
    async fn weights_send_to_proper_store() -> Result<(), Error> {
        // Very low chance anything will ever go to second store due to weights being so much diff.
        let (shard_store, stores) = make_stores(&[100000, 1]);
        let shard_store = Pin::new(shard_store.as_ref());
        let stores: Vec<_> = stores
            .iter()
            .map(|store| Pin::new(store.as_ref()))
            .collect();

        let original_data1 = make_random_data(MEGABYTE_SZ);
        let digest1 = DigestInfo::try_new(STORE1_HASH, 100).unwrap();
        shard_store
            .update_oneshot(digest1, original_data1.clone().into())
            .await?;

        assert_eq!(stores[0].has(digest1).await, Ok(Some(MEGABYTE_SZ)));
        assert_eq!(stores[1].has(digest1).await, Ok(None));
        Ok(())
    }

    #[nativelink_test]
    async fn verify_weights_even_weights() -> Result<(), Error> {
        verify_weights(
            &[1, 1, 1, 1, 1, 1],
            &[188, 168, 158, 175, 147, 164],
            1000,
            false,
        )
        .await
    }

    #[nativelink_test]
    async fn verify_weights_mid_right_bias() -> Result<(), Error> {
        verify_weights(&[1, 1, 1, 100, 1, 1], &[5, 13, 12, 956, 4, 10], 1000, false).await
    }

    #[nativelink_test]
    async fn verify_weights_mid_left_bias() -> Result<(), Error> {
        verify_weights(&[1, 1, 100, 1, 1, 1], &[5, 13, 962, 6, 4, 10], 1000, false).await
    }

    #[nativelink_test]
    async fn verify_weights_left_bias() -> Result<(), Error> {
        verify_weights(&[100, 1, 1, 1, 1, 1], &[961, 11, 8, 6, 4, 10], 1000, false).await
    }

    #[nativelink_test]
    async fn verify_weights_right_bias() -> Result<(), Error> {
        verify_weights(&[1, 1, 1, 1, 1, 100], &[5, 13, 12, 5, 11, 954], 1000, false).await
    }
}
