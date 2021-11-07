// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::cmp;
use std::io::Cursor;
use std::pin::Pin;
use std::str::from_utf8;
use std::sync::Arc;

use bincode::{DefaultOptions, Options};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use tokio::io::AsyncReadExt;

use common::DigestInfo;
use compression_store::{
    CompressionStore, Footer, Lz4Config, SliceIndex, CURRENT_STREAM_FORMAT_VERSION, DEFAULT_BLOCK_SIZE,
    FOOTER_FRAME_TYPE,
};
use error::{make_err, Code, Error, ResultExt};
use memory_store::MemoryStore;
use traits::{StoreTrait, UploadSizeInfo};

/// Utility function that will build a Footer object from the input.
fn extract_footer(data: &[u8]) -> Result<Footer, Error> {
    let mut pos = data.len() - 1; // Skip version byte(u8)
    pos -= 4; // Skip block_size(u32).
    pos -= 8; // Skip uncompressed_data_size(u64).
    let index_count = u32::from_le_bytes(data[pos - 4..pos].try_into().unwrap());
    pos -= 4; // Skip index_count(u32).
    pos -= (index_count * 4) as usize; // Skip indexes(u32 * index_count).
    pos -= 8; // Skip bincode_index_count(u64).
    let footer_len = u32::from_le_bytes(data[pos - 4..pos].try_into().unwrap());
    assert_eq!(footer_len as usize, data.len() - pos, "Expected footer_len to match");
    assert_eq!(data[pos - 4 - 1], FOOTER_FRAME_TYPE, "Expected frame_type to be footer");

    DefaultOptions::new()
        .with_fixint_encoding()
        .deserialize::<Footer>(&data[pos..])
        .map_err(|e| make_err!(Code::Internal, "Failed to deserialize header : {:?}", e))
}

#[cfg(test)]
mod compression_store_tests {
    use super::*;
    use pretty_assertions::assert_eq; // Must be declared in every module.

    const VALID_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
    const DUMMY_DATA_SIZE: usize = 100; // Some dummy size to populate DigestInfo with.
    const MEGABYTE_SZ: usize = 1024 * 1024;

    #[tokio::test]
    async fn simple_smoke_test() -> Result<(), Error> {
        let store_owned = CompressionStore::new(
            config::backends::CompressionStore {
                backend: config::backends::StoreConfig::memory(config::backends::MemoryStore::default()),
                compression_algorithm: config::backends::CompressionAlgorithm::LZ4(config::backends::Lz4Config {
                    ..Default::default()
                }),
            },
            Arc::new(MemoryStore::new(&config::backends::MemoryStore::default())),
        )
        .err_tip(|| "Failed to create compression store")?;
        let store = Pin::new(&store_owned);

        const RAW_INPUT: &str = "123";
        let digest = DigestInfo::try_new(&VALID_HASH, DUMMY_DATA_SIZE).unwrap();
        store
            .update(
                digest.clone(),
                Box::new(Cursor::new(RAW_INPUT.clone())),
                UploadSizeInfo::ExactSize(RAW_INPUT.len()),
            )
            .await?;

        let mut store_data = Vec::new();
        store
            .get(digest, &mut Cursor::new(&mut store_data))
            .await
            .err_tip(|| "Failed to get from inner store")?;

        assert_eq!(from_utf8(&store_data[..]).unwrap(), RAW_INPUT, "Expected data to match");
        Ok(())
    }

    #[tokio::test]
    async fn partial_reads_test() -> Result<(), Error> {
        let store_owned = CompressionStore::new(
            config::backends::CompressionStore {
                backend: config::backends::StoreConfig::memory(config::backends::MemoryStore::default()),
                compression_algorithm: config::backends::CompressionAlgorithm::LZ4(config::backends::Lz4Config {
                    block_size: 10,
                    ..Default::default()
                }),
            },
            Arc::new(MemoryStore::new(&config::backends::MemoryStore::default())),
        )
        .err_tip(|| "Failed to create compression store")?;
        let store = Pin::new(&store_owned);

        const RAW_DATA: [u8; 30] = [
            00, 01, 02, 03, 04, 05, 06, 07, 08, 09, // BR.
            10, 11, 12, 13, 14, 15, 16, 17, 18, 19, // BR.
            20, 21, 22, 23, 24, 25, 26, 27, 28, 29, // BR.
        ];

        let digest = DigestInfo::try_new(&VALID_HASH, DUMMY_DATA_SIZE).unwrap();
        store
            .update(
                digest.clone(),
                Box::new(Cursor::new(RAW_DATA.clone())),
                UploadSizeInfo::ExactSize(RAW_DATA.len()),
            )
            .await?;

        // Read through the store forcing lots of decompression steps at different offsets
        // and different window sizes. This will ensure we get most edge cases for when
        // we go across block boundaries inclusive, on the fence and exclusive.
        for read_slice_size in 0..(RAW_DATA.len() + 5) {
            for offset in 0..(RAW_DATA.len() + 5) {
                let mut store_data = Vec::new();
                store
                    .get_part(
                        digest.clone(),
                        &mut Cursor::new(&mut store_data),
                        offset,
                        Some(read_slice_size),
                    )
                    .await
                    .err_tip(|| "Failed to get from inner store")?;

                let start_pos = cmp::min(RAW_DATA.len(), offset);
                let end_pos = cmp::min(RAW_DATA.len(), offset + read_slice_size);
                assert_eq!(
                    &store_data,
                    &RAW_DATA[start_pos..end_pos],
                    "Expected data to match at {} - {}",
                    offset,
                    read_slice_size,
                );
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn rand_5mb_smoke_test() -> Result<(), Error> {
        let store_owned = CompressionStore::new(
            config::backends::CompressionStore {
                backend: config::backends::StoreConfig::memory(config::backends::MemoryStore::default()),
                compression_algorithm: config::backends::CompressionAlgorithm::LZ4(config::backends::Lz4Config {
                    ..Default::default()
                }),
            },
            Arc::new(MemoryStore::new(&config::backends::MemoryStore::default())),
        )
        .err_tip(|| "Failed to create compression store")?;
        let store = Pin::new(&store_owned);

        let mut value = vec![0u8; 5 * MEGABYTE_SZ];

        let mut rng = SmallRng::seed_from_u64(1);
        rng.fill(&mut value[..]);

        let digest = DigestInfo::try_new(&VALID_HASH, DUMMY_DATA_SIZE).unwrap();
        store
            .update(
                digest.clone(),
                Box::new(Cursor::new(value.clone())),
                UploadSizeInfo::ExactSize(value.len()),
            )
            .await?;

        let mut store_data = Vec::new();
        store
            .get(digest, &mut Cursor::new(&mut store_data))
            .await
            .err_tip(|| "Failed to get from inner store")?;

        assert_eq!(&store_data, &value, "Expected data to match");
        Ok(())
    }

    #[tokio::test]
    async fn sanity_check_zero_bytes_test() -> Result<(), Error> {
        let inner_store = Arc::new(MemoryStore::new(&config::backends::MemoryStore::default()));
        let store_owned = CompressionStore::new(
            config::backends::CompressionStore {
                backend: config::backends::StoreConfig::memory(config::backends::MemoryStore::default()),
                compression_algorithm: config::backends::CompressionAlgorithm::LZ4(config::backends::Lz4Config {
                    ..Default::default()
                }),
            },
            inner_store.clone(),
        )
        .err_tip(|| "Failed to create compression store")?;
        let store = Pin::new(&store_owned);

        let digest = DigestInfo::try_new(&VALID_HASH, DUMMY_DATA_SIZE).unwrap();
        store
            .update(
                digest.clone(),
                Box::new(Cursor::new(vec![].into_boxed_slice())),
                UploadSizeInfo::ExactSize(0),
            )
            .await?;

        let mut store_data = Vec::new();
        store
            .get(digest.clone(), &mut Cursor::new(&mut store_data))
            .await
            .err_tip(|| "Failed to get from inner store")?;

        assert_eq!(store_data.len(), 0, "Expected store data to have no data in it");

        let mut compressed_data = Vec::new();
        Pin::new(inner_store.as_ref())
            .get(digest, &mut Cursor::new(&mut compressed_data))
            .await
            .err_tip(|| "Failed to get from inner store")?;
        assert_eq!(
            extract_footer(&compressed_data)?,
            Footer {
                indexes: vec![],
                index_count: 0,
                uncompressed_data_size: 0,
                config: Lz4Config {
                    block_size: DEFAULT_BLOCK_SIZE,
                },
                version: CURRENT_STREAM_FORMAT_VERSION,
            },
            "Expected footers to match"
        );
        Ok(())
    }
    #[tokio::test]
    async fn check_header_test() -> Result<(), Error> {
        const BLOCK_SIZE: u32 = 150;
        const MAX_SIZE_INPUT: usize = 1024 * 1024; // 1MB.
        let inner_store = Arc::new(MemoryStore::new(&config::backends::MemoryStore::default()));
        let store_owned = CompressionStore::new(
            config::backends::CompressionStore {
                backend: config::backends::StoreConfig::memory(config::backends::MemoryStore::default()),
                compression_algorithm: config::backends::CompressionAlgorithm::LZ4(config::backends::Lz4Config {
                    block_size: BLOCK_SIZE,
                    ..Default::default()
                }),
            },
            inner_store.clone(),
        )
        .err_tip(|| "Failed to create compression store")?;
        let store = Pin::new(&store_owned);

        const RAW_INPUT: &str = "123";

        let digest = DigestInfo::try_new(&VALID_HASH, DUMMY_DATA_SIZE).unwrap();
        store
            .update(
                digest.clone(),
                Box::new(Cursor::new(RAW_INPUT.clone())),
                UploadSizeInfo::MaxSize(MAX_SIZE_INPUT),
            )
            .await?;

        let mut compressed_data = Vec::new();
        Pin::new(inner_store.as_ref())
            .get(digest, &mut Cursor::new(&mut compressed_data))
            .await
            .err_tip(|| "Failed to get from inner store")?;

        let mut reader = Cursor::new(&compressed_data);
        {
            // Check version in header.
            let version = reader.read_u8().await?;
            assert_eq!(
                version, CURRENT_STREAM_FORMAT_VERSION,
                "Expected header version to match current version"
            );
        }
        {
            // Check block size.
            let block_size = reader.read_u32_le().await?;
            assert_eq!(block_size, BLOCK_SIZE, "Expected block size to match");
        }
        {
            // Check upload_type and upload_size.
            const MAX_SIZE_OPT_CODE: u32 = 1;
            let upload_type = reader.read_u32_le().await?;
            assert_eq!(upload_type, MAX_SIZE_OPT_CODE, "Expected enum size type to match");
            let upload_size = reader.read_u32_le().await?;
            assert_eq!(upload_size, MAX_SIZE_INPUT as u32, "Expected upload size to match");
        }

        // As a sanity check lets check our footer.
        assert_eq!(
            extract_footer(&compressed_data)?,
            Footer {
                indexes: vec![],
                index_count: 0,
                uncompressed_data_size: RAW_INPUT.len() as u64,
                config: Lz4Config { block_size: BLOCK_SIZE },
                version: CURRENT_STREAM_FORMAT_VERSION,
            },
            "Expected footers to match"
        );

        Ok(())
    }

    #[tokio::test]
    async fn check_footer_test() -> Result<(), Error> {
        const BLOCK_SIZE: u32 = 32 * 1024;
        let inner_store = Arc::new(MemoryStore::new(&config::backends::MemoryStore::default()));
        let store_owned = CompressionStore::new(
            config::backends::CompressionStore {
                backend: config::backends::StoreConfig::memory(config::backends::MemoryStore::default()),
                compression_algorithm: config::backends::CompressionAlgorithm::LZ4(config::backends::Lz4Config {
                    block_size: BLOCK_SIZE,
                    ..Default::default()
                }),
            },
            inner_store.clone(),
        )
        .err_tip(|| "Failed to create compression store")?;
        let store = Pin::new(&store_owned);

        let mut value = vec![0u8; MEGABYTE_SZ / 4];
        let data_len = value.len();

        let mut rng = SmallRng::seed_from_u64(1);
        // Fill first half of data with random data that is not compressible.
        rng.fill(&mut value[..(data_len / 2)]);

        let digest = DigestInfo::try_new(&VALID_HASH, DUMMY_DATA_SIZE).unwrap();
        store
            .update(
                digest.clone(),
                Box::new(Cursor::new(value.clone())),
                UploadSizeInfo::ExactSize(value.len()),
            )
            .await?;

        let mut compressed_data = Vec::new();
        Pin::new(inner_store.as_ref())
            .get(digest, &mut Cursor::new(&mut compressed_data))
            .await
            .err_tip(|| "Failed to get from inner store")?;

        let mut pos = compressed_data.len();
        {
            // Check version in footer.
            let version = compressed_data[pos - 1];
            pos -= 1;
            assert_eq!(
                version, CURRENT_STREAM_FORMAT_VERSION,
                "Expected footer version to match current version"
            );
        }
        {
            // Check block size in footer.
            let block_size = u32::from_le_bytes(compressed_data[pos - 4..pos].try_into().unwrap());
            pos -= 4;
            assert_eq!(
                block_size, BLOCK_SIZE,
                "Expected uncompressed_data_size to match original data size"
            );
        }
        {
            // Check data size in footer.
            let uncompressed_data_size = u64::from_le_bytes(compressed_data[pos - 8..pos].try_into().unwrap());
            pos -= 8;
            assert_eq!(
                uncompressed_data_size,
                value.len() as u64,
                "Expected uncompressed_data_size to match original data size"
            );
        }
        const EXPECTED_INDEXES: [u32; 7] = [32898, 32898, 32898, 32898, 140, 140, 140];
        let index_count = {
            // Check index count in footer.
            let index_count = u32::from_le_bytes(compressed_data[pos - 4..pos].try_into().unwrap());
            pos -= 4;
            assert_eq!(
                index_count as usize,
                EXPECTED_INDEXES.len(),
                "Expected index_count to match"
            );
            index_count
        };
        {
            // Check indexes in footer.
            let byte_count = (index_count * 4) as usize;
            let index_vec_raw = &compressed_data[pos - byte_count..pos];
            pos -= byte_count;
            let mut cursor = Cursor::new(index_vec_raw);
            let mut i = 0;
            while let Ok(index_pos) = cursor.read_u32_le().await {
                assert_eq!(
                    index_pos, EXPECTED_INDEXES[i],
                    "Expected index to equal at position {}",
                    i
                );
                i += 1;
            }
        }
        {
            // `bincode` adds the size again as a u64 before our index vector so check it too.
            let bincode_index_count = u64::from_le_bytes(compressed_data[pos - 8..pos].try_into().unwrap());
            pos -= 8;
            assert_eq!(
                bincode_index_count, index_count as u64,
                "Expected index_count and bincode_index_count to match"
            );
        }
        {
            // Check our footer length.
            let footer_len = u32::from_le_bytes(compressed_data[pos - 4..pos].try_into().unwrap());
            pos -= 4;
            assert_eq!(
                footer_len,
                1 + 4 + 8 + 4 + (index_count * 4) + 8,
                "Expected frame type to be footer"
            );
        }
        {
            // Check our frame type.
            let frame_type = u8::from_le_bytes(compressed_data[pos - 1..pos].try_into().unwrap());
            assert_eq!(frame_type, FOOTER_FRAME_TYPE, "Expected frame type to be footer");
        }

        // Just as one last sanity check lets check our deserialized footer.
        assert_eq!(
            extract_footer(&compressed_data)?,
            Footer {
                indexes: EXPECTED_INDEXES
                    .map(|v| SliceIndex {
                        position_from_prev_index: v
                    })
                    .to_vec(),
                index_count: EXPECTED_INDEXES.len() as u32,
                uncompressed_data_size: data_len as u64,
                config: Lz4Config { block_size: BLOCK_SIZE },
                version: CURRENT_STREAM_FORMAT_VERSION,
            },
            "Expected footers to match"
        );

        Ok(())
    }
}
