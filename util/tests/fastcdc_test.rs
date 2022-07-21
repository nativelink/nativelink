// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

// #![feature(stmt_expr_attributes)]

use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::marker::Unpin;

use bytes::Bytes;
use futures::stream::StreamExt;
use rand::{rngs::SmallRng, Rng, SeedableRng};
use sha2::{Digest, Sha256};

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::codec::{Decoder, FramedRead};

use common::fs;
use fastcdc::FastCDC;

const MEGABYTE_SZ: usize = 1024 * 1024;

/// Extracts the frames of the reader into a vector.
async fn get_frames<T: AsyncRead + Unpin, D: Decoder>(
    frame_reader: &mut FramedRead<T, D>,
) -> Result<Vec<D::Item>, D::Error> {
    let mut frames = vec![];
    while let Some(frame) = frame_reader.next().await {
        frames.push(frame?);
    }
    Ok(frames)
}

#[cfg(test)]
mod fastcdc_tests {
    use super::*;
    use pretty_assertions::assert_eq; // Must be declared in every module.

    #[tokio::test]
    async fn test_all_zeros() -> Result<(), std::io::Error> {
        // For all zeros, always returns chunks of maximum size.
        const ZERO_DATA: [u8; 10240] = [0u8; 10240];
        let mut cursor = Cursor::new(&ZERO_DATA);
        let mut frame_reader = FramedRead::new(&mut cursor, FastCDC::new(64, 256, 1024));

        let mut total_data = 0;
        while let Some(frame) = frame_reader.next().await {
            let frame = frame?;
            assert_eq!(frame.len(), 1024);
            total_data += frame.len();
        }
        assert_eq!(ZERO_DATA.len(), total_data);
        Ok(())
    }

    #[tokio::test]
    async fn test_sekien_16k_chunks() -> Result<(), Box<dyn std::error::Error>> {
        let mut file = fs::open_file("util/tests/data/SekienAkashita.jpg").await?;
        let mut contents = vec![];
        file.read_to_end(&mut contents).await?;
        let mut cursor = Cursor::new(&contents);
        let mut frame_reader = FramedRead::new(&mut cursor, FastCDC::new(8192, 16384, 32768));

        let mut frames = vec![];
        let mut sum_frame_len = 0;
        while let Some(frame) = frame_reader.next().await {
            let frame = frame?;
            sum_frame_len += frame.len();
            frames.push(frame);
        }
        assert_eq!(frames.len(), 6);
        assert_eq!(frames[0].len(), 22365);
        assert_eq!(frames[1].len(), 8282);
        assert_eq!(frames[2].len(), 16303);
        assert_eq!(frames[3].len(), 18696);
        assert_eq!(frames[4].len(), 32768);
        assert_eq!(frames[5].len(), 11052);
        assert_eq!(sum_frame_len, contents.len());
        Ok(())
    }

    #[tokio::test]
    async fn test_random_20mb_16k_chunks() -> Result<(), std::io::Error> {
        let data = {
            let mut data = vec![0u8; 20 * MEGABYTE_SZ];
            let mut rng = SmallRng::seed_from_u64(1);
            rng.fill(&mut data[..]);
            data
        };
        let mut cursor = Cursor::new(&data);
        let mut frame_reader = FramedRead::new(&mut cursor, FastCDC::new(1024, 2048, 4096));

        let mut lens = vec![];
        for frame in get_frames(&mut frame_reader).await? {
            lens.push(frame.len());
        }
        lens.sort();
        Ok(())
    }

    #[tokio::test]
    async fn insert_garbage_check_boundarys_recover_test() -> Result<(), std::io::Error> {
        let mut rand_data = {
            let mut data = vec![0u8; 100_000];
            let mut rng = SmallRng::seed_from_u64(1);
            rng.fill(&mut data[..]);
            data
        };

        let fast_cdc = FastCDC::new(1024, 2048, 4096);
        let left_frames = {
            let mut frame_reader = FramedRead::new(Cursor::new(&rand_data), fast_cdc.clone());
            let frames: Vec<Bytes> = get_frames(&mut frame_reader).await?;

            let mut frames_map = HashMap::new();
            let mut pos = 0;
            for frame in frames {
                let frame_len = frame.len();
                frames_map.insert(hex::encode(Sha256::digest(&frame[..])), (frame, pos));
                pos += frame_len;
            }
            frames_map
        };

        {
            // Replace 2k of bytes and append one byte to middle.
            let mut rng = SmallRng::seed_from_u64(2);
            rng.fill(&mut rand_data[0..2000]);
            rand_data.insert(rand_data.len() / 2, 0x71);
        }

        let mut right_frames = {
            let mut frame_reader = FramedRead::new(Cursor::new(&rand_data), fast_cdc.clone());
            let frames: Vec<Bytes> = get_frames(&mut frame_reader).await?;

            let mut frames_map = HashMap::new();
            let mut pos = 0;
            for frame in frames {
                let frame_len = frame.len();
                frames_map.insert(hex::encode(Sha256::digest(&frame[..])), (frame, pos));
                pos += frame_len;
            }
            frames_map
        };

        let mut expected_missing_hashes = HashSet::<String>::new();
        {
            expected_missing_hashes.insert("076a696917163caac2725f753bd2be2e902478c59cd92eeff012851da5868800".into());
            expected_missing_hashes.insert("b60ffdcaa8f833ca6a9900814e55d5a47b3205a6ac7d21aaadc64f889b556932".into());
            expected_missing_hashes.insert("11367bd6ecba2833556decd869ac714180456ab9ef815b167ffae947796377fd".into());

            for key in left_frames.keys() {
                let maybe_right_frame = right_frames.get(key);
                if maybe_right_frame.is_none() {
                    assert_eq!(expected_missing_hashes.contains(key), true, "Expected to find: {}", key);
                    expected_missing_hashes.remove(key);
                } else {
                    right_frames.remove(key);
                }
            }
        }
        let mut expected_new_hashes = HashMap::<String, usize>::new();
        {
            expected_new_hashes.insert(
                "867f8da2007726835f89eca1e891c292f648660d0af8cfd169262d13d650fdbd".into(),
                0,
            );
            expected_new_hashes.insert(
                "21930b56aa9fd7dca4160be0e4c92f04b883ffda4cdb78704f73eca8b3b98036".into(),
                1832,
            );
            expected_new_hashes.insert(
                "0a58dd0fbd1b4942a83d12d3f143ca26cfa2e8b606065298cf37eebde9232a83".into(),
                49296,
            );

            for (key, (_, pos)) in right_frames {
                let expected_pos = expected_new_hashes.get(&key);
                assert_eq!(Some(&pos), expected_pos, "For hash {}", key);
                expected_new_hashes.remove(&key);
            }
        }
        assert_eq!(expected_missing_hashes.len(), 0, "Some old hashes were not found");
        assert_eq!(expected_new_hashes.len(), 0, "Some new hashes were not consumed");

        Ok(())
    }
}
