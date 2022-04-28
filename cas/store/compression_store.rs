// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::cmp;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bincode::{
    self,
    config::{FixintEncoding, WithOtherIntEncoding},
    DefaultOptions, Options,
};
use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BufMut, BytesMut};
use futures::future::FutureExt;
use lz4_flex::block::{compress_into, decompress_into, get_maximum_output_size};
use serde::{Deserialize, Serialize};

use buf_channel::{make_buf_channel_pair, DropCloserReadHalf, DropCloserWriteHalf};
use common::{DigestInfo, JoinHandleDropGuard};
use error::{error_if, make_err, Code, Error, ResultExt};
use traits::{StoreTrait, UploadSizeInfo};

// In the event the bytestream format changes this number should be incremented to prevent
// backwards compatibility issues.
pub const CURRENT_STREAM_FORMAT_VERSION: u8 = 1;

// Default block size that will be used to slice stream into.
pub const DEFAULT_BLOCK_SIZE: u32 = 64 * 1024;

const U32_SZ: usize = std::mem::size_of::<u8>();

type BincodeOptions = WithOtherIntEncoding<DefaultOptions, FixintEncoding>;

// We use a custom frame format here because I wanted the ability in the future to:
// * Read a random part of the data without needing to parse entire file.
// * Compress the data on the fly without needing to know the exact input size.
//
// The frame format that LZ4 uses does not contain an index of where the different
// blocks are located. This would mean in the event we only wanted the last byte of
// a file, we'd need to seek to the header of each block to find where the next block
// offset is until we got to the last block then decompress it.
//
// By using this custom frame format we keep the ability to have each block reference
// the next block (for efficiency), but also after all blocks we have a footer frame
// which contains an index of each block in the stream. This means that we can read a
// fixed number of bytes of the file, which always contain the size of the index, then
// with that size we know the exact size of the footer frame, which contains the entire
// index. Once this footer frame is loaded, we could then do some math to figure out
// which block each byte is in and the offset of each block within the compressed file.
//
// The frame format is as follows:
// |----------------------------------HEADER-----------------------------------------|
// |  version(u8) |  block_size (u32) |  upload_size_type (u32) |  upload_size (u32) |
// |----------------------------------BLOCK------------------------------------------|
// |  frame_type(u8) 0x00 |  compressed_data_size (u32) |        ...DATA...          |
// |                                ...DATA...                                       |
// | [Possibly repeat block]                                                         |
// |----------------------------------FOOTER-----------------------------------------|
// |  frame_type(u8) 0x01 |    footer_size (u32) |           index_count1 (u64)      |
// |      ...[pos_from_prev_index (u32) - repeat for count {index_count*}]...        |
// |  index_count2 (u32) |    uncompressed_data_sz (u64) |    block_size (u32)       |
// | version (u8) |------------------------------------------------------------------|
// |---------------------------------------------------------------------------------|
//
// version              - A constant number used to define what version of this format is being
//                        used. Version in header and footer must match.
// block_size           - Size of each block uncompressed except for last block. This means that
//                        every block uncompressed will be a constant size except last block may
//                        be variable size. Block size in header and footer must match.
// upload_size_type     - Value of 0 = UploadSizeInfo::ExactSize, 1 = UploadSizeInfo::MaxSize.
//                        This is for debug reasons only.
// upload_size          - The size of the data. WARNING: Do not rely on this being the uncompressed
//                        payload size. It is a debug field and a "best guess" on how large the data
//                        is. The header does not contain the upload data size. This value is the
//                        value counter part to what the `upload_size_type` field.
// frame_type           - Type of each frame. 0 = BLOCK frame, 1 = FOOTER frame. Header frame will
//                        always start with the first byte of the stream, so no magic number for it.
// compressed_data_size - The size of this block. The bytes after this field should be read
//                        in sequence to get all of the block's data in this block.
// footer_size          - Size of the footer for bytes after this field.
// index_count1         - Number of items in the index. ({index_count1} * 4) represents the number
//                        of bytes that should be read after this field in order to get all index
//                        data.
// pos_from_prev_index  - Index of an individual block in the stream relative to the previous
//                        block. This field may repeat {index_count} times.
// index_count2         - Same as {index_count1} and should always be equal. This field might be
//                        useful if you want to read a random byte from this stream, have random
//                        access to it and know the size of the stream during reading, because
//                        this field is always in the exact same place in the stream relative to
//                        the last byte.
// uncompressed_data_sz - Size of the original uncompressed data.
//
// Note: All fields fields little-endian.

/// Number representing a chunk.
pub const CHUNK_FRAME_TYPE: u8 = 0;

/// Number representing the footer.
pub const FOOTER_FRAME_TYPE: u8 = 1;

/// This is a partial mirror of config::backends::Lz4Config.
/// We cannot use that natively here because it could cause our
/// serialized format to change if we added more configs.
#[derive(Serialize, Deserialize, PartialEq, Debug, Default, Copy, Clone)]
pub struct Lz4Config {
    pub block_size: u32,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Header {
    pub version: u8,
    pub config: Lz4Config,
    pub upload_size: UploadSizeInfo,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Default, Clone, Copy)]
pub struct SliceIndex {
    pub position_from_prev_index: u32,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Default)]
pub struct Footer {
    pub indexes: Vec<SliceIndex>,
    pub index_count: u32,
    pub uncompressed_data_size: u64,
    pub config: Lz4Config,
    pub version: u8,
}

/// lz4_flex::block::get_maximum_output_size() way over estimates, so we use the
/// one provided here: https://github.com/torvalds/linux/blob/master/include/linux/lz4.h#L61
/// Local testing shows this gives quite accurate worst case given random input.
fn lz4_compress_bound(input_size: usize) -> usize {
    input_size + (input_size / 255) + 16
}

struct UploadState {
    header: Header,
    footer: Footer,
    max_output_size: usize,
    input_max_size: usize,
}

impl UploadState {
    pub fn new(store: &CompressionStore, upload_size: UploadSizeInfo) -> Self {
        let input_max_size = match upload_size {
            UploadSizeInfo::ExactSize(sz) => sz,
            UploadSizeInfo::MaxSize(sz) => sz,
        };

        let max_index_count = (input_max_size / store.config.block_size as usize) + 1;

        let header = Header {
            version: CURRENT_STREAM_FORMAT_VERSION,
            config: Lz4Config {
                block_size: store.config.block_size,
            },
            upload_size,
        };
        let footer = Footer {
            indexes: vec![SliceIndex { ..Default::default() }; max_index_count],
            index_count: max_index_count as u32,
            uncompressed_data_size: 0, // Updated later.
            config: header.config.clone(),
            version: CURRENT_STREAM_FORMAT_VERSION,
        };

        // This is more accurate of an estimate than what get_maximum_output_size calculates.
        let max_block_size = lz4_compress_bound(store.config.block_size as usize) + U32_SZ + 1;

        let max_output_size = {
            let header_size = store.bincode_options.serialized_size(&header).unwrap() as usize;
            let max_content_size = max_block_size * max_index_count;
            let max_footer_size = U32_SZ + 1 + store.bincode_options.serialized_size(&footer).unwrap() as usize;
            header_size + max_content_size + max_footer_size
        };

        Self {
            header,
            footer,
            max_output_size,
            input_max_size,
        }
    }
}

/// This store will compress data before sending it on to the inner store.
/// Note: Currently using get_part() and trying to read part of the data will
/// result in the entire contents being read from the inner store but will
/// only send the contents requested.
pub struct CompressionStore {
    inner_store: Arc<dyn StoreTrait>,
    config: config::backends::Lz4Config,
    bincode_options: BincodeOptions,
}

impl CompressionStore {
    pub fn new(
        compression_config: config::backends::CompressionStore,
        inner_store: Arc<dyn StoreTrait>,
    ) -> Result<Self, Error> {
        let lz4_config = match compression_config.compression_algorithm {
            config::backends::CompressionAlgorithm::LZ4(mut lz4_config) => {
                if lz4_config.block_size == 0 {
                    lz4_config.block_size = DEFAULT_BLOCK_SIZE;
                }
                if lz4_config.max_decode_block_size == 0 {
                    lz4_config.max_decode_block_size = lz4_config.block_size;
                }
                lz4_config
            }
        };
        Ok(CompressionStore {
            inner_store: inner_store,
            config: lz4_config,
            bincode_options: DefaultOptions::new().with_fixint_encoding(),
        })
    }
}

#[async_trait]
impl StoreTrait for CompressionStore {
    async fn has(self: Pin<&Self>, digest: DigestInfo) -> Result<Option<usize>, Error> {
        Pin::new(self.inner_store.as_ref()).has(digest).await
    }

    async fn update(
        self: Pin<&Self>,
        digest: DigestInfo,
        mut reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        let mut output_state = UploadState::new(&self, upload_size);

        let (mut tx, rx) = make_buf_channel_pair();

        let inner_store = self.inner_store.clone();
        let update_fut = JoinHandleDropGuard::new(tokio::spawn(async move {
            Pin::new(inner_store.as_ref())
                .update(
                    digest,
                    rx,
                    UploadSizeInfo::MaxSize(output_state.max_output_size as usize),
                )
                .await
                .err_tip(|| "Inner store update in compression store failed")
        }))
        .map(
            |result| match result.err_tip(|| "Failed to run compression update spawn") {
                Ok(inner_result) => inner_result.err_tip(|| "Compression underlying store update failed"),
                Err(e) => Err(e),
            },
        );

        let write_fut = async move {
            {
                // Write Header.
                let serialized_header = self
                    .bincode_options
                    .serialize(&output_state.header)
                    .map_err(|e| make_err!(Code::Internal, "Failed to serialize header : {:?}", e))?;
                tx.send(serialized_header.into())
                    .await
                    .err_tip(|| "Failed to write compression header on upload")?;
            }

            let mut received_amt = 0;
            let mut index_count = 0;
            for index in &mut output_state.footer.indexes {
                let chunk = reader
                    .take(self.config.block_size as usize)
                    .await
                    .err_tip(|| "Failed to read take in update in compression store")?;
                if chunk.len() == 0 {
                    break; // EOF.
                }

                received_amt = received_amt + chunk.len();
                error_if!(
                    received_amt > output_state.input_max_size,
                    "Got more data than stated in compression store upload request"
                );

                let max_output_size = get_maximum_output_size(self.config.block_size as usize);
                let mut compressed_data_buf = BytesMut::with_capacity(max_output_size);
                compressed_data_buf.put_u8(CHUNK_FRAME_TYPE);
                compressed_data_buf.put_u32_le(0); // Filled later.

                // For efficiency reasons we do some raw slice manipulation so we can write directly
                // into our buffer instead of having to do another allocation.
                let mut raw_compressed_data = unsafe {
                    std::slice::from_raw_parts_mut(compressed_data_buf.chunk_mut().as_mut_ptr(), max_output_size)
                };

                let compressed_data_sz = compress_into(&chunk, &mut raw_compressed_data)
                    .map_err(|e| make_err!(Code::Internal, "Compression error {:?}", e))?;
                unsafe {
                    compressed_data_buf.advance_mut(compressed_data_sz);
                }

                // Now fill the size in our slice.
                LittleEndian::write_u32(&mut compressed_data_buf[1..5], compressed_data_sz as u32);

                // Now send our chunk.
                tx.send(compressed_data_buf.freeze())
                    .await
                    .err_tip(|| "Failed to write chunk to inner store in compression store")?;

                index.position_from_prev_index = compressed_data_sz as u32;

                index_count += 1;
            }
            // Index 0 is actually a pointer to the second chunk. This is because we don't need
            // an index for the first item, since it starts at position `{header_len}`.
            // The code above causes us to create 1 more index than we actually need, so we
            // remove the last index from our vector here, because at this point we are always
            // one index too many.
            // Note: We need to be careful that if we don't have any data (zero bytes) it
            // doesn't go to -1.
            if index_count > 0 {
                index_count -= 1;
            }
            output_state
                .footer
                .indexes
                .resize(index_count, SliceIndex { ..Default::default() });
            output_state.footer.index_count = output_state.footer.indexes.len() as u32;
            output_state.footer.uncompressed_data_size = received_amt as u64;
            {
                // Write Footer.
                let serialized_footer = self
                    .bincode_options
                    .serialize(&output_state.footer)
                    .map_err(|e| make_err!(Code::Internal, "Failed to serialize header : {:?}", e))?;

                let mut footer = BytesMut::with_capacity(1 + 4 + serialized_footer.len());
                footer.put_u8(FOOTER_FRAME_TYPE);
                footer.put_u32_le(serialized_footer.len() as u32);
                footer.extend_from_slice(&serialized_footer);

                tx.send(footer.freeze())
                    .await
                    .err_tip(|| "Failed to write footer to inner store in compression store")?;
                tx.send_eof()
                    .await
                    .err_tip(|| "Failed writing EOF in compression store update")?;
            }

            Result::<(), Error>::Ok(())
        };
        let (write_result, update_result) = tokio::join!(write_fut, update_fut);
        write_result.merge(update_result)
    }

    async fn get_part(
        self: Pin<&Self>,
        digest: DigestInfo,
        mut writer: DropCloserWriteHalf,
        offset: usize,
        length: Option<usize>,
    ) -> Result<(), Error> {
        let offset = offset as u64;
        let (tx, mut rx) = make_buf_channel_pair();

        let inner_store = self.inner_store.clone();
        let get_part_fut = JoinHandleDropGuard::new(tokio::spawn(async move {
            Pin::new(inner_store.as_ref())
                .get_part(digest, tx, 0, None)
                .await
                .err_tip(|| "Inner store get in compression store failed")
        }))
        .map(
            |result| match result.err_tip(|| "Failed to run compression get spawn") {
                Ok(inner_result) => inner_result.err_tip(|| "Compression underlying store get failed"),
                Err(e) => Err(e),
            },
        );
        let read_fut = async move {
            let header = {
                // Read header.
                static EMPTY_HEADER: Header = Header {
                    version: CURRENT_STREAM_FORMAT_VERSION,
                    config: Lz4Config { block_size: 0 },
                    upload_size: UploadSizeInfo::ExactSize(0),
                };
                let header_size = self.bincode_options.serialized_size(&EMPTY_HEADER).unwrap() as u64;
                let chunk = rx
                    .take(header_size as usize)
                    .await
                    .err_tip(|| "Failed to read header in get_part compression store")?;
                error_if!(
                    chunk.len() as u64 != header_size,
                    "Expected inner store to return the proper amount of data in compression store {} != {}",
                    chunk.len(),
                    header_size,
                );

                self.bincode_options
                    .deserialize::<Header>(&chunk)
                    .map_err(|e| make_err!(Code::Internal, "Failed to deserialize header : {:?}", e))?
            };

            error_if!(
                header.version != CURRENT_STREAM_FORMAT_VERSION,
                "Expected header version to match in get compression, got {}, want {}",
                header.version,
                CURRENT_STREAM_FORMAT_VERSION
            );
            error_if!(
                header.config.block_size > self.config.max_decode_block_size,
                "Block size is too large in compression, got {} > {}",
                header.config.block_size,
                self.config.max_decode_block_size
            );

            let mut chunk = rx
                .take(1 + 4)
                .await
                .err_tip(|| "Failed to read init frame info in compression store")?;
            error_if!(
                chunk.len() < 1 + 4,
                "Received EOF too early while reading init frame info in compression store"
            );

            let mut frame_type = chunk.get_u8();
            let mut frame_sz = chunk.get_u32_le();

            let mut uncompressed_data_sz: u64 = 0;
            let mut remaining_bytes_to_send: u64 = length.unwrap_or(usize::MAX) as u64;
            let mut chunks_count = 0;
            while frame_type != FOOTER_FRAME_TYPE {
                error_if!(
                    frame_type != CHUNK_FRAME_TYPE,
                    "Expected frame to be BODY in compression store, got {} at {}",
                    frame_type,
                    chunks_count
                );

                let chunk = rx
                    .take(frame_sz as usize)
                    .await
                    .err_tip(|| "Failed to read chunk in get_part compression store")?;
                if chunk.len() < frame_sz as usize {
                    return Err(make_err!(
                        Code::Internal,
                        "Got EOF earlier than expected. Maybe the data is not compressed or different format?"
                    ));
                }
                {
                    let max_output_size = get_maximum_output_size(header.config.block_size as usize);
                    let mut uncompressed_data = BytesMut::with_capacity(max_output_size);

                    // For efficiency reasons we do some raw slice manipulation so we can write directly
                    // into our buffer instead of having to do another allocation.
                    let mut raw_decompressed_data = unsafe {
                        std::slice::from_raw_parts_mut(uncompressed_data.chunk_mut().as_mut_ptr(), max_output_size)
                    };

                    let uncompressed_chunk_sz = decompress_into(&chunk, &mut raw_decompressed_data)
                        .map_err(|e| make_err!(Code::Internal, "Decompression error {:?}", e))?;
                    unsafe { uncompressed_data.advance_mut(uncompressed_chunk_sz) };
                    let new_uncompressed_data_sz = uncompressed_data_sz + uncompressed_chunk_sz as u64;
                    if new_uncompressed_data_sz >= offset && remaining_bytes_to_send > 0 {
                        let start_pos = if offset <= uncompressed_data_sz {
                            0
                        } else {
                            offset - uncompressed_data_sz
                        } as usize;
                        let end_pos = cmp::min(start_pos + remaining_bytes_to_send as usize, uncompressed_chunk_sz);
                        if end_pos != start_pos {
                            // Make sure we don't send an EOF by accident.
                            writer
                                .send(uncompressed_data.freeze().slice(start_pos..end_pos))
                                .await
                                .err_tip(|| "Failed sending chunk in compression store")?;
                        }
                        remaining_bytes_to_send -= (end_pos - start_pos) as u64;
                    }
                    uncompressed_data_sz = new_uncompressed_data_sz;
                }
                chunks_count += 1;

                let mut chunk = rx
                    .take(1 + 4)
                    .await
                    .err_tip(|| "Failed to read frame info in compression store")?;
                error_if!(
                    chunk.len() < 1 + 4,
                    "Received EOF too early while reading frame info in compression store"
                );

                frame_type = chunk.get_u8();
                frame_sz = chunk.get_u32_le();
            }
            // Index count will always be +1 (unless it is zero bytes long).
            if chunks_count > 0 {
                chunks_count -= 1;
            }
            {
                // Read and validate footer.
                let chunk = rx
                    .take(frame_sz as usize)
                    .await
                    .err_tip(|| "Failed to read chunk in get_part compression store")?;
                error_if!(
                    chunk.len() < frame_sz as usize,
                    "Unexpected EOF when reading footer in compression store get_part"
                );

                let footer = self
                    .bincode_options
                    .deserialize::<Footer>(&chunk)
                    .map_err(|e| make_err!(Code::Internal, "Failed to deserialize footer : {:?}", e))?;

                error_if!(
                    header.version != footer.version,
                    "Expected header and footer versions to match compression store get_part, {} != {}",
                    header.version,
                    footer.version
                );
                error_if!(
                    footer.indexes.len() != footer.index_count as usize,
                    "Expected index counts to match in compression store footer in get_part, {} != {}",
                    footer.indexes.len(),
                    footer.index_count
                );
                error_if!(
                    footer.index_count != chunks_count,
                    concat!(
                        "Expected index counts to match received chunks count ",
                        "in compression store footer in get_part, {} != {}"
                    ),
                    footer.index_count,
                    chunks_count
                );
                error_if!(
                    footer.uncompressed_data_size != uncompressed_data_sz,
                    "Expected uncompressed data sizes to match in compression store footer in get_part, {} != {}",
                    footer.uncompressed_data_size,
                    uncompressed_data_sz
                );
            }

            writer
                .send_eof()
                .await
                .err_tip(|| "Failed to send eof in compression store write")?;
            Ok(())
        };

        let (read_result, get_part_fut_result) = tokio::join!(read_fut, get_part_fut);
        if let Err(mut e) = read_result {
            // We may need to propagate the error from reading the data through first.
            if let Err(err) = get_part_fut_result {
                e = err.merge(e);
            }
            return Err(e);
        }
        Ok(())
    }

    fn as_any(self: Arc<Self>) -> Box<dyn std::any::Any> {
        Box::new(self)
    }
}
