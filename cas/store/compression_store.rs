// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::cmp;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::FutureExt;
use lz4_flex::block::{compress_into, decompress_into, get_maximum_output_size};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use async_fixed_buffer::AsyncFixedBuf;
use bincode::{
    self,
    config::{FixintEncoding, WithOtherIntEncoding},
    DefaultOptions, Options,
};
use common::{DigestInfo, JoinHandleDropGuard};
use error::{error_if, make_err, Code, Error, ResultExt};
use traits::{ResultFuture, StoreTrait, UploadSizeInfo};

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
    fn has<'a>(self: Pin<&'a Self>, digest: DigestInfo) -> ResultFuture<'a, bool> {
        Box::pin(async move { Pin::new(self.inner_store.as_ref()).has(digest).await })
    }

    fn update<'a>(
        self: Pin<&'a Self>,
        digest: DigestInfo,
        mut reader: Box<dyn AsyncRead + Send + Sync + Unpin + 'static>,
        upload_size: UploadSizeInfo,
    ) -> ResultFuture<'a, ()> {
        Box::pin(async move {
            let mut output_state = UploadState::new(&self, upload_size);

            let fixed_buffer = AsyncFixedBuf::new(vec![0u8; 4096]);
            let (rx, mut tx) = fixed_buffer.split_into_reader_writer();

            let inner_store = self.inner_store.clone();
            let update_fut = JoinHandleDropGuard::new(tokio::spawn(async move {
                Pin::new(inner_store.as_ref())
                    .update(
                        digest,
                        Box::new(rx),
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
                    tx.write_all(&serialized_header)
                        .await
                        .err_tip(|| "Failed to write compression header on upload")?;
                }

                let mut rx_data_buf = Vec::with_capacity(self.config.block_size as usize);
                let mut compressed_data_buf = vec![0u8; get_maximum_output_size(self.config.block_size as usize)];
                let mut received_amt = 0;
                let mut index_count = 0;
                for index in &mut output_state.footer.indexes {
                    rx_data_buf.clear();
                    let mut taker = reader.take(self.config.block_size as u64);
                    let len = taker
                        .read_to_end(&mut rx_data_buf)
                        .await
                        .err_tip(|| "Failed to read in compression update")?;
                    if len == 0 {
                        break; // Received EOF.
                    }

                    received_amt = received_amt + len;
                    error_if!(
                        received_amt > output_state.input_max_size,
                        "Got more data than stated in compression store upload request"
                    );

                    let compressed_data_sz = compress_into(&rx_data_buf, &mut compressed_data_buf)
                        .map_err(|e| make_err!(Code::Internal, "Compression error {:?}", e))?;

                    tx.write_u8(CHUNK_FRAME_TYPE)
                        .await
                        .err_tip(|| "Failed to write type chunk to inner store in compression store")?;
                    tx.write_u32_le(compressed_data_sz as u32)
                        .await
                        .err_tip(|| "Failed to write size chunk to inner store in compression store")?;
                    tx.write_all(&compressed_data_buf[..compressed_data_sz])
                        .await
                        .err_tip(|| "Failed to write chunk to inner store in compression store")?;

                    index.position_from_prev_index = compressed_data_sz as u32;

                    reader = taker.into_inner();
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
                    tx.write_u8(FOOTER_FRAME_TYPE)
                        .await
                        .err_tip(|| "Failed to write type footer to inner store in compression store")?;
                    tx.write_u32_le(serialized_footer.len() as u32)
                        .await
                        .err_tip(|| "Failed to write size footer to inner store in compression store")?;
                    tx.write_all(&serialized_footer)
                        .await
                        .err_tip(|| "Failed writing compression header on upload")?;
                }
                {
                    // Close writer.
                    tx.write(&[])
                        .await
                        .err_tip(|| "Failed writing EOF in compression store update")?;
                    tx.shutdown()
                        .await
                        .err_tip(|| "Failed shutting down write stream in compression store update")?;
                }

                Ok(())
            };
            let (write_result, update_result) = tokio::join!(write_fut, update_fut);
            if let Err(mut e) = write_result {
                // We may need to propagate the error from reading the data through first.
                if let Err(err) = update_result {
                    e = err.merge(e);
                }
                return Err(e);
            }
            Ok(())
        })
    }

    fn get_part<'a>(
        self: Pin<&'a Self>,
        digest: DigestInfo,
        writer: &'a mut (dyn AsyncWrite + Send + Unpin + Sync),
        offset: usize,
        length: Option<usize>,
    ) -> ResultFuture<'a, ()> {
        Box::pin(async move {
            let offset = offset as u64;
            let fixed_buffer = AsyncFixedBuf::new(vec![0u8; 4096]);
            let (mut rx, mut tx) = fixed_buffer.split_into_reader_writer();

            let inner_store = self.inner_store.clone();
            let get_part_fut = JoinHandleDropGuard::new(tokio::spawn(async move {
                Pin::new(inner_store.as_ref())
                    .get_part(digest, &mut tx, 0, None)
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
                let mut rx_data_buf = Vec::with_capacity(get_maximum_output_size(self.config.block_size as usize));

                let header = {
                    // Read header.
                    let header_size = self
                        .bincode_options
                        .serialized_size(&Header {
                            version: CURRENT_STREAM_FORMAT_VERSION,
                            config: Default::default(),
                            upload_size: UploadSizeInfo::ExactSize(0),
                        })
                        .unwrap() as u64;
                    let mut taker = rx.take(header_size);
                    let len = taker
                        .read_to_end(&mut rx_data_buf)
                        .await
                        .err_tip(|| "Failed to read in compression get header")?;
                    error_if!(
                        len as u64 != header_size,
                        "Expected inner store to return the proper amount of data in compression store"
                    );

                    let header = self
                        .bincode_options
                        .deserialize::<Header>(&rx_data_buf)
                        .map_err(|e| make_err!(Code::Internal, "Failed to deserialize header : {:?}", e))?;

                    rx = taker.into_inner();
                    header
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

                let mut frame_type = rx
                    .read_u8()
                    .await
                    .err_tip(|| "Failed to read init frame type in compression store")?;
                let mut frame_sz = rx
                    .read_u32_le()
                    .await
                    .err_tip(|| "Failed to read init chunk size in compression store")?;

                let mut uncompressed_data = vec![0u8; get_maximum_output_size(header.config.block_size as usize)];
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
                    rx_data_buf.clear();
                    let mut taker = rx.take(frame_sz as u64);
                    let len = taker
                        .read_to_end(&mut rx_data_buf)
                        .await
                        .err_tip(|| "Failed to read chunk in compression store")? as u32;

                    if len < frame_sz {
                        return Err(make_err!(
                            Code::Internal,
                            "Got EOF earlier than expected. Maybe the data is not compressed or different format?"
                        ));
                    }
                    {
                        let uncompressed_chunk_sz = decompress_into(&rx_data_buf, &mut uncompressed_data)
                            .map_err(|e| make_err!(Code::Internal, "Decompression error {:?}", e))?;
                        let new_uncompressed_data_sz = uncompressed_data_sz + uncompressed_chunk_sz as u64;
                        if new_uncompressed_data_sz >= offset && remaining_bytes_to_send > 0 {
                            let start_pos = if offset <= uncompressed_data_sz {
                                0
                            } else {
                                offset - uncompressed_data_sz
                            } as usize;
                            let end_pos = cmp::min(start_pos + remaining_bytes_to_send as usize, uncompressed_chunk_sz);
                            writer
                                .write_all(&uncompressed_data[start_pos..end_pos])
                                .await
                                .err_tip(|| "Failed writing chunk in compression store")?;
                            remaining_bytes_to_send -= (end_pos - start_pos) as u64;
                        }
                        uncompressed_data_sz = new_uncompressed_data_sz;
                    }
                    chunks_count += 1;

                    rx = taker.into_inner();

                    frame_type = rx
                        .read_u8()
                        .await
                        .err_tip(|| "Failed to read frame type in compression store")?;
                    frame_sz = rx
                        .read_u32_le()
                        .await
                        .err_tip(|| "Failed to read frame size in compression store")?;
                }
                // Index count will always be +1 (unless it is zero bytes long).
                if chunks_count > 0 {
                    chunks_count -= 1;
                }
                {
                    // Read and validate footer.
                    rx_data_buf.clear();
                    let mut taker = rx.take(frame_sz as u64);
                    let len = taker
                        .read_to_end(&mut rx_data_buf)
                        .await
                        .err_tip(|| "Failed to read chunk in compression store")? as u32;
                    error_if!(
                        len < frame_sz,
                        "Unexpected EOF when reading footer in compression store get_part"
                    );

                    let footer = self
                        .bincode_options
                        .deserialize::<Footer>(&rx_data_buf)
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
                    .write(&[])
                    .await
                    .err_tip(|| "Failed writing EOF in compression store in get_part")?;
                writer
                    .shutdown()
                    .await
                    .err_tip(|| "Failed shutting down write stream in compression store get_part")?;
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
        })
    }
}
