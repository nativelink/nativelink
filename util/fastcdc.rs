// Copyright 2020-2021 Nathan Fiedler, Nathan (Blaise) Bruer.  Some rights reserved.
// This file differs in copyright and is available under the MIT license.
//
// This implementation is heavily based on:
// https://github.com/nlfiedler/fastcdc-rs/blob/1e804fe27444e37b2c4f93d540f861d170c8a257/src/lib.rs

use bytes::BytesMut;
use tokio_util::codec::Decoder;

struct State {
    hash: u32,
    position: usize,
}

impl State {
    fn reset(&mut self) {
        self.hash = 0;
        self.position = 0;
    }
}

/// This algorithm will take an input stream and build frames based on FastCDC algorithm.
/// see: https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf
///
/// In layman's terms this means we can take an input of unknown size and composition
/// and chunk it into smaller chunks with chunk boundaries that will be very similar
/// even when a small part of the file is changed. This use cases where a large file
/// (say 1G) has a few minor changes weather they byte inserts, deletes or updates
/// and when running through this algorithm it will slice the file up so that under
/// normal conditions all the chunk boundaries will be identical except the ones near
/// the mutations.
///
/// This is not very useful on it's own, but is extremely useful because we can pair
/// this together with a hash algorithm (like sha256) to then hash each chunk and
/// then check to see if we already have the sha256 somewhere before attempting to
/// upload the file. If the file does exist, we can skip the chunk and continue then
/// in the index file we can reference the same chunk that already exists.
///
/// Or put simply, it helps upload only the parts of the files that change, instead
/// of the entire file.
pub struct FastCDC {
    min_size: usize,
    avg_size: usize,
    max_size: usize,

    norm_size: usize,
    mask_easy: u32,
    mask_hard: u32,

    state: State,
}

impl FastCDC {
    pub fn new(min_size: usize, avg_size: usize, max_size: usize) -> Self {
        assert!(min_size < avg_size, "Expected {} < {}", min_size, avg_size);
        assert!(avg_size < max_size, "Expected {} < {}", avg_size, max_size);
        let norm_size = {
            let mut offset = min_size + ((min_size + 1) / 2);
            if offset > avg_size {
                offset = avg_size;
            }
            avg_size - offset
        };
        // Calculate the number of bits closest approximating our average.
        let bits = (avg_size as f64).log2().round() as u32;
        Self {
            min_size,
            avg_size,
            max_size,

            norm_size,
            // Turn our bits into a bitmask we can use later on for more
            // efficient bitwise operations.
            mask_hard: 2u32.pow(bits + 1) - 1,
            mask_easy: 2u32.pow(bits - 1) - 1,

            state: State { hash: 0, position: 0 },
        }
    }
}

impl Decoder for FastCDC {
    type Item = BytesMut;
    type Error = std::io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.len() <= self.min_size {
            return Ok(None);
        }
        let mut found_split_point = Option::<usize>::None;

        let start_point = std::cmp::max(self.state.position, self.min_size);

        for i in start_point..buf.len() {
            let byte = buf[i] as usize;
            self.state.hash = (self.state.hash >> 1) + TABLE[byte];

            // If we are < norm_size we start using the harder bit mask and if we go
            // beyond the norm_size start using the more difficult bit mask.
            let mask = if i < self.norm_size {
                self.mask_hard
            } else {
                self.mask_easy
            };
            if (self.state.hash & mask) == 0 {
                found_split_point = Some(i);
                break;
            }
            if i >= self.max_size {
                found_split_point = Some(i);
                break;
            }
        }

        if let Some(split_point) = found_split_point {
            if split_point >= self.min_size {
                self.state.reset();
                assert!(
                    split_point <= self.max_size,
                    "Expected {} < {}",
                    split_point,
                    self.max_size
                );
                return Ok(Some(buf.split_to(split_point)));
            }
        }
        self.state.position = found_split_point.unwrap_or(buf.len());

        return Ok(None);
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.decode(buf)? {
            Some(frame) => Ok(Some(frame)),
            // If we are EOF and have no more bytes in stream return the entire buffer.
            None => {
                self.state.reset();
                if buf.is_empty() {
                    // If our buffer is empty we don't have any more data.
                    return Ok(None);
                }
                Ok(Some(buf.split()))
            }
        }
    }
}

impl Clone for FastCDC {
    /// Clone configuration but with new state. This is useful where you can create
    /// a base FastCDC object then clone it when you want to process a new stream.
    fn clone(&self) -> Self {
        Self {
            min_size: self.min_size,
            avg_size: self.avg_size,
            max_size: self.max_size,

            norm_size: self.norm_size,
            mask_easy: self.mask_easy,
            mask_hard: self.mask_hard,

            state: State { hash: 0, position: 0 },
        }
    }
}

//
// TABLE contains seemingly "random" numbers which are created by ciphering a
// 1024-byte array of all zeros using a 32-byte key and 16-byte nonce (a.k.a.
// initialization vector) of all zeroes. The high bit of each value is cleared
// because 31-bit integers are immune from signed 32-bit integer overflow, which
// the implementation above relies on for hashing.
//
// While this may seem to be effectively noise, it is predictable noise, so the
// results are always the same. That is the most important aspect of the
// content-defined chunking algorithm, consistent results over time.
//
// Note: These values are based on:
// https://github.com/nlfiedler/fastcdc-rs/blob/1e804fe27444e37b2c4f93d540f861d170c8a257/src/lib.rs#L250
#[rustfmt::skip]
const TABLE: [u32; 256] = [
    0x5c95_c078, 0x2240_8989, 0x2d48_a214, 0x1284_2087, 0x530f_8afb, 0x4745_36b9,
    0x2963_b4f1, 0x44cb_738b, 0x4ea7_403d, 0x4d60_6b6e, 0x074e_c5d3, 0x3af3_9d18,
    0x7260_03ca, 0x37a6_2a74, 0x51a2_f58e, 0x7506_358e, 0x5d4a_b128, 0x4d4a_e17b,
    0x41e8_5924, 0x470c_36f7, 0x4741_cbe1, 0x01bb_7f30, 0x617c_1de3, 0x2b0c_3a1f,
    0x50c4_8f73, 0x21a8_2d37, 0x6095_ace0, 0x4191_67a0, 0x3caf_49b0, 0x40ce_a62d,
    0x66bc_1c66, 0x545e_1dad, 0x2bfa_77cd, 0x6e85_da24, 0x5fb0_bdc5, 0x652c_fc29,
    0x3a0a_e1ab, 0x2837_e0f3, 0x6387_b70e, 0x1317_6012, 0x4362_c2bb, 0x66d8_f4b1,
    0x37fc_e834, 0x2c9c_d386, 0x2114_4296, 0x6272_68a8, 0x650d_f537, 0x2805_d579,
    0x3b21_ebbd, 0x7357_ed34, 0x3f58_b583, 0x7150_ddca, 0x7362_225e, 0x620a_6070,
    0x2c5e_f529, 0x7b52_2466, 0x768b_78c0, 0x4b54_e51e, 0x75fa_07e5, 0x06a3_5fc6,
    0x30b7_1024, 0x1c86_26e1, 0x296a_d578, 0x28d7_be2e, 0x1490_a05a, 0x7cee_43bd,
    0x698b_56e3, 0x09dc_0126, 0x4ed6_df6e, 0x02c1_bfc7, 0x2a59_ad53, 0x29c0_e434,
    0x7d6c_5278, 0x5079_40a7, 0x5ef6_ba93, 0x68b6_af1e, 0x4653_7276, 0x611b_c766,
    0x155c_587d, 0x301b_a847, 0x2cc9_dda7, 0x0a43_8e2c, 0x0a69_d514, 0x744c_72d3,
    0x4f32_6b9b, 0x7ef3_4286, 0x4a0e_f8a7, 0x6ae0_6ebe, 0x669c_5372, 0x1240_2dcb,
    0x5fea_e99d, 0x76c7_f4a7, 0x6abd_b79c, 0x0dfa_a038, 0x20e2_282c, 0x730e_d48b,
    0x069d_ac2f, 0x168e_cf3e, 0x2610_e61f, 0x2c51_2c8e, 0x15fb_8c06, 0x5e62_bc76,
    0x6955_5135, 0x0adb_864c, 0x4268_f914, 0x349a_b3aa, 0x20ed_fdb2, 0x5172_7981,
    0x37b4_b3d8, 0x5dd1_7522, 0x6b2c_bfe4, 0x5c47_cf9f, 0x30fa_1ccd, 0x23de_db56,
    0x13d1_f50a, 0x64ed_dee7, 0x0820_b0f7, 0x46e0_7308, 0x1e2d_1dfd, 0x17b0_6c32,
    0x2500_36d8, 0x284d_bf34, 0x6829_2ee0, 0x362e_c87c, 0x087c_b1eb, 0x76b4_6720,
    0x1041_30db, 0x7196_6387, 0x482d_c43f, 0x2388_ef25, 0x5241_44e1, 0x44bd_834e,
    0x448e_7da3, 0x3fa6_eaf9, 0x3cda_215c, 0x3a50_0cf3, 0x395c_b432, 0x5195_129f,
    0x4394_5f87, 0x5186_2ca4, 0x56ea_8ff1, 0x2010_34dc, 0x4d32_8ff5, 0x7d73_a909,
    0x6234_d379, 0x64cf_bf9c, 0x36f6_589a, 0x0a2c_e98a, 0x5fe4_d971, 0x03bc_15c5,
    0x4402_1d33, 0x16c1_932b, 0x3750_3614, 0x1aca_f69d, 0x3f03_b779, 0x49e6_1a03,
    0x1f52_d7ea, 0x1c6d_dd5c, 0x0622_18ce, 0x07e7_a11a, 0x1905_757a, 0x7ce0_0a53,
    0x49f4_4f29, 0x4bcc_70b5, 0x39fe_ea55, 0x5242_cee8, 0x3ce5_6b85, 0x00b8_1672,
    0x46be_eccc, 0x3ca0_ad56, 0x2396_cee8, 0x7854_7f40, 0x6b08_089b, 0x66a5_6751,
    0x781e_7e46, 0x1e2c_f856, 0x3bc1_3591, 0x494a_4202, 0x5204_94d7, 0x2d87_459a,
    0x7575_55b6, 0x4228_4cc1, 0x1f47_8507, 0x75c9_5dff, 0x35ff_8dd7, 0x4e47_57ed,
    0x2e11_f88c, 0x5e1b_5048, 0x420e_6699, 0x226b_0695, 0x4d16_79b4, 0x5a22_646f,
    0x161d_1131, 0x125c_68d9, 0x1313_e32e, 0x4aa8_5724, 0x21dc_7ec1, 0x4ffa_29fe,
    0x7296_8382, 0x1ca8_eef3, 0x3f3b_1c28, 0x39c2_fb6c, 0x6d76_493f, 0x7a22_a62e,
    0x789b_1c2a, 0x16e0_cb53, 0x7dec_eeeb, 0x0dc7_e1c6, 0x5c75_bf3d, 0x5221_8333,
    0x106d_e4d6, 0x7dc6_4422, 0x6559_0ff4, 0x2c02_ec30, 0x64a9_ac67, 0x59ca_b2e9,
    0x4a21_d2f3, 0x0f61_6e57, 0x23b5_4ee8, 0x0273_0aaa, 0x2f3c_634d, 0x7117_fc6c,
    0x01ac_6f05, 0x5a9e_d20c, 0x158c_4e2a, 0x42b6_99f0, 0x0c7c_14b3, 0x02bd_9641,
    0x15ad_56fc, 0x1c72_2f60, 0x7da1_af91, 0x23e0_dbcb, 0x0e93_e12b, 0x64b2_791d,
    0x440d_2476, 0x588e_a8dd, 0x4665_a658, 0x7446_c418, 0x1877_a774, 0x5626_407e,
    0x7f63_bd46, 0x32d2_dbd8, 0x3c79_0f4a, 0x772b_7239, 0x6f8b_2826, 0x677f_f609,
    0x0dc8_2c11, 0x23ff_e354, 0x2eac_53a6, 0x1613_9e09, 0x0afd_0dbc, 0x2a4d_4237,
    0x56a3_68c7, 0x2343_25e4, 0x2dce_9187, 0x32e8_ea7e
];
