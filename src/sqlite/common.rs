use std::fs::File;
use std::os::unix::fs::FileExt;

use crate::error::Error;
use crate::error::Result;

pub const WALFRAME_HEADER_SIZE: usize = 24;
pub const WAL_HEADER_SIZE: usize = 32;
pub const WAL_HEADER_CHECKSUM_OFFSET: u64 = 24;

pub const WAL_HEADER_BIG_ENDIAN_MAGIC: [u8; 4] = [0x37, 0x7f, 0x06, 0x83];
pub const WAL_HEADER_LITTLE_ENDIAN_MAGIC: [u8; 4] = [0x37, 0x7f, 0x06, 0x82];

// SQLite checkpoint modes.
pub const CHECKPOINT_MODE_PASSIVE: &str = "PASSIVE";
pub const CHECKPOINT_MODE_FULL: &str = "FULL";
pub const CHECKPOINT_MODE_RESTART: &str = "RESTART";
pub const CHECKPOINT_MODE_TRUNCATE: &str = "TRUNCATE";

// implementation of sqlite check algorithm
pub fn checksum(data: &[u8], s1: u32, s2: u32, is_big_endian: bool) -> (u32, u32) {
    let mut i = 0;
    let mut s1: u32 = s1;
    let mut s2: u32 = s2;
    while i < data.len() {
        let bytes1 = &data[i..i + 4];
        let bytes2 = &data[i + 4..i + 8];
        let (n1, n2) = if is_big_endian {
            (
                u32::from_be_bytes(bytes1.try_into().unwrap()),
                u32::from_be_bytes(bytes2.try_into().unwrap()),
            )
        } else {
            (
                u32::from_le_bytes(bytes1.try_into().unwrap()),
                u32::from_le_bytes(bytes2.try_into().unwrap()),
            )
        };
        // use `wrapping_add` instead of `+` directly, or else will be overflow panic
        s1 = s1.wrapping_add(n1).wrapping_add(s2);
        s2 = s2.wrapping_add(n2).wrapping_add(s1);

        i += 8;
    }

    (s1, s2)
}

pub fn read_last_checksum(file: &mut File, page_size: u32) -> Result<(u32, u32)> {
    let metadata = file.metadata()?;
    let fsize = metadata.len();
    let sz = align_frame(page_size, fsize);
    let offset = if fsize > WAL_HEADER_SIZE as u64 {
        sz - page_size as u64 - WALFRAME_HEADER_SIZE as u64 + WAL_HEADER_CHECKSUM_OFFSET
    } else {
        WAL_HEADER_CHECKSUM_OFFSET as u64
    };

    let mut buf = [0u8; 8];
    let n = file.read_at(&mut buf, offset)?;
    if n != buf.len() {
        return Err(Error::UnexpectedEofError(
            "UnexpectedEOFError when read last checksum".to_string(),
        ));
    }

    let checksum1 = &buf[0..4];
    let checksum1 = u32::from_be_bytes(checksum1.try_into()?);
    let checksum2 = &buf[4..8];
    let checksum2 = u32::from_be_bytes(checksum2.try_into()?);

    Ok((checksum1, checksum2))
}

pub fn align_frame(page_size: u32, offset: u64) -> u64 {
    if offset < WAL_HEADER_SIZE as u64 {
        return 0;
    }

    let frame_size = WALFRAME_HEADER_SIZE as u64 + page_size as u64;
    let frame_num = (offset as u64 - WAL_HEADER_SIZE as u64) / frame_size;

    (frame_num * frame_size) + WAL_HEADER_SIZE as u64
}
