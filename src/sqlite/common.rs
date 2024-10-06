use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use std::os::unix::fs::MetadataExt;

use crate::error::Error;
use crate::error::Result;

pub const WAL_FRAME_HEADER_SIZE: u64 = 24;
pub const WAL_HEADER_SIZE: u64 = 32;

static WAL_HEADER_CHECKSUM_OFFSET: u64 = 24;
static WAL_FRAME_HEADER_CHECKSUM_OFFSET: u64 = 16;

pub const WAL_HEADER_BIG_ENDIAN_MAGIC: [u8; 4] = [0x37, 0x7f, 0x06, 0x83];
pub const WAL_HEADER_LITTLE_ENDIAN_MAGIC: [u8; 4] = [0x37, 0x7f, 0x06, 0x82];

// SQLite checkpoint modes.
static CHECKPOINT_MODE_PASSIVE: &str = "PASSIVE";
static CHECKPOINT_MODE_FULL: &str = "FULL";
static CHECKPOINT_MODE_RESTART: &str = "RESTART";
static CHECKPOINT_MODE_TRUNCATE: &str = "TRUNCATE";

#[derive(Clone)]
pub enum CheckpointMode {
    Passive,
    Full,
    Restart,
    Truncate,
}

impl CheckpointMode {
    pub fn as_str(&self) -> &str {
        match self {
            CheckpointMode::Passive => CHECKPOINT_MODE_PASSIVE,
            CheckpointMode::Full => CHECKPOINT_MODE_FULL,
            CheckpointMode::Restart => CHECKPOINT_MODE_RESTART,
            CheckpointMode::Truncate => CHECKPOINT_MODE_TRUNCATE,
        }
    }
}

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

pub fn read_last_checksum(file_name: &str, page_size: u64) -> Result<(u32, u32)> {
    let file = OpenOptions::new().read(true).open(file_name)?;
    let metadata = file.metadata()?;
    let fsize = metadata.size();
    let offset = if fsize > WAL_HEADER_SIZE {
        let sz = align_frame(page_size, fsize);
        sz - page_size - WAL_FRAME_HEADER_SIZE + WAL_FRAME_HEADER_CHECKSUM_OFFSET
    } else {
        WAL_HEADER_CHECKSUM_OFFSET
    };

    let mut buf = [0u8; 8];
    let n = file.read_at(&mut buf, offset)?;
    if n != buf.len() {
        return Err(Error::UnexpectedEofError(
            "UnexpectedEOFError when read last checksum".to_string(),
        ));
    }

    let checksum1 = from_be_bytes_at(&buf, 0)?;
    let checksum2 = from_be_bytes_at(&buf, 4)?;

    Ok((checksum1, checksum2))
}

// returns a frame-aligned offset.
// Returns zero if offset is less than the WAL header size.
pub fn align_frame(page_size: u64, offset: u64) -> u64 {
    if offset < WAL_HEADER_SIZE {
        return 0;
    }

    let page_size = page_size as i64;
    let offset = offset as i64;
    let frame_size = WAL_FRAME_HEADER_SIZE as i64 + page_size;
    let frame_num = (offset - WAL_HEADER_SIZE as i64) / frame_size;

    (frame_num * frame_size) as u64 + WAL_HEADER_SIZE
}

pub(crate) fn from_be_bytes_at(data: &[u8], offset: usize) -> Result<u32> {
    let p = &data[offset..offset + 4];
    Ok(u32::from_be_bytes(p.try_into()?))
}

#[cfg(test)]
mod tests {
    use super::align_frame;
    use crate::error::Result;

    #[test]
    fn test_align_frame() -> Result<()> {
        assert_eq!(4152, align_frame(4096, 4152));

        Ok(())
    }
}
