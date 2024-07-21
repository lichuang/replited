pub const WALFRAME_HEADER_SIZE: u64 = 24;
pub const WAL_HEADER_SIZE: usize = 32;

const BIG_ENDIAN_WAL_HEADER_MAGIC: [u8; 4] = [0x37, 0x7f, 0x06, 0x83];
const LITTLE_ENDIAN_WAL_HEADER_MAGIC: [u8; 4] = [0x37, 0x7f, 0x06, 0x82];

use std::fs::File;
use std::io::Read;

use crate::base::is_power_of_two;
use crate::error::Error;
use crate::error::Result;

#[derive(Clone, Debug)]
pub struct WALHeader {
    data: [u8; WAL_HEADER_SIZE],
    pub page_size: u64,
}

// implementation of sqlite check algorithm
fn checksum(data: &[u8], is_big_endian: bool) -> (u32, u32) {
    let mut i = 0;
    let mut s1: u32 = 0;
    let mut s2: u32 = 0;
    while i < data.len() {
        let bytes1 = &data[i..i + 4];
        let bytes2 = &data[i + 4..i + 8];
        let (n1, n2) = if is_big_endian {
            let n1 = u32::from_be_bytes(bytes1.try_into().unwrap());
            let n2 = u32::from_be_bytes(bytes2.try_into().unwrap());

            (n1, n2)
        } else {
            let n1 = u32::from_le_bytes(bytes1.try_into().unwrap());
            let n2 = u32::from_le_bytes(bytes2.try_into().unwrap());

            (n1, n2)
        };
        // use `wrapping_add` instead of `+` directly, or else will be overflow
        s1 = s1.wrapping_add(n1);
        s1 = s1.wrapping_add(s2);
        s2 = s2.wrapping_add(n2);
        s2 = s2.wrapping_add(s1);

        i += 8;
    }

    (s1, s2)
}

impl WALHeader {
    // see: https://www.sqlite.org/fileformat2.html#walformat
    pub fn read(file_path: &str) -> Result<WALHeader> {
        let mut file = File::open(file_path)?;

        if file.metadata()?.len() < WAL_HEADER_SIZE as u64 {
            return Err(Error::SqliteWalHeaderError("Invalid WAL file"));
        }

        let mut data = [0u8; WAL_HEADER_SIZE];
        file.read_exact(&mut data)?;

        let magic: &[u8] = &data[0..4];
        // check magic
        let is_big_endian = if magic == &BIG_ENDIAN_WAL_HEADER_MAGIC {
            true
        } else if magic == &LITTLE_ENDIAN_WAL_HEADER_MAGIC {
            false
        } else {
            return Err(Error::SqliteWalHeaderError("Unknown WAL file header magic"));
        };

        // check page size
        let page_size = &data[8..12];
        let page_size = u32::from_be_bytes(page_size.try_into().unwrap());
        if !is_power_of_two(page_size) || page_size < 1024 {
            return Err(Error::SqliteWalHeaderError("Invalid page size"));
        }

        // checksum
        let (s1, s2) = checksum(&data[0..24], is_big_endian);

        let checksum1 = &data[24..28];
        let checksum1 = u32::from_be_bytes(checksum1.try_into().unwrap());
        let checksum2 = &data[28..32];
        let checksum2 = u32::from_be_bytes(checksum2.try_into().unwrap());

        if checksum1 != s1 || checksum2 != s2 {
            return Err(Error::SqliteWalHeaderError("Invalid wal header checksum"));
        }
        Ok(WALHeader {
            data,
            page_size: page_size as u64,
        })
    }
}
