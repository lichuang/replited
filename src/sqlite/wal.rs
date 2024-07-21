use std::fs::File;
use std::io::Read;

use crate::base::is_power_of_two;
use crate::error::Error;
use crate::error::Result;
use crate::sqlite::checksum;
use crate::sqlite::WAL_HEADER_BIG_ENDIAN_MAGIC;
use crate::sqlite::WAL_HEADER_LITTLE_ENDIAN_MAGIC;
use crate::sqlite::WAL_HEADER_SIZE;

#[derive(Clone, Debug)]
pub struct WALHeader {
    data: [u8; WAL_HEADER_SIZE],
    pub page_size: u64,
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
        let is_big_endian = if magic == &WAL_HEADER_BIG_ENDIAN_MAGIC {
            true
        } else if magic == &WAL_HEADER_LITTLE_ENDIAN_MAGIC {
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
