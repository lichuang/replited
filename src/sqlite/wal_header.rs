use std::fs::File;
use std::io::Read;

use crate::base::is_power_of_two;
use crate::error::Error;
use crate::error::Result;
use crate::sqlite::checksum;
use crate::sqlite::WAL_HEADER_BIG_ENDIAN_MAGIC;
use crate::sqlite::WAL_HEADER_LITTLE_ENDIAN_MAGIC;
use crate::sqlite::WAL_HEADER_SIZE;

#[derive(Clone, Debug, PartialEq)]
pub struct WALHeader {
    pub data: Vec<u8>,
    pub salt: u64,
    pub page_size: u32,
    pub is_big_endian: bool,
}

impl WALHeader {
    // see: https://www.sqlite.org/fileformat2.html#walformat
    pub fn read_from(file: &mut File) -> Result<WALHeader> {
        if file.metadata()?.len() < WAL_HEADER_SIZE as u64 {
            return Err(Error::SqliteWalHeaderError("Invalid WAL file"));
        }

        let mut data: Vec<u8> = vec![0u8; WAL_HEADER_SIZE];
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
        let page_size = u32::from_be_bytes(page_size.try_into()?);
        if !is_power_of_two(page_size) || page_size < 1024 {
            return Err(Error::SqliteWalHeaderError("Invalid page size"));
        }

        // checksum
        let (s1, s2) = checksum(&data[0..24], 0, 0, is_big_endian);

        let checksum1 = &data[24..28];
        let checksum1 = u32::from_be_bytes(checksum1.try_into()?);
        let checksum2 = &data[28..32];
        let checksum2 = u32::from_be_bytes(checksum2.try_into()?);

        if checksum1 != s1 || checksum2 != s2 {
            return Err(Error::SqliteWalHeaderError("Invalid wal header checksum"));
        }

        let s = &data[16..24];
        let salt = u64::from_be_bytes(s.try_into()?);

        Ok(WALHeader {
            data,
            salt,
            page_size,
            is_big_endian,
        })
    }

    pub fn read(file_path: &str) -> Result<WALHeader> {
        let mut file = File::open(file_path)?;

        Self::read_from(&mut file)
    }
}
