pub const WALFRAME_HEADER_SIZE: u64 = 24;
const BIG_ENDIAN_WAL_HEADER_MAGIC: [u8; 4] = [0x37, 0x7f, 0x06, 0x83];
const LITTLE_ENDIAN_WAL_HEADER_MAGIC: [u8; 4] = [0x37, 0x7f, 0x06, 0x82];

use std::fs::File;
use std::io::Read;

use crate::base::is_power_of_two;
use crate::error::Error;
use crate::error::Result;

// see: https://www.sqlite.org/fileformat2.html#walformat
pub fn read_wal_header(file_path: &str) -> Result<WALHeader> {
    let mut file = File::open(file_path)?;

    let mut magic = [0u8; 4];
    file.read_exact(&mut magic)?;

    // check magic
    let is_big_endian = match magic {
        BIG_ENDIAN_WAL_HEADER_MAGIC => true,
        LITTLE_ENDIAN_WAL_HEADER_MAGIC => false,
        _ => {
            return Err(Error::SqliteWalHeaderError("Unknown WAL file header magic"));
        }
    };

    let mut version = [0u8; 4];
    file.read_exact(&mut version)?;

    let mut page_size = [0u8; 4];
    file.read_exact(&mut page_size)?;
    let page_size = u32::from_be_bytes(page_size.clone());
    // check page size
    if !is_power_of_two(page_size) || page_size < 1024 {
        return Err(Error::SqliteWalHeaderError("Invalid page size"));
    }

    let mut checkpoint_number = [0u8; 4];
    file.read_exact(&mut checkpoint_number)?;

    let mut salt1 = [0u8; 4];
    file.read_exact(&mut salt1)?;

    let mut salt2 = [0u8; 4];
    file.read_exact(&mut salt2)?;

    let mut checksum1 = [0u8; 4];
    file.read_exact(&mut checksum1)?;

    let mut checksum2 = [0u8; 4];
    file.read_exact(&mut checksum2)?;

    Ok(WALHeader {
        magic,
        version,
        page_size: page_size as u64,
        checkpoint_number,
        salt1,
        salt2,
        checksum1,
        checksum2,
        is_big_endian,
    })
}

#[derive(Clone, Debug)]
pub struct WALHeader {
    pub magic: [u8; 4],
    pub version: [u8; 4],
    pub page_size: u64,
    pub checkpoint_number: [u8; 4],
    pub salt1: [u8; 4],
    pub salt2: [u8; 4],
    pub checksum1: [u8; 4],
    pub checksum2: [u8; 4],

    pub is_big_endian: bool,
}
