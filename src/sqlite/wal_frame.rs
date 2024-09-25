use std::io::Read;

use super::from_be_bytes_at;
use super::WAL_FRAME_HEADER_SIZE;
use crate::error::Result;

#[derive(Clone, Debug, PartialEq)]
pub struct WALFrame {
    pub data: Vec<u8>,
    pub page_num: u32,
    pub db_size: u32,
    pub salt1: u32,
    pub salt2: u32,
    pub checksum1: u32,
    pub checksum2: u32,
}

impl WALFrame {
    pub fn read<R: Read + ?Sized>(reader: &mut R, page_size: u64) -> Result<WALFrame> {
        let mut data: Vec<u8> = vec![0u8; (WAL_FRAME_HEADER_SIZE + page_size) as usize];
        reader.read_exact(&mut data)?;

        let page_num = from_be_bytes_at(&data, 0)?;
        let db_size = from_be_bytes_at(&data, 4)?;

        let checksum1 = from_be_bytes_at(&data, 16)?;
        let checksum2 = from_be_bytes_at(&data, 20)?;

        let salt1 = from_be_bytes_at(&data, 8)?;
        let salt2 = from_be_bytes_at(&data, 12)?;

        Ok(WALFrame {
            data,
            page_num,
            db_size,
            salt1,
            salt2,
            checksum1,
            checksum2,
        })
    }
}
