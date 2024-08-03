use std::fs::File;
use std::io::Read;

use super::checksum;
use super::WALHeader;
use super::WALFRAME_HEADER_SIZE;
use crate::error::Error;
use crate::error::Result;

#[derive(Clone, Debug)]
pub struct WALFrame {
    pub data: Vec<u8>,
    pub page_num: u32,
    pub db_size: u32,
}

impl WALFrame {
    pub fn read(
        file: &mut File,
        ck1: u32,
        ck2: u32,
        page_size: u32,
        wal_header: &WALHeader,
    ) -> Result<WALFrame> {
        let metadata = file.metadata()?;
        if metadata.len() < WALFRAME_HEADER_SIZE as u64 {
            return Err(Error::SqliteWalFrameHeaderError("Invalid WAL frame header"));
        }

        let mut data: Vec<u8> = vec![0u8; WALFRAME_HEADER_SIZE + page_size as usize];
        // let mut buf = data.as_mut_slice();
        file.read_exact(&mut data)?;

        // read page num
        let page_num = &data[0..4];
        let page_num = u32::from_be_bytes(page_num.try_into()?);

        let db_size = &data[4..8];
        let db_size = u32::from_be_bytes(db_size.try_into()?);

        let salt1 = &data[8..12];
        let salt1 = u32::from_be_bytes(salt1.try_into()?);
        let salt2 = &data[12..16];
        let salt2 = u32::from_be_bytes(salt2.try_into()?);
        if salt1 != wal_header.salt1 || salt2 != wal_header.salt2 {
            return Err(Error::SqliteWalFrameHeaderError(
                "Invalid wal frame header checksum",
            ));
        }

        // checksum
        let checksum1 = &data[16..20];
        let checksum1 = u32::from_be_bytes(checksum1.try_into().unwrap());
        let checksum2 = &data[20..24];
        let checksum2 = u32::from_be_bytes(checksum2.try_into().unwrap());

        // frame header
        let (ck1, ck2) = checksum(&data[0..8], ck1, ck2, wal_header.is_big_endian);
        // frame data
        let (ck1, ck2) = checksum(&data[24..], ck1, ck2, wal_header.is_big_endian);

        if checksum1 != ck1 || checksum2 != ck2 {
            return Err(Error::SqliteWalFrameHeaderError(
                "Invalid wal frame header checksum",
            ));
        }

        Ok(WALFrame {
            data,
            page_num,
            db_size,
        })
    }
}
