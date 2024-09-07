use std::fs::OpenOptions;
use std::io::Read;
use std::io::Write;

use lz4::Decoder;
use lz4::EncoderBuilder;

use crate::error::Result;

const COMPRESS_BUFFER_SIZE: usize = 102400;

pub fn compress_file(file_name: &str) -> Result<Vec<u8>> {
    // Open db file descriptor
    let mut reader = OpenOptions::new().read(true).open(file_name)?;
    let bytes = reader.metadata()?.len() as usize;
    let mut buffer = Vec::with_capacity(bytes);
    let mut encoder = EncoderBuilder::new().build(&mut buffer)?;

    let mut temp_buffer = vec![0; COMPRESS_BUFFER_SIZE];

    loop {
        let bytes_read = reader.read(&mut temp_buffer)?;
        if bytes_read == 0 {
            break; // EOF
        }
        encoder.write_all(&temp_buffer[..bytes_read])?;
    }
    let (compressed_data, result) = encoder.finish();
    result?;

    Ok(compressed_data.to_owned())
}

pub fn decompressed_data(compressed_data: Vec<u8>) -> Result<Vec<u8>> {
    let compressed_data = compressed_data.as_slice();
    let mut decoder = Decoder::new(compressed_data)?;
    let mut decompressed_data = Vec::new();
    let mut buffer = vec![0; COMPRESS_BUFFER_SIZE];

    loop {
        let bytes_read = decoder.read(&mut buffer)?;
        if bytes_read == 0 {
            break; // EOF
        }
        decompressed_data.extend_from_slice(&buffer[..bytes_read]);
    }

    Ok(decompressed_data)
}
