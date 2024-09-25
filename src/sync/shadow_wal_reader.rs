use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::os::unix::fs::MetadataExt;

use crate::base::shadow_wal_file;
use crate::database::DatabaseInfo;
use crate::database::WalGenerationPos;
use crate::error::Error;
use crate::error::Result;
use crate::sqlite::align_frame;

pub struct ShadowWalReader {
    pub position: WalGenerationPos,
    pub file: File,
    pub left: u64,
}

impl ShadowWalReader {
    // ShadowWALReader opens a reader for a shadow WAL file at a given position.
    // If the reader is at the end of the file, it attempts to return the next file.
    //
    // The caller should check Pos() & Size() on the returned reader to check offset.
    pub fn try_create(pos: WalGenerationPos, info: &DatabaseInfo) -> Result<ShadowWalReader> {
        let reader = ShadowWalReader::new(pos.clone(), info)?;
        if reader.left > 0 {
            return Ok(reader);
        }

        // no data, try next
        let mut pos = pos;
        pos.index += 1;
        pos.offset = 0;

        match ShadowWalReader::new(pos, info) {
            Err(e) => {
                if e.code() == Error::STORAGE_NOT_FOUND {
                    return Err(Error::from_error_code(
                        Error::UNEXPECTED_EOF_ERROR,
                        "no wal shadow file".to_string(),
                    ));
                }
                Err(e)
            }
            Ok(reader) => Ok(reader),
        }
    }

    fn new(pos: WalGenerationPos, info: &DatabaseInfo) -> Result<ShadowWalReader> {
        let file_name = shadow_wal_file(&info.meta_dir, pos.generation.as_str(), pos.index);
        let mut file = OpenOptions::new().read(true).open(file_name)?;
        let size = align_frame(info.page_size, file.metadata()?.size());

        if pos.offset > size {
            return Err(Error::WalReaderOffsetTooHighError(format!(
                "wal reader offset {} > file size {}",
                pos.offset, size
            )));
        }

        // Move file handle to offset position.
        file.seek(SeekFrom::Start(pos.offset))?;
        let left = size - pos.offset;
        Ok(ShadowWalReader {
            position: pos,
            file,
            left,
        })
    }

    pub fn position(&self) -> WalGenerationPos {
        self.position.clone()
    }
}

impl Read for ShadowWalReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.left == 0 {
            // nothing to read
            return Ok(0);
        }
        let n = buf.len() as u64;
        if self.left < n {
            return Err(std::io::Error::from(io::ErrorKind::Interrupted));
        }

        let ret = self.file.read(buf)?;
        self.left -= n;
        self.position.offset += n;

        Ok(ret)
    }
}
