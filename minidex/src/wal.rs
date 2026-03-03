use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use crate::entry::IndexEntry;

pub struct Wal {
    path: PathBuf,
    writer: Option<BufWriter<File>>,
}

impl Wal {
    pub(crate) fn open<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        Ok(Self {
            path,
            writer: Some(BufWriter::new(file)),
        })
    }

    pub(crate) fn append(&mut self, path: &str, entry: &IndexEntry) -> std::io::Result<()> {
        let writer = self.writer.as_mut().expect("WAL writer missing");

        let path_bytes = path.as_bytes();
        let path_len = path_bytes.len() as u32;

        writer.write_all(&path_len.to_le_bytes())?;
        writer.write_all(path_bytes)?;
        writer.write_all(&entry.to_bytes())?;
        Ok(())
    }

    pub(crate) fn flush(&mut self) -> std::io::Result<()> {
        if let Some(writer) = self.writer.as_mut() {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }

        Ok(())
    }

    pub(crate) fn replay<P: AsRef<Path>>(path: P) -> std::io::Result<Vec<(String, IndexEntry)>> {
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };

        let mut reader = BufReader::new(file);
        let mut results = Vec::new();
        let mut len_buf = [0u8; 4];

        loop {
            match reader.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut path_buf = vec![0u8; len];

            if let Err(e) = reader.read_exact(&mut path_buf) {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(e);
            }
            let path = String::from_utf8_lossy(&path_buf).to_string();

            let mut entry_buf = [0u8; IndexEntry::SIZE];
            if let Err(e) = reader.read_exact(&mut entry_buf) {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    log::warn!("Wal parser hit EOF early! Stopped at {}", results.len());
                    break;
                }
                return Err(e);
            }
            let entry = IndexEntry::from_bytes(&entry_buf);

            results.push((path, entry));
        }

        log::debug!("Wal replay complete, recovered {} entries", results.len());
        Ok(results)
    }

    pub(crate) fn rotate<P: AsRef<Path>>(&mut self, path: P) -> std::io::Result<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }

        std::fs::rename(&self.path, path)?;

        let new_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;

        self.writer = Some(BufWriter::new(new_file));

        Ok(())
    }
}
