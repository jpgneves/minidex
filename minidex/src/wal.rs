use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use crate::entry::IndexEntry;

const WAL_RECORD_INSERT: u8 = 0;
const WAL_RECORD_TOMBSTONE: u8 = 1;

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

    pub(crate) fn append(
        &mut self,
        path: &str,
        volume: &str,
        entry: &IndexEntry,
    ) -> std::io::Result<()> {
        let writer = self.writer.as_mut().expect("WAL writer missing");

        let path_bytes = path.as_bytes();
        let path_len = path_bytes.len() as u32;

        let volume_bytes = volume.as_bytes();
        let volume_len = volume_bytes.len() as u32;

        writer.write_all(&[WAL_RECORD_INSERT])?;
        writer.write_all(&path_len.to_le_bytes())?;
        writer.write_all(path_bytes)?;
        writer.write_all(&volume_len.to_le_bytes())?;
        writer.write_all(volume_bytes)?;
        writer.write_all(&entry.as_bytes())?;
        Ok(())
    }

    pub fn write_prefix_tombstone(
        &mut self,
        volume: Option<&str>,
        prefix: &str,
        seq: u64,
    ) -> std::io::Result<()> {
        let writer = self.writer.as_mut().expect("WAL writer missing");
        let prefix_bytes = prefix.as_bytes();

        writer.write_all(&[WAL_RECORD_TOMBSTONE])?;

        writer.write_all(&seq.to_le_bytes())?;

        if let Some(volume) = volume {
            writer.write_all(&[1])?;

            let volume_bytes = volume.as_bytes();
            writer.write_all(&(volume_bytes.len() as u32).to_le_bytes())?;
            writer.write_all(volume_bytes)?;
        } else {
            writer.write_all(&[0])?;
        }

        writer.write_all(&(prefix_bytes.len() as u32).to_le_bytes())?;

        writer.write_all(prefix_bytes)?;

        writer.flush()?;
        writer.get_ref().sync_data()?;

        Ok(())
    }

    pub(crate) fn flush(&mut self) -> std::io::Result<()> {
        if let Some(writer) = self.writer.as_mut() {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }

        Ok(())
    }

    pub(crate) fn replay<P: AsRef<Path>>(path: P) -> std::io::Result<ReplayData> {
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(ReplayData::new()),
            Err(e) => return Err(e),
        };

        let mut reader = BufReader::new(file);
        let mut results = ReplayData::new();
        let mut len_buf = [0u8; 4];

        loop {
            let mut type_buf = [0u8; 1];

            if let Err(e) = reader.read_exact(&mut type_buf) {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    break; // Reached the end of the WAL normally
                }
                return Err(e);
            }

            match type_buf[0] {
                WAL_RECORD_INSERT => {
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

                    if let Err(e) = reader.read_exact(&mut len_buf) {
                        if e.kind() == std::io::ErrorKind::UnexpectedEof {
                            log::warn!(
                                "Wal parser hit EOF early! Stopped at {}",
                                results.inserts.len()
                            );
                            break;
                        }
                        return Err(e);
                    }
                    let len = u32::from_le_bytes(len_buf) as usize;
                    let mut vol_buf = vec![0u8; len];

                    if let Err(e) = reader.read_exact(&mut vol_buf) {
                        if e.kind() == std::io::ErrorKind::UnexpectedEof {
                            break;
                        }
                        return Err(e);
                    }

                    let volume = String::from_utf8_lossy(&vol_buf).to_string();

                    let mut entry_buf = [0u8; IndexEntry::SIZE];
                    if let Err(e) = reader.read_exact(&mut entry_buf) {
                        if e.kind() == std::io::ErrorKind::UnexpectedEof {
                            log::warn!(
                                "Wal parser hit EOF early! Stopped at {}",
                                results.inserts.len()
                            );
                            break;
                        }
                        return Err(e);
                    }
                    let entry = IndexEntry::from_bytes(&entry_buf);

                    results.inserts.push((path, volume, entry));
                }
                WAL_RECORD_TOMBSTONE => {
                    let mut seq_buf = [0u8; 8];
                    reader.read_exact(&mut seq_buf)?;
                    let seq = u64::from_le_bytes(seq_buf);

                    let mut len_buf = [0u8; 4];

                    let mut flag_buf = [0u8; 1];
                    reader.read_exact(&mut flag_buf)?;
                    let has_volume = flag_buf[0] == 1;

                    let volume =
                        if has_volume {
                            reader.read_exact(&mut len_buf)?;
                            let len = u32::from_le_bytes(len_buf) as usize;

                            let mut volume_buf = vec![0u8; len];
                            reader.read_exact(&mut volume_buf)?;

                            Some(String::from_utf8(volume_buf).map_err(|e| {
                                std::io::Error::new(std::io::ErrorKind::InvalidData, e)
                            })?)
                        } else {
                            None
                        };

                    reader.read_exact(&mut len_buf)?;
                    let len = u32::from_le_bytes(len_buf) as usize;

                    let mut prefix_buf = vec![0u8; len];
                    reader.read_exact(&mut prefix_buf)?;
                    let prefix = String::from_utf8(prefix_buf)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

                    results.tombstones.push((volume, prefix, seq));
                }
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Corrupted WAL",
                    ));
                }
            }
        }

        log::debug!(
            "Wal replay complete, recovered {} entries",
            results.inserts.len()
        );
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

pub(crate) struct ReplayData {
    pub inserts: Vec<(String, String, IndexEntry)>,
    pub tombstones: Vec<(Option<String>, String, u64)>,
}

impl ReplayData {
    fn new() -> Self {
        Self {
            inserts: Vec::new(),
            tombstones: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Kind;
    use crate::opstamp::Opstamp;

    #[test]
    fn test_wal_append_and_replay() -> std::io::Result<()> {
        let temp_dir = std::env::temp_dir().join(format!("minidex_test_wal_{}", rand_id()));
        std::fs::create_dir_all(&temp_dir)?;
        let wal_path = temp_dir.join("test.wal");

        {
            let mut wal = Wal::open(&wal_path)?;
            let entry = IndexEntry {
                opstamp: Opstamp::insertion(10),
                kind: Kind::File,
                last_modified: 100,
                last_accessed: 100,
                category: 0,
                volume_type: crate::common::VolumeType::Local,
            };
            wal.append("/foo", "vol1", &entry)?;
            wal.write_prefix_tombstone(None, "/bar", 20)?;
            wal.write_prefix_tombstone(Some("vol1"), "/baz", 30)?;
            wal.flush()?;
        }

        let replay = Wal::replay(&wal_path)?;
        assert_eq!(replay.inserts.len(), 1);
        assert_eq!(replay.inserts[0].0, "/foo");
        assert_eq!(replay.inserts[0].1, "vol1");
        assert_eq!(replay.inserts[0].2.opstamp.sequence(), 10);

        assert_eq!(replay.tombstones.len(), 2);
        assert_eq!(replay.tombstones[0].0, None);
        assert_eq!(replay.tombstones[0].1, "/bar");
        assert_eq!(replay.tombstones[0].2, 20);

        assert_eq!(replay.tombstones[1].0, Some("vol1".to_string()));
        assert_eq!(replay.tombstones[1].1, "/baz");
        assert_eq!(replay.tombstones[1].2, 30);

        std::fs::remove_dir_all(temp_dir)?;
        Ok(())
    }

    #[test]
    fn test_wal_rotation() -> std::io::Result<()> {
        let temp_dir = std::env::temp_dir().join(format!("minidex_test_wal_rot_{}", rand_id()));
        std::fs::create_dir_all(&temp_dir)?;
        let wal_path = temp_dir.join("journal.wal");
        let rot_path = temp_dir.join("journal.old.wal");

        {
            let mut wal = Wal::open(&wal_path)?;
            let entry = IndexEntry {
                opstamp: Opstamp::insertion(10),
                kind: Kind::File,
                last_modified: 100,
                last_accessed: 100,
                category: 0,
                volume_type: crate::common::VolumeType::Local,
            };
            wal.append("/foo", "vol1", &entry)?;
            wal.rotate(&rot_path)?;

            let entry2 = IndexEntry {
                opstamp: Opstamp::insertion(20),
                kind: Kind::File,
                last_modified: 200,
                last_accessed: 200,
                category: 0,
                volume_type: crate::common::VolumeType::Local,
            };
            wal.append("/bar", "vol1", &entry2)?;
            wal.flush()?;
        }

        // Verify rotated file
        let replay_rot = Wal::replay(&rot_path)?;
        assert_eq!(replay_rot.inserts.len(), 1);
        assert_eq!(replay_rot.inserts[0].0, "/foo");

        // Verify current file
        let replay_cur = Wal::replay(&wal_path)?;
        assert_eq!(replay_cur.inserts.len(), 1);
        assert_eq!(replay_cur.inserts[0].0, "/bar");

        std::fs::remove_dir_all(temp_dir)?;
        Ok(())
    }

    fn rand_id() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}
