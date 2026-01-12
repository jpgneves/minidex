use std::{
    fs::File,
    io::{BufWriter, Write},
    str::FromStr,
    sync::Arc,
};

use crate::{Path, PathBuf, entry::IndexEntry};
use fst::{Map, MapBuilder};
use lockfile::Lockfile;
use memmap2::Mmap;
use thiserror::Error;

pub(crate) mod compactor;

const LAST_OP_FILE: &str = "last_op";
const LOCK_FILE: &str = ".minidex.lock";

const SEGMENT_EXT: &str = "seg";
const DATA_EXT: &str = "dat";

pub(crate) struct Segment {
    map: Map<Mmap>,
    data: Mmap,
}

impl Segment {
    pub fn load(path: PathBuf) -> Result<Self, SegmentedIndexError> {
        let entry_file_path = path.with_extension(SEGMENT_EXT);
        let entry_file = File::open(&entry_file_path).map_err(SegmentedIndexError::Io)?;
        let mmap = unsafe { Mmap::map(&entry_file).map_err(SegmentedIndexError::Io)? };
        let map = Map::new(mmap).map_err(SegmentedIndexError::Fst)?;

        // Load the data file for the same segment
        let dat_file_path = path.with_extension(DATA_EXT);
        let dat_file = File::open(dat_file_path).map_err(SegmentedIndexError::Io)?;
        let data = unsafe { Mmap::map(&dat_file).map_err(SegmentedIndexError::Io)? };
        Ok(Self { map, data })
    }

    pub(crate) fn get_entry(&self, offset: u64) -> Option<IndexEntry> {
        let start = offset as usize;
        let end = start + IndexEntry::SIZE;

        if end > self.data.len() {
            None
        } else {
            Some(IndexEntry::from_bytes(&self.data[start..end]))
        }
    }
}

impl AsRef<Map<Mmap>> for Segment {
    fn as_ref(&self) -> &Map<Mmap> {
        &self.map
    }
}

/// A `SegmentedIndex` contains the (on-disk) segments
/// that are committed with index data.
pub(crate) struct SegmentedIndex {
    segments: Vec<Arc<Segment>>,
    dir: PathBuf,
    _lockfile: Lockfile,
}

impl SegmentedIndex {
    /// Open an on-disk index, locking the target directory and reading all
    /// segment files found in it.
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<(Self, Option<u64>), SegmentedIndexError> {
        std::fs::create_dir_all(&dir)?;
        let lockfile = Lockfile::create(&dir.as_ref().join(LOCK_FILE))
            .map_err(SegmentedIndexError::LockfileError)?;
        let op_file = dir.as_ref().join(LAST_OP_FILE);
        let last_op = if let Ok(contents) = std::fs::read_to_string(op_file) {
            u64::from_str(&contents).ok()
        } else {
            None
        };
        let entries = std::fs::read_dir(&dir)?;

        let mut result = Self {
            segments: Vec::new(),
            dir: dir.as_ref().to_path_buf(),
            _lockfile: lockfile,
        };

        for entry in entries.flatten() {
            if entry
                .path()
                .extension()
                .is_some_and(|ext| ext == SEGMENT_EXT)
            {
                result.load(entry.path())?;
            }
        }

        Ok((result, last_op))
    }

    pub fn load<P: AsRef<Path>>(&mut self, path: P) -> Result<(), SegmentedIndexError> {
        let segment = Segment::load(path.as_ref().to_path_buf())?;

        Ok(self.segments.push(Arc::new(segment)))
    }

    pub fn snapshot(&self) -> Vec<Arc<Segment>> {
        self.segments.clone()
    }

    pub fn save_last_op(&self, seq: u64) -> Result<(), SegmentedIndexError> {
        let op_file = self.dir.join(LAST_OP_FILE);
        std::fs::write(op_file, seq.to_string()).map_err(SegmentedIndexError::Io)?;
        Ok(())
    }

    pub fn segments(&self) -> impl Iterator<Item = &Arc<Segment>> {
        self.segments.iter()
    }

    pub fn write_segment<I>(&self, segment_path: &PathBuf, it: I) -> Result<(), SegmentedIndexError>
    where
        I: Iterator<Item = (String, IndexEntry)>,
    {
        let seg_path = segment_path.with_extension(SEGMENT_EXT);
        let data_path = segment_path.with_extension(DATA_EXT);

        let seg_file = File::create_new(seg_path).map_err(SegmentedIndexError::Io)?;
        let mut seg_writer = BufWriter::new(seg_file);

        let dat_file = File::create(&data_path).map_err(SegmentedIndexError::Io)?;
        let mut dat_writer = BufWriter::new(dat_file);

        let mut builder = MapBuilder::new(&mut seg_writer).map_err(SegmentedIndexError::Fst)?;

        let mut current_offset = 0u64;
        for (path, entry) in it {
            let bytes = entry.to_bytes();
            dat_writer.write_all(&bytes)?;
            builder
                .insert(path, current_offset)
                .map_err(SegmentedIndexError::Fst)?;
            current_offset += bytes.len() as u64;
        }

        dat_writer
            .into_inner()
            .map_err(|e| SegmentedIndexError::Io(e.into_error()))?
            .sync_all()
            .map_err(SegmentedIndexError::Io)?;

        builder.finish().map_err(SegmentedIndexError::Fst)?;
        seg_writer
            .into_inner()
            .map_err(|e| SegmentedIndexError::Io(e.into_error()))?
            .sync_all()
            .map_err(SegmentedIndexError::Io)?;

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum SegmentedIndexError {
    #[error(
        "failed to create lockfile, this typically means there is another instance of an index running in the same directory"
    )]
    LockfileError(lockfile::Error),
    #[error(transparent)]
    Io(std::io::Error),
    #[error(transparent)]
    Fst(fst::Error),
}

impl From<std::io::Error> for SegmentedIndexError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
