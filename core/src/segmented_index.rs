use std::io::BufWriter;
use std::str::FromStr;

use crate::{opstamp::Opstamp, Path, PathBuf};
use fst::{Map, MapBuilder};
use lockfile::Lockfile;
use memmap2::Mmap;
use thiserror::Error;

const LAST_OP_FILE: &str = "last_op";
const LOCK_FILE: &str = ".minidex.lock";

pub(crate) struct Segment(Map<Mmap>);

impl std::ops::Deref for Segment {
    type Target = Map<Mmap>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A `SegmentedIndex` contains the (on-disk) segments
/// that are committed with index data.
pub(crate) struct SegmentedIndex {
    segments: Vec<Segment>,
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
            _lockfile: lockfile,
        };

        for entry in entries.flatten() {
            if entry.path().extension().is_some_and(|ext| ext == "seg") {
                result.load(entry.path())?;
            }
        }

        Ok((result, last_op))
    }

    pub fn load<P: AsRef<Path>>(&mut self, path: P) -> Result<(), SegmentedIndexError> {
        let entry_file = std::fs::File::open(path).map_err(SegmentedIndexError::Io)?;
        let mmap = unsafe { Mmap::map(&entry_file).map_err(SegmentedIndexError::Io)? };
        if let Ok(map) = Map::new(mmap) {
            self.segments.push(Segment(map));
        }

        Ok(())
    }

    pub fn segments(&self) -> impl Iterator<Item = &Segment> {
        self.segments.iter()
    }

    pub fn append_segment<I>(
        &self,
        segment_path: &PathBuf,
        it: I,
    ) -> Result<(), SegmentedIndexError>
    where
        I: Iterator<Item = (String, Opstamp)>,
    {
        let file = std::fs::File::create_new(segment_path).map_err(SegmentedIndexError::Io)?;
        let mut writer = BufWriter::new(file);
        let mut builder = MapBuilder::new(&mut writer).map_err(SegmentedIndexError::Fst)?;
        for (k, v) in it {
            builder.insert(k, *v).map_err(SegmentedIndexError::Fst)?;
        }
        builder.finish().map_err(SegmentedIndexError::Fst)?;
        writer
            .into_inner()
            .map_err(|e| SegmentedIndexError::Io(e.into_error()))?
            .sync_all()
            .map_err(SegmentedIndexError::Io)?;

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum SegmentedIndexError {
    #[error("failed to create lockfile, this typically means there is another instance of an index running in the same directory")]
    LockfileError(lockfile::Error),
    #[error(transparent)]
    Io(std::io::Error),
    #[error(transparent)]
    Fst(fst::Error),
}
