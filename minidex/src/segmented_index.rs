use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Write},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::{Path, PathBuf, entry::IndexEntry};
use fs4::fs_std::FileExt;
use fst::Map;
use memmap2::Mmap;
use thiserror::Error;

pub(crate) mod compactor;

const LOCK_FILE: &str = ".minidex.lock";

const SEGMENT_EXT: &str = "seg";
const DATA_EXT: &str = "dat";
const POST_EXT: &str = "post";

/// A live index segment
pub(crate) struct Segment {
    map: Option<Map<Vec<u8>>>,
    data: Option<Mmap>,
    post: Option<Mmap>,
    path: PathBuf,
    deleted: AtomicBool,
}

impl Segment {
    /// Load a segment (segment, data and postings) from disk into memory
    pub fn load(path: PathBuf) -> Result<Self, SegmentedIndexError> {
        let (seg_path, dat_path, post_path) = Self::to_paths(&path);

        let mut entry_file = File::open(&seg_path).map_err(SegmentedIndexError::Io)?;
        let mut buf = Vec::new();
        entry_file
            .read_to_end(&mut buf)
            .map_err(SegmentedIndexError::Io)?;

        let map = Map::new(buf).map_err(SegmentedIndexError::Fst)?;

        // Load the data file for the same segment
        let dat_file = File::open(dat_path).map_err(SegmentedIndexError::Io)?;
        let data = unsafe { Mmap::map(&dat_file).map_err(SegmentedIndexError::Io)? };

        // Load the postings
        let post_file = File::open(post_path).map_err(SegmentedIndexError::Io)?;
        let post = unsafe { Mmap::map(&post_file).map_err(SegmentedIndexError::Io)? };

        Ok(Self {
            map: Some(map),
            data: Some(data),
            post: Some(post),
            path,
            deleted: AtomicBool::new(false),
        })
    }

    pub(crate) fn mark_deleted(&self) {
        self.deleted.store(true, Ordering::SeqCst);
    }

    pub(crate) fn to_paths(path: &Path) -> (PathBuf, PathBuf, PathBuf) {
        (
            path.with_extension(SEGMENT_EXT),
            path.with_extension(DATA_EXT),
            path.with_extension(POST_EXT),
        )
    }

    pub(crate) fn paths_with_additional_extension(path: &Path) -> (PathBuf, PathBuf, PathBuf) {
        (
            path.with_added_extension(SEGMENT_EXT),
            path.with_added_extension(DATA_EXT),
            path.with_added_extension(POST_EXT),
        )
    }

    /// Helper to read posting list for given offset.
    pub(crate) fn read_posting_list(&self, offset: u64) -> Vec<u64> {
        let start = offset as usize;

        let post = self.post.as_ref().expect("posting should be loaded");

        if start + size_of::<u32>() > post.len() {
            return Vec::new();
        }

        let count =
            u32::from_le_bytes(post[start..start + size_of::<u32>()].try_into().unwrap()) as usize;

        let mut docs = Vec::with_capacity(count);
        let mut cursor = start + size_of::<u32>();
        let post_count = post.len();

        for _ in 0..count {
            if cursor + size_of::<u64>() > post_count {
                break;
            }
            docs.push(u64::from_le_bytes(
                post[cursor..cursor + size_of::<u64>()].try_into().unwrap(),
            ));
            cursor += size_of::<u64>();
        }

        docs
    }

    /// Iterator over the documents in this segment
    pub(crate) fn documents(&self) -> DocumentIterator<'_> {
        DocumentIterator::new(self.data.as_ref().expect("Expected data to be loaded"))
    }

    /// Find all document offsets whose path starts with `prefix.
    pub(crate) fn find_docs_by_prefix(&self, prefix: &str) -> Vec<u64> {
        let synth = crate::tokenizer::synthesize_path_token(&prefix.to_lowercase());
        todo!()
    }

    /// Reads document data for the given offset.
    pub(crate) fn read_document(&self, offset: u64) -> Option<(String, String, IndexEntry)> {
        let cursor = offset as usize;
        let data = self.data.as_ref().expect("expected data to be loaded");
        let data_len = data.len();

        if cursor + size_of::<u32>() > data_len {
            return None;
        }

        let path_len =
            u32::from_le_bytes(data[cursor..cursor + size_of::<u32>()].try_into().unwrap())
                as usize;
        let path_start = cursor + size_of::<u32>();

        if path_start + path_len > data_len {
            return None;
        }
        let path_str = std::str::from_utf8(&data[path_start..path_start + path_len])
            .ok()?
            .to_string();

        let volume_len =
            u32::from_le_bytes(data[cursor..cursor + size_of::<u32>()].try_into().unwrap())
                as usize;
        let volume_start = cursor + size_of::<u32>();

        if volume_start + volume_len > data_len {
            return None;
        }
        let volume_str = std::str::from_utf8(&data[volume_start..volume_start + volume_len])
            .ok()?
            .to_string();

        let entry_start = volume_start + volume_len;
        if entry_start + IndexEntry::SIZE > data_len {
            return None;
        }
        let entry = IndexEntry::from_bytes(&data[entry_start..entry_start + IndexEntry::SIZE]);

        Some((path_str, volume_str, entry))
    }
}

impl AsRef<Map<Vec<u8>>> for Segment {
    fn as_ref(&self) -> &Map<Vec<u8>> {
        self.map.as_ref().unwrap()
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        if self.deleted.load(Ordering::SeqCst) {
            self.map.take();
            self.data.take();

            let (seg_path, dat_path, post_path) = Self::to_paths(&self.path);

            let _ = std::fs::remove_file(seg_path);
            let _ = std::fs::remove_file(dat_path);
            let _ = std::fs::remove_file(post_path);
        }
    }
}

/// A `SegmentedIndex` contains the (on-disk) segments
/// that are committed with index data.
pub(crate) struct SegmentedIndex {
    segments: Vec<Arc<Segment>>,
    _lockfile: File,
}

impl SegmentedIndex {
    /// Open an on-disk index, locking the target directory and reading all
    /// segment files found in it.
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self, SegmentedIndexError> {
        std::fs::create_dir_all(&dir)?;
        let lock_path = dir.as_ref().join(LOCK_FILE);
        let lockfile = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&lock_path)
            .map_err(SegmentedIndexError::Io)?;

        lockfile
            .try_lock_exclusive()
            .map_err(SegmentedIndexError::LockfileError)?;

        let entries = std::fs::read_dir(&dir)?;

        let mut result = Self {
            segments: Vec::new(),
            _lockfile: lockfile,
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == SEGMENT_EXT) {
                let file_name = path.file_name().unwrap_or_default().to_string_lossy();
                if file_name.contains(".tmp") {
                    log::trace!("Cleaning up orphaned temporary file: {}", file_name);

                    // We can safely delete the .seg and its matching .dat file
                    let _ = std::fs::remove_file(&path);
                    let _ = std::fs::remove_file(path.with_extension(DATA_EXT));

                    continue; // Skip loading!
                }
                result.load(entry.path())?;
            }
        }

        Ok(result)
    }

    /// Load a segment into the index
    pub(crate) fn load<P: AsRef<Path>>(&mut self, path: P) -> Result<(), SegmentedIndexError> {
        let segment = Segment::load(path.as_ref().to_path_buf())?;

        self.segments.push(Arc::new(segment));
        Ok(())
    }

    /// Take a snapshop of all currently living segments
    pub(crate) fn snapshot(&self) -> Vec<Arc<Segment>> {
        self.segments.clone()
    }

    pub(crate) fn segments(&self) -> impl Iterator<Item = &Arc<Segment>> {
        self.segments.iter()
    }

    /// Write a segment to disk
    pub(crate) fn write_segment<I>(
        &self,
        segment_path: &Path,
        it: I,
    ) -> Result<(), SegmentedIndexError>
    where
        I: Iterator<Item = (String, String, IndexEntry)>,
    {
        Self::build_segment_files(segment_path, it, false)?;
        Ok(())
    }

    /// Atomically swaps out old segments for a newly compacted segment,
    /// and cleans up the old files from disk.
    pub(crate) fn apply_compaction(
        &mut self,
        old_segments: &[Arc<Segment>],
        tmp_path: PathBuf,
    ) -> Result<(), SegmentedIndexError> {
        let (tmp_seg, tmp_dat, tmp_post) = Segment::paths_with_additional_extension(&tmp_path);

        let final_path_str = tmp_path.to_string_lossy().replace(".tmp", "");
        let final_path = PathBuf::from(final_path_str);

        let (final_seg, final_dat, final_post) = Segment::to_paths(&final_path);

        std::fs::rename(tmp_seg, &final_seg).map_err(SegmentedIndexError::Io)?;

        std::fs::rename(tmp_dat, &final_dat).map_err(SegmentedIndexError::Io)?;

        std::fs::rename(tmp_post, &final_post).map_err(SegmentedIndexError::Io)?;

        let new_seg = Arc::new(Segment::load(final_path)?);

        self.segments
            .retain(|active_seg| !old_segments.iter().any(|old| Arc::ptr_eq(active_seg, old)));

        self.segments.push(new_seg);

        for old_seg in old_segments {
            old_seg.mark_deleted();
        }

        Ok(())
    }

    pub(crate) fn build_segment_files<I, S>(
        out_path: &Path,
        items: I,
        drop_deletions: bool,
    ) -> Result<u64, SegmentedIndexError>
    where
        I: IntoIterator<Item = (S, S, IndexEntry)>,
        S: AsRef<str>,
    {
        let seg_path = out_path.with_extension(SEGMENT_EXT);
        let dat_path = out_path.with_extension(DATA_EXT);
        let post_path = out_path.with_extension(POST_EXT);

        let capacity = 8 * 1024 * 1024;
        let mut dat_writer = BufWriter::with_capacity(capacity, File::create(&dat_path)?);
        let mut post_writer = BufWriter::with_capacity(capacity, File::create(&post_path)?);
        let mut seg_writer = BufWriter::with_capacity(capacity, File::create(&seg_path)?);

        let mut inverted_index: BTreeMap<String, Vec<u64>> = BTreeMap::new();
        let mut current_dat_offset = 0u64;
        let mut written = 0;

        for (path, volume, entry) in items {
            if drop_deletions && entry.opstamp.is_deletion() {
                continue; // Always drop deletions before they hit the disk segment!
            }

            let path_ref = path.as_ref();
            let path_bytes = path_ref.as_bytes();
            let volume_ref = volume.as_ref();
            let volume_bytes = volume_ref.as_bytes();

            let entry_bytes = entry.to_bytes();

            dat_writer.write_all(&(path_bytes.len() as u32).to_le_bytes())?;
            dat_writer.write_all(path_bytes)?;
            dat_writer.write_all(&(volume_bytes.len() as u32).to_le_bytes())?;
            dat_writer.write_all(volume_bytes)?;
            dat_writer.write_all(&entry_bytes)?;

            // Tokenize the path, generate synthetic tokens and add them too.

            let tokens = crate::tokenizer::tokenize(path_ref);
            for token in tokens {
                inverted_index
                    .entry(token)
                    .or_default()
                    .push(current_dat_offset);
            }

            let path_lower = path_ref.to_lowercase();
            for (i, _) in path_lower.match_indices(['/', '\\']) {
                if i > 0 {
                    let synth = crate::tokenizer::synthesize_path_token(&path_lower[..=i]);
                    inverted_index
                        .entry(synth)
                        .or_default()
                        .push(current_dat_offset);
                }
            }

            // Generate synthetic tokens for the volume data and insert them
            if !volume_ref.is_empty() {
                let synth = crate::tokenizer::synthesize_volume_token(&volume_ref.to_lowercase());
                inverted_index
                    .entry(synth)
                    .or_default()
                    .push(current_dat_offset);
            }

            current_dat_offset +=
                (size_of::<u32>() + path_bytes.len() + volume_bytes.len() + entry_bytes.len())
                    as u64;
            written += 1;
        }

        dat_writer
            .into_inner()
            .map_err(|e| SegmentedIndexError::Io(e.into_error()))?
            .sync_all()?;

        let mut seg_builder =
            fst::MapBuilder::new(&mut seg_writer).map_err(SegmentedIndexError::Fst)?;
        let mut current_post_offset = 0u64;

        for (token, doc_offsets) in inverted_index {
            post_writer.write_all(&(doc_offsets.len() as u32).to_le_bytes())?;
            for offset in &doc_offsets {
                post_writer.write_all(&offset.to_le_bytes())?;
            }

            seg_builder
                .insert(token, current_post_offset)
                .map_err(SegmentedIndexError::Fst)?;

            current_post_offset += (4 + doc_offsets.len() * 8) as u64;
        }

        post_writer
            .into_inner()
            .map_err(|e| SegmentedIndexError::Io(e.into_error()))?
            .sync_all()?;
        seg_builder.finish().map_err(SegmentedIndexError::Fst)?;
        seg_writer
            .into_inner()
            .map_err(|e| SegmentedIndexError::Io(e.into_error()))?
            .sync_all()?;

        Ok(written)
    }
}

#[derive(Debug, Error)]
pub enum SegmentedIndexError {
    #[error(
        "failed to create lockfile, this typically means there is another instance of an index running in the same directory"
    )]
    LockfileError(std::io::Error),
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

pub(crate) struct DocumentIterator<'a> {
    data: &'a [u8],
    cursor: usize,
}

impl<'a> DocumentIterator<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, cursor: 0 }
    }
}

impl Iterator for DocumentIterator<'_> {
    type Item = (String, String, IndexEntry);

    fn next(&mut self) -> Option<Self::Item> {
        let data = self.data;

        if self.cursor + size_of::<u32>() > data.len() {
            return None;
        }

        let path_len = u32::from_le_bytes(
            data[self.cursor..self.cursor + size_of::<u32>()]
                .try_into()
                .expect("failed to read path_len"),
        ) as usize;
        self.cursor += size_of::<u32>();

        if self.cursor + path_len > data.len() {
            return None;
        }

        let path = std::str::from_utf8(&data[self.cursor..self.cursor + path_len])
            .ok()?
            .to_string();
        self.cursor += path_len;

        if self.cursor + size_of::<u32>() > data.len() {
            return None;
        }

        let volume_len = u32::from_le_bytes(
            data[self.cursor..self.cursor + size_of::<u32>()]
                .try_into()
                .expect("failed to read path_len"),
        ) as usize;
        self.cursor += size_of::<u32>();

        if self.cursor + path_len > data.len() {
            return None;
        }

        let volume = std::str::from_utf8(&data[self.cursor..self.cursor + volume_len])
            .ok()?
            .to_string();
        self.cursor += volume_len;

        if self.cursor + IndexEntry::SIZE > data.len() {
            return None;
        }
        let entry = IndexEntry::from_bytes(&data[self.cursor..self.cursor + IndexEntry::SIZE]);
        self.cursor += IndexEntry::SIZE;

        Some((path, volume, entry))
    }
}
