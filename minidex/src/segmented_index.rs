use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
};

use crate::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use crate::{Kind, Path, PathBuf, entry::IndexEntry, leb128::DeltaLeb128Iterator};
use fs4::fs_std::FileExt;
use fst::Map;
use memmap2::Mmap;
use thiserror::Error;

pub(crate) mod compactor;

pub(crate) type DocumentId = u32;

const LOCK_FILE: &str = ".minidex.lock";

/// FTS mapping tokens to posting offsets
const SEGMENT_EXT: &str = "seg";
/// Data - raw string paths, volume information and index entryies
const DATA_EXT: &str = "dat";
/// Posting (arrays of u32 Document IDs) files
const POST_EXT: &str = "post";
/// Flat array of 16-byte u128 integers containing document IDs
const META_EXT: &str = "meta";

/// A live index segment
pub(crate) struct Segment {
    map: Option<Map<Mmap>>,
    data: Option<Mmap>,
    post: Option<Mmap>,
    meta: Option<Mmap>,
    path: PathBuf,
    deleted: AtomicBool,
}

impl Segment {
    /// Load a segment (segment, data and postings) from disk into memory
    pub fn load(path: PathBuf) -> Result<Self, SegmentedIndexError> {
        let (seg_path, dat_path, post_path, meta_path) = Self::to_paths(&path);

        let seg_file = File::open(&seg_path).map_err(SegmentedIndexError::Io)?;
        let seg = unsafe { Mmap::map(&seg_file).map_err(SegmentedIndexError::Io)? };
        #[cfg(unix)]
        let _ = seg.advise(memmap2::Advice::WillNeed);

        let map = Map::new(seg).map_err(SegmentedIndexError::Fst)?;

        // Load the data file for the same segment
        let dat_file = File::open(dat_path).map_err(SegmentedIndexError::Io)?;
        let data = unsafe { Mmap::map(&dat_file).map_err(SegmentedIndexError::Io)? };

        // Load the postings
        let post_file = File::open(post_path).map_err(SegmentedIndexError::Io)?;
        let post = unsafe { Mmap::map(&post_file).map_err(SegmentedIndexError::Io)? };

        // Load the meta
        let meta_file = File::open(meta_path).map_err(SegmentedIndexError::Io)?;
        let meta = unsafe { Mmap::map(&meta_file).map_err(SegmentedIndexError::Io)? };

        Ok(Self {
            map: Some(map),
            data: Some(data),
            post: Some(post),
            meta: Some(meta),
            path,
            deleted: AtomicBool::new(false),
        })
    }

    pub(crate) fn mark_deleted(&self) {
        self.deleted.store(true, Ordering::SeqCst);
    }

    pub(crate) fn to_paths(path: &Path) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
        (
            path.with_extension(SEGMENT_EXT),
            path.with_extension(DATA_EXT),
            path.with_extension(POST_EXT),
            path.with_extension(META_EXT),
        )
    }

    pub(crate) fn paths_with_additional_extension(
        path: &Path,
    ) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
        (
            path.with_added_extension(SEGMENT_EXT),
            path.with_added_extension(DATA_EXT),
            path.with_added_extension(POST_EXT),
            path.with_added_extension(META_EXT),
        )
    }

    /// Helper to append a posting list directly to an existing Vec
    pub(crate) fn append_posting_list(&self, offset: u64, out: &mut Vec<u32>) {
        let start = offset as usize;
        let post = self.post.as_ref().expect("posting should be loaded");

        if start + size_of::<u32>() > post.len() {
            return;
        }

        let count =
            u32::from_le_bytes(post[start..start + size_of::<u32>()].try_into().unwrap()) as usize;

        let byte_len = u32::from_le_bytes(
            post[start + size_of::<u32>()..start + (2 * size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize;

        let cursor = start + (2 * size_of::<u32>());
        let end = cursor + byte_len;

        if end > post.len() {
            return;
        }

        out.reserve(count);

        let compressed_slice = &post[cursor..end];
        let iter = DeltaLeb128Iterator::new(compressed_slice);

        out.extend(iter);
    }

    /// Iterator over the documents in this segment
    pub(crate) fn documents(&self) -> DocumentIterator<'_> {
        DocumentIterator::new(self.data.as_ref().expect("Expected data to be loaded"))
    }

    /// Reads document data for the given offset.
    pub(crate) fn read_document(&self, offset: u64) -> Option<(&str, &str, IndexEntry)> {
        let cursor = offset as usize;
        let data = self.data.as_ref().expect("expected data to be loaded");
        Self::parse_document_borrowed(data, cursor)
            .map(|(path, volume, entry, _)| (path, volume, entry))
    }

    pub(crate) fn meta_map(&self) -> &Mmap {
        self.meta.as_ref().expect("meta should be loaded")
    }

    pub(crate) fn remove_files(paths: &(PathBuf, PathBuf, PathBuf, PathBuf)) {
        let _ = std::fs::remove_file(&paths.0);
        let _ = std::fs::remove_file(&paths.1);
        let _ = std::fs::remove_file(&paths.2);
        let _ = std::fs::remove_file(&paths.3);
    }

    pub(crate) fn rename_files(
        src: &(PathBuf, PathBuf, PathBuf, PathBuf),
        dst: &(PathBuf, PathBuf, PathBuf, PathBuf),
    ) -> std::io::Result<()> {
        std::fs::rename(&src.0, &dst.0)?;
        std::fs::rename(&src.1, &dst.1)?;
        std::fs::rename(&src.2, &dst.2)?;
        std::fs::rename(&src.3, &dst.3)?;
        Ok(())
    }

    fn parse_document_borrowed(
        data: &[u8],
        mut cursor: usize,
    ) -> Option<(&str, &str, IndexEntry, usize)> {
        let data_len = data.len();

        if cursor + size_of::<u32>() > data_len {
            return None;
        }
        let path_len =
            u32::from_le_bytes(data[cursor..cursor + size_of::<u32>()].try_into().unwrap())
                as usize;
        cursor += size_of::<u32>();

        if cursor + path_len > data_len {
            return None;
        }
        let path_str = std::str::from_utf8(&data[cursor..cursor + path_len]).ok()?;
        cursor += path_len;

        if cursor + size_of::<u32>() > data_len {
            return None;
        }
        let volume_len =
            u32::from_le_bytes(data[cursor..cursor + size_of::<u32>()].try_into().unwrap())
                as usize;
        cursor += size_of::<u32>();

        if cursor + volume_len > data_len {
            return None;
        }
        let volume_str = std::str::from_utf8(&data[cursor..cursor + volume_len]).ok()?;

        cursor += volume_len;

        if cursor + IndexEntry::SIZE > data_len {
            return None;
        }
        let entry = IndexEntry::from_bytes(&data[cursor..cursor + IndexEntry::SIZE]);
        cursor += IndexEntry::SIZE;

        Some((path_str, volume_str, entry, cursor))
    }

    fn parse_document_owned(
        data: &[u8],
        cursor: usize,
    ) -> Option<(String, String, IndexEntry, usize)> {
        Self::parse_document_borrowed(data, cursor).map(|(path, volume, entry, cursor)| {
            (path.to_owned(), volume.to_owned(), entry, cursor)
        })
    }
}

impl AsRef<Map<Mmap>> for Segment {
    fn as_ref(&self) -> &Map<Mmap> {
        self.map.as_ref().unwrap()
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        if self.deleted.load(Ordering::SeqCst) {
            self.map.take();
            self.data.take();
            self.post.take();
            self.meta.take();

            let paths = Self::to_paths(&self.path);

            Self::remove_files(&paths);
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

                    // Derive the base tmp path (e.g. "7.tmp") from the seg
                    // file (e.g. "7.tmp.seg") and clean up all sibling files.
                    let base_tmp_path = path.with_extension(""); // strip .seg → "7.tmp"
                    let paths = Segment::paths_with_additional_extension(&base_tmp_path);
                    Segment::remove_files(&paths);

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

    /// Add segment to the index
    pub(crate) fn add_segment(&mut self, segment: Arc<Segment>) {
        self.segments.push(segment);
    }

    /// Atomically swaps out old segments for a newly compacted segment,
    /// and cleans up the old files from disk.
    /// Returns `true` if the compaction covered every segment (i.e. the
    /// resulting index contains only the new segment).
    pub(crate) fn apply_compaction(
        &mut self,
        old_segments: &[Arc<Segment>],
        new_segment: Arc<Segment>,
    ) -> bool {
        self.segments
            .retain(|active_seg| !old_segments.iter().any(|old| Arc::ptr_eq(active_seg, old)));

        let was_full = self.segments.is_empty();

        self.segments.push(new_segment);

        for old_seg in old_segments {
            old_seg.mark_deleted();
        }

        was_full
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
        let (seg_path, dat_path, post_path, meta_path) =
            Segment::paths_with_additional_extension(out_path);

        let capacity = 8 * 1024 * 1024;
        let mut dat_writer = BufWriter::with_capacity(capacity, File::create(&dat_path)?);
        let mut post_writer = BufWriter::with_capacity(capacity, File::create(&post_path)?);
        let mut seg_writer = BufWriter::with_capacity(capacity, File::create(&seg_path)?);
        let mut meta_writer = BufWriter::new(File::create(&meta_path)?);

        let mut inverted_index: BTreeMap<String, Vec<DocumentId>> = BTreeMap::new();
        let mut current_dat_offset = 0u64;

        let mut doc_id_counter: u32 = 0;

        for (path, volume, entry) in items {
            if drop_deletions && entry.opstamp.is_deletion() {
                continue; // Always drop deletions before they hit the disk segment!
            }

            let path_ref = path.as_ref();
            let path_bytes = path_ref.as_bytes();
            let volume_ref = volume.as_ref();
            let volume_bytes = volume_ref.as_bytes();

            let entry_bytes = entry.as_bytes();

            dat_writer.write_all(&(path_bytes.len() as u32).to_le_bytes())?;
            dat_writer.write_all(path_bytes)?;
            dat_writer.write_all(&(volume_bytes.len() as u32).to_le_bytes())?;
            dat_writer.write_all(volume_bytes)?;
            dat_writer.write_all(&entry_bytes)?;

            // Pack u128 metadata
            let depth = path_bytes
                .iter()
                .filter(|&&b| b == std::path::MAIN_SEPARATOR as u8)
                .count() as u16;
            let is_dir = entry.kind == Kind::Directory;

            let packed_meta = Self::pack_u128(
                current_dat_offset,
                entry.last_modified / 1_000_000,
                entry.last_accessed / 1_000_000,
                depth,
                is_dir,
                entry.category,
                entry.volume_type as u8,
            );

            meta_writer.write_all(&packed_meta.to_le_bytes())?;

            let tokens = crate::tokenizer::extract_all_tokens(path_ref, volume_ref);
            for token in tokens {
                inverted_index
                    .entry(token)
                    .or_default()
                    .push(doc_id_counter);
            }

            current_dat_offset += (size_of::<u32>()
                + path_bytes.len()
                + size_of::<u32>()
                + volume_bytes.len()
                + entry_bytes.len()) as u64;
            doc_id_counter += 1
        }

        dat_writer
            .into_inner()
            .map_err(|e| SegmentedIndexError::Io(e.into_error()))?
            .sync_all()?;

        let mut seg_builder =
            fst::MapBuilder::new(&mut seg_writer).map_err(SegmentedIndexError::Fst)?;

        let mut current_post_offset = 0u64;
        let mut compressed_buffer = Vec::new();

        for (token, doc_offsets) in inverted_index {
            compressed_buffer.clear();
            let mut last_id = 0u32;

            for &offset in &doc_offsets {
                let delta = offset - last_id;
                last_id = offset;
                let mut val = delta;

                loop {
                    let mut byte = (val & 0x7F) as u8;
                    val >>= 7;
                    if val != 0 {
                        byte |= 0x80;
                        compressed_buffer.push(byte);
                    } else {
                        compressed_buffer.push(byte);
                        break;
                    }
                }
            }

            post_writer.write_all(&(doc_offsets.len() as u32).to_le_bytes())?;
            post_writer.write_all(&(compressed_buffer.len() as u32).to_le_bytes())?;
            post_writer.write_all(&compressed_buffer)?;

            seg_builder
                .insert(token, current_post_offset)
                .map_err(SegmentedIndexError::Fst)?;

            current_post_offset += (2 * size_of::<u32>() as u64) + compressed_buffer.len() as u64;
        }

        meta_writer
            .into_inner()
            .map_err(|e| SegmentedIndexError::Io(e.into_error()))?
            .sync_all()?;
        post_writer
            .into_inner()
            .map_err(|e| SegmentedIndexError::Io(e.into_error()))?
            .sync_all()?;
        seg_builder.finish().map_err(SegmentedIndexError::Fst)?;
        seg_writer
            .into_inner()
            .map_err(|e| SegmentedIndexError::Io(e.into_error()))?
            .sync_all()?;

        Ok(doc_id_counter as u64)
    }

    // Bits 127-128: Reserved
    // Bits 125-126: Volume Type (2 bits)
    // Bits 117-124: File category (8 bits)
    // Bit 116: is_dir (1 bit)
    // Bits 108-115: Depth (8 bits)
    // Bits 74-107: Last Accessed Timestamp (Seconds) (34 bits)
    // Bits 40-73: Last Modified Timestamp (Seconds) (34 bits)
    // Bits 0-39: dat_offset

    pub fn pack_u128(
        dat_offset: u64,
        last_modified: u64,
        last_accessed: u64,
        depth: u16,
        is_dir: bool,
        category: u8,
        volume_type: u8,
    ) -> u128 {
        let mut packed = (dat_offset as u128) & 0x0000_00FF_FFFF_FFFF;
        packed |= ((last_modified as u128) & 0x3_FFFF_FFFF) << 40;
        packed |= ((last_accessed as u128) & 0x3_FFFF_FFFF) << 74;
        packed |= ((depth.min(255) as u128) & 0xFF) << 108;
        if is_dir {
            packed |= 1 << 116;
        }
        packed |= ((category as u128) & 0xFF) << 117;
        packed |= ((volume_type as u128) & 0b11) << 125;
        packed
    }

    pub fn unpack_u128(packed: u128) -> (u64, u64, u64, u16, bool, u8, u8) {
        let offset = (packed & 0x0000_00FF_FFFF_FFFF) as u64;
        let last_modified = ((packed >> 40) & 0x3_FFFF_FFFF) as u64; // In seconds
        let last_accessed = ((packed >> 74) & 0x3_FFFF_FFFF) as u64; // In seconds
        let depth = ((packed >> 108) & 0xFF) as u16;
        let is_dir = ((packed >> 116) & 1) == 1;
        let category = ((packed >> 117) & 0xFF) as u8;
        let volume_type = ((packed >> 125) & 0b11) as u8;
        (
            offset,
            last_modified,
            last_accessed,
            depth,
            is_dir,
            category,
            volume_type,
        )
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
        let (path, volume, entry, new_cursor) =
            Segment::parse_document_owned(self.data, self.cursor)?;
        self.cursor = new_cursor;

        Some((path, volume, entry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VolumeType;
    use crate::opstamp::Opstamp;

    #[test]
    fn test_pack_unpack_u128() {
        let original = (123456789, 456789, 789012, 10, true, 0xAB, 1);
        let packed = SegmentedIndex::pack_u128(
            original.0, original.1, original.2, original.3, original.4, original.5, original.6,
        );
        let unpacked = SegmentedIndex::unpack_u128(packed);
        assert_eq!(original, unpacked);
    }

    #[test]
    fn test_segment_build_and_load() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = std::env::temp_dir().join(format!("minidex_test_seg_{}", rand_id()));
        std::fs::create_dir_all(&temp_dir)?;
        let seg_path = temp_dir.join("0");

        let entries = vec![
            (
                "/foo/bar.txt".to_string(),
                "vol1".to_string(),
                IndexEntry {
                    opstamp: Opstamp::insertion(1),
                    kind: Kind::File,
                    last_modified: 100,
                    last_accessed: 100,
                    category: 1,
                    volume_type: VolumeType::Local,
                },
            ),
            (
                "/foo/baz".to_string(),
                "vol1".to_string(),
                IndexEntry {
                    opstamp: Opstamp::insertion(2),
                    kind: Kind::Directory,
                    last_modified: 200,
                    last_accessed: 200,
                    category: 2,
                    volume_type: VolumeType::Local,
                },
            ),
        ];

        SegmentedIndex::build_segment_files(&seg_path, entries.clone(), false)?;

        let segment = Segment::load(seg_path)?;

        // Check documents iterator
        let docs: Vec<_> = segment.documents().collect();
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].0, "/foo/bar.txt");
        assert_eq!(docs[1].0, "/foo/baz");
        assert_eq!(docs[0].2.opstamp.sequence(), 1);
        assert_eq!(docs[1].2.opstamp.sequence(), 2);

        // Check FST searches
        let map = segment.as_ref();
        let tokens = crate::tokenizer::tokenize("/foo/bar.txt");
        for token in tokens {
            let offset = map.get(&token).expect("Token should be in FST");
            let mut post = Vec::new();
            segment.append_posting_list(offset, &mut post);
            assert!(post.contains(&0)); // doc_id 0 is "/foo/bar.txt"
        }

        // Check meta
        let meta_map = segment.meta_map();
        assert_eq!(meta_map.len(), 2 * 16);
        let packed0 = u128::from_le_bytes(meta_map[0..16].try_into()?);
        let (_, _, _, _, is_dir, _, _) = SegmentedIndex::unpack_u128(packed0);
        assert!(!is_dir);

        let packed1 = u128::from_le_bytes(meta_map[16..32].try_into()?);
        let (_, _, _, _, is_dir, _, _) = SegmentedIndex::unpack_u128(packed1);
        assert!(is_dir);

        std::fs::remove_dir_all(temp_dir)?;
        Ok(())
    }

    fn rand_id() -> u64 {
        crate::sync::time::SystemTime::now()
            .duration_since(crate::sync::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}
