#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
};

use crate::{
    Kind, Path, PathBuf,
    entry::IndexEntry,
    leb128::DeltaLeb128Iterator,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use fs4::fs_std::FileExt;
use fst::{Map, Streamer};
use memmap2::Mmap;
use thiserror::Error;

pub(crate) mod compactor;
mod utils;

const LOCK_FILE: &str = ".minidex.lock";

/// FTS mapping tokens to posting offsets
const SEGMENT_EXT: &str = "seg";
/// Data - raw string paths, volume information and index entryies
const DATA_EXT: &str = "dat";
/// Posting (arrays of u32 Document IDs) files
const POST_EXT: &str = "post";
/// Flat array of 16-byte u128 integers containing document IDs
const META_EXT: &str = "meta";

/// Data files magic number
const DATA_MAGIC: &[u8; 4] = b"zMDX";

/// Postings file magic number
const POSTINGS_HEADER_MAGIC: &[u8; 4] = b"mDXP";
/// Postings header length, which includes the tokenizer version
const POSTINGS_HEADER_LEN: u64 = 2 * size_of::<u32>() as u64;

type PendingEntry = (String, String, IndexEntry, Vec<u8>, Option<(usize, usize)>);

/// A live index segment
pub(crate) struct Segment {
    map: Option<Map<Mmap>>,
    data: Option<Mmap>,
    dict: Option<Vec<u8>>,
    post: Option<Mmap>,
    meta: Option<Mmap>,
    path: PathBuf,
    deleted: AtomicBool,
    tokenizer_version: Option<u32>,
}

impl Segment {
    /// Load a segment (segment, data and postings) from disk into memory
    pub fn load(path: PathBuf) -> Result<Self, SegmentedIndexError> {
        let (seg_path, dat_path, post_path, meta_path) = Self::to_paths(&path);

        let seg_file = File::open(&seg_path).map_err(SegmentedIndexError::Io)?;
        let seg = unsafe { Mmap::map(&seg_file).map_err(SegmentedIndexError::Io)? };
        utils::prefetch_memory(&seg);

        let map = Map::new(seg).map_err(SegmentedIndexError::Fst)?;

        // Load the data file for the same segment
        let dat_file = File::open(dat_path).map_err(SegmentedIndexError::Io)?;
        let data = unsafe { Mmap::map(&dat_file).map_err(SegmentedIndexError::Io)? };

        let mut dict = None;
        if data.len() >= DATA_MAGIC.len() && &data[0..DATA_MAGIC.len()] == DATA_MAGIC {
            let dict_len = u32::from_le_bytes(
                data[DATA_MAGIC.len()..DATA_MAGIC.len() + size_of::<u32>()]
                    .try_into()
                    .map_err(|_| {
                        SegmentedIndexError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Invalid dictionary length",
                        ))
                    })?,
            ) as usize;
            if DATA_MAGIC.len() + size_of::<u32>() + dict_len <= data.len() {
                dict = Some(
                    data[DATA_MAGIC.len() + size_of::<u32>()
                        ..DATA_MAGIC.len() + size_of::<u32>() + dict_len]
                        .to_vec(),
                );
            }
        }

        // Load the postings
        let post_file =
            Self::open_file_with_random_access(&post_path).map_err(SegmentedIndexError::Io)?;
        let post = unsafe { Mmap::map(&post_file).map_err(SegmentedIndexError::Io)? };
        #[cfg(unix)]
        post.advise(memmap2::Advice::Random)?;

        // Load the meta
        let meta_file =
            Self::open_file_with_random_access(&meta_path).map_err(SegmentedIndexError::Io)?;
        let meta = unsafe { Mmap::map(&meta_file).map_err(SegmentedIndexError::Io)? };
        #[cfg(unix)]
        {
            meta.advise(memmap2::Advice::WillNeed)?;
            meta.advise(memmap2::Advice::Random)?;
        }

        let tokenizer_version = Self::read_postings_header(&post);

        Ok(Self {
            map: Some(map),
            data: Some(data),
            dict,
            post: Some(post),
            meta: Some(meta),
            path,
            deleted: AtomicBool::new(false),
            tokenizer_version,
        })
    }

    fn read_postings_header(post: &[u8]) -> Option<u32> {
        let header = post.get(..POSTINGS_HEADER_LEN as usize)?;
        let (magic, version) = header.split_at(size_of::<u32>());
        (magic == POSTINGS_HEADER_MAGIC).then(|| {
            u32::from_le_bytes(
                version
                    .try_into()
                    .expect("failed to parse tokenizer version from header"),
            )
        })
    }

    pub(crate) fn has_current_tokens(&self) -> bool {
        self.tokenizer_version == Some(crate::tokenizer::TOKENIZER_VERSION)
    }

    fn open_file_with_random_access(path: &std::path::Path) -> std::io::Result<std::fs::File> {
        let mut options = OpenOptions::new();
        options.read(true);

        #[cfg(windows)]
        {
            use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_RANDOM_ACCESS;
            options.custom_flags(FILE_FLAG_RANDOM_ACCESS);
        }

        options.open(path)
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
        let mut cursor = 0;
        if let Some(data) = self.data.as_ref()
            && data.len() >= DATA_MAGIC.len()
            && &data[0..DATA_MAGIC.len()] == DATA_MAGIC
        {
            let dict_len = u32::from_le_bytes(
                data[DATA_MAGIC.len()..DATA_MAGIC.len() + size_of::<u32>()]
                    .try_into()
                    .unwrap_or([0; 4]),
            ) as usize;
            cursor = DATA_MAGIC.len() + size_of::<u32>() + dict_len;
        }
        DocumentIterator::new(self, cursor)
    }

    /// Reads document data for the given offset.
    pub(crate) fn read_document(&self, offset: u64) -> Option<(String, String, IndexEntry)> {
        let cursor = offset as usize;
        let data = self.data.as_ref().expect("expected data to be loaded");

        if let Some(dict) = &self.dict {
            if cursor + size_of::<u32>() > data.len() {
                return None;
            }
            let compressed_len =
                u32::from_le_bytes(data[cursor..cursor + size_of::<u32>()].try_into().unwrap())
                    as usize;
            if cursor + size_of::<u32>() + compressed_len > data.len() {
                return None;
            }
            let compressed_data =
                &data[cursor + size_of::<u32>()..cursor + size_of::<u32>() + compressed_len];

            // Fast block decompression
            let mut decompressed = vec![0u8; 8 * 1024];
            let size = zstd::bulk::Decompressor::with_dictionary(dict)
                .ok()?
                .decompress_to_buffer(compressed_data, &mut decompressed)
                .ok()?;
            decompressed.truncate(size);

            Self::parse_document_owned(&decompressed, 0).map(|(p, v, e, _)| (p, v, e))
        } else {
            Self::parse_document_owned(data, cursor).map(|(p, v, e, _)| (p, v, e))
        }
    }

    /// Stream document IDs into a closure.
    pub(crate) fn for_each_posting_id(&self, offset: u64, mut f: impl FnMut(u32)) {
        let start = offset as usize;
        let post = self.post.as_ref().expect("posting should be loaded");

        if start + size_of::<u32>() > post.len() {
            return;
        }

        let byte_len = u32::from_le_bytes(
            post[start + size_of::<u32>()..start + (2 * size_of::<u32>())]
                .try_into()
                .expect("invalid byte length"),
        ) as usize;

        let cursor = start + (2 * size_of::<u32>());
        let end = cursor + byte_len;

        if end > post.len() {
            return;
        }

        for doc_id in crate::leb128::DeltaLeb128Iterator::new(&post[cursor..end]) {
            f(doc_id);
        }
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
        std::fs::rename(&src.1, &dst.1)?;
        std::fs::rename(&src.2, &dst.2)?;
        std::fs::rename(&src.3, &dst.3)?;
        // Rename the segment file last, to guarantee that the
        // sibling files exist already in case of crash.
        // If this happens, the temporary file cleanup will ensure
        // we are in a consistent state.
        std::fs::rename(&src.0, &dst.0)?;
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
#[derive(Clone)]
pub struct SegmentedIndex {
    segments: Vec<Arc<Segment>>,
    _lockfile: Arc<File>,
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
            _lockfile: Arc::new(lockfile),
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

    pub fn build_segment_files<I, S>(
        out_path: &Path,
        items: I,
        drop_deletions: bool,
        existing_dict: Option<&[u8]>,
    ) -> Result<u64, SegmentedIndexError>
    where
        I: IntoIterator<Item = (S, S, IndexEntry)>,
        S: AsRef<str>,
    {
        let items = items
            .into_iter()
            .map(|(path, volume, entry)| (path, volume, entry, None));
        Self::build_segment_files_with(out_path, items, drop_deletions, existing_dict, None)
    }

    pub(crate) fn build_segment_files_with<I, S>(
        out_path: &Path,
        items: I,
        drop_deletions: bool,
        existing_dict: Option<&[u8]>,
        merge_sources: Option<&[Arc<Segment>]>,
    ) -> Result<u64, SegmentedIndexError>
    where
        I: IntoIterator<Item = (S, S, IndexEntry, Option<(usize, usize)>)>,
        S: AsRef<str>,
    {
        let tokenize = merge_sources.is_none();
        let mut remap: Vec<Vec<u32>> = merge_sources.map_or_else(Vec::new, |sources| {
            sources
                .iter()
                .map(|seg| vec![u32::MAX; seg.meta_map().len() / 16])
                .collect()
        });

        let (seg_path, dat_path, post_path, meta_path) =
            Segment::paths_with_additional_extension(out_path);

        let capacity = 8 * 1024 * 1024;
        let mut dat_writer = BufWriter::with_capacity(capacity, File::create(&dat_path)?);
        let mut post_writer = BufWriter::with_capacity(capacity, File::create(&post_path)?);
        let mut seg_writer = BufWriter::with_capacity(capacity, File::create(&seg_path)?);
        let mut meta_writer = BufWriter::new(File::create(&meta_path)?);

        let mut inverted_index: BTreeMap<String, (u32, u32, Vec<u8>)> = BTreeMap::new();

        const SAMPLE_WINDOW: usize = 100 * 1000;
        let mut pending: Vec<PendingEntry> = Vec::new();
        let mut samples = Vec::new();
        let mut sample_sizes = Vec::new();

        let mut current_dat_offset = 0u64;
        let mut doc_id_counter: u32 = 0;
        let mut compressor: Option<zstd::bulk::Compressor<'static>> = None;
        let mut dict_ready = false;

        let start = |dict: Vec<u8>,
                     dat_writer: &mut BufWriter<File>,
                     current_dat_offset: &mut u64,
                     compressor: &mut Option<zstd::bulk::Compressor<'static>>|
         -> Result<(), SegmentedIndexError> {
            dat_writer.write_all(DATA_MAGIC)?;
            dat_writer.write_all(&(dict.len() as u32).to_le_bytes())?;
            dat_writer.write_all(&dict)?;

            *current_dat_offset = (DATA_MAGIC.len() + size_of::<u32>() + dict.len()) as u64;
            *compressor = if !dict.is_empty() {
                Some(
                    zstd::bulk::Compressor::with_dictionary(0, &dict)
                        .map_err(|e| SegmentedIndexError::Io(std::io::Error::other(e)))?,
                )
            } else {
                None
            };
            Ok(())
        };

        let write_record = |path_ref: &str,
                            volume_ref: &str,
                            entry: &IndexEntry,
                            serialized: &[u8],
                            dat_writer: &mut BufWriter<File>,
                            meta_writer: &mut BufWriter<File>,
                            compressor: &mut Option<zstd::bulk::Compressor<'static>>,
                            inverted_index: &mut BTreeMap<String, (u32, u32, Vec<u8>)>,
                            current_dat_offset: &mut u64,
                            doc_id_counter: &mut u32,
                            source: Option<(usize, usize)>,
                            remap: &mut Vec<Vec<u32>>|
         -> Result<(), SegmentedIndexError> {
            let compressed = if let Some(comp) = compressor.as_mut() {
                let max_size = serialized.len() + (serialized.len() / 16) + 64;
                let mut out = vec![0u8; max_size];
                let size = comp
                    .compress_to_buffer(serialized, &mut out)
                    .map_err(|e| SegmentedIndexError::Io(std::io::Error::other(e)))?;
                out.truncate(size);
                out
            } else {
                zstd::encode_all(serialized, 0)
                    .map_err(|e| SegmentedIndexError::Io(std::io::Error::other(e)))?
            };

            dat_writer.write_all(&(compressed.len() as u32).to_le_bytes())?;
            dat_writer.write_all(&compressed)?;

            // Pack u128 metadata
            let depth = path_ref
                .as_bytes()
                .iter()
                .filter(|&&b| b == std::path::MAIN_SEPARATOR as u8)
                .count() as u16;
            let is_dir = entry.kind == Kind::Directory;

            let packed_meta = Self::pack_u128(
                *current_dat_offset,
                entry.last_modified / 1_000_000,
                entry.last_accessed / 1_000_000,
                depth,
                is_dir,
                entry.category,
                entry.volume_type as u8,
            );

            meta_writer.write_all(&packed_meta.to_le_bytes())?;

            let tokens = if tokenize {
                crate::tokenizer::extract_all_tokens(path_ref, volume_ref)
            } else {
                Vec::new()
            };

            for token in tokens {
                let (count, last, bytes) = inverted_index.entry(token).or_default();
                crate::leb128::push_leb128(bytes, *doc_id_counter - *last);
                *last = *doc_id_counter;
                *count += 1;
            }

            if let Some((segment, old_id)) = source
                && let Some(table) = remap.get_mut(segment)
            {
                table[old_id] = *doc_id_counter
            }

            *current_dat_offset += (size_of::<u32>() + compressed.len()) as u64;
            *doc_id_counter += 1;
            Ok(())
        };

        if let Some(d) = existing_dict {
            start(
                d.to_vec(),
                &mut dat_writer,
                &mut current_dat_offset,
                &mut compressor,
            )?;
            dict_ready = true;
        }

        for (loop_counter, (path, volume, entry, source)) in (0_usize..).zip(items) {
            if loop_counter.is_multiple_of(500) {
                crate::sync::thread::yield_now();
            }
            if drop_deletions && entry.opstamp.is_deletion() {
                continue; // Always drop deletions before they hit the disk segment!
            }

            let path_ref = path.as_ref();
            let path_bytes = path_ref.as_bytes();
            let volume_ref = volume.as_ref();
            let volume_bytes = volume_ref.as_bytes();

            let entry_bytes = entry.as_bytes();

            let mut serialized = Vec::with_capacity(
                size_of::<u32>()
                    + path_bytes.len()
                    + size_of::<u32>()
                    + volume_bytes.len()
                    + entry_bytes.len(),
            );
            serialized.extend_from_slice(&(path_bytes.len() as u32).to_le_bytes());
            serialized.extend_from_slice(path_bytes);
            serialized.extend_from_slice(&(volume_bytes.len() as u32).to_le_bytes());
            serialized.extend_from_slice(volume_bytes);
            serialized.extend_from_slice(&entry_bytes);

            // If the dictionary is ready we can skip sampling
            if dict_ready {
                write_record(
                    path_ref,
                    volume_ref,
                    &entry,
                    &serialized,
                    &mut dat_writer,
                    &mut meta_writer,
                    &mut compressor,
                    &mut inverted_index,
                    &mut current_dat_offset,
                    &mut doc_id_counter,
                    source,
                    &mut remap,
                )?;
                continue;
            }
            if pending.len().is_multiple_of(100) && sample_sizes.len() < 1000 {
                samples.extend_from_slice(&serialized);
                sample_sizes.push(serialized.len());
            }

            pending.push((
                path_ref.to_owned(),
                volume_ref.to_owned(),
                entry,
                serialized,
                source,
            ));

            if pending.len() == SAMPLE_WINDOW {
                let dict = zstd::dict::from_continuous(&samples, &sample_sizes, 40 * 1024)
                    .unwrap_or_default();
                start(
                    dict,
                    &mut dat_writer,
                    &mut current_dat_offset,
                    &mut compressor,
                )?;
                dict_ready = true;
                for (p, v, e, ser, src) in pending.drain(..) {
                    write_record(
                        &p,
                        &v,
                        &e,
                        &ser,
                        &mut dat_writer,
                        &mut meta_writer,
                        &mut compressor,
                        &mut inverted_index,
                        &mut current_dat_offset,
                        &mut doc_id_counter,
                        src,
                        &mut remap,
                    )?;
                }
            }
        }

        if !dict_ready {
            let dict = if !samples.is_empty() {
                zstd::dict::from_continuous(&samples, &sample_sizes, 40 * 1024).unwrap_or_default()
            } else {
                Vec::new()
            };
            start(
                dict,
                &mut dat_writer,
                &mut current_dat_offset,
                &mut compressor,
            )?;
            for (p, v, e, ser, src) in pending.drain(..) {
                write_record(
                    &p,
                    &v,
                    &e,
                    &ser,
                    &mut dat_writer,
                    &mut meta_writer,
                    &mut compressor,
                    &mut inverted_index,
                    &mut current_dat_offset,
                    &mut doc_id_counter,
                    src,
                    &mut remap,
                )?;
            }
        }

        dat_writer
            .into_inner()
            .map_err(|e| SegmentedIndexError::Io(e.into_error()))?
            .sync_all()?;

        let mut seg_builder =
            fst::MapBuilder::new(&mut seg_writer).map_err(SegmentedIndexError::Fst)?;

        post_writer.write_all(POSTINGS_HEADER_MAGIC)?;
        post_writer.write_all(&crate::tokenizer::TOKENIZER_VERSION.to_le_bytes())?;
        let mut current_post_offset = POSTINGS_HEADER_LEN;

        if let Some(sources) = merge_sources {
            let mut op = fst::map::OpBuilder::new();

            for seg in sources {
                op = op.add(seg.map.as_ref().expect("segment map loaded"));
            }
            // Tokens are yielded in byte order, same ordering as the BTreeMap
            // path writes
            let mut union = op.union();
            let mut ids: Vec<u32> = Vec::new();
            let mut bytes: Vec<u8> = Vec::new();
            let mut loop_counter: usize = 0;

            while let Some((token, hits)) = union.next() {
                loop_counter += 1;
                if loop_counter.is_multiple_of(1000) {
                    crate::sync::thread::yield_now();
                }
                ids.clear();
                for hit in hits {
                    let post = sources[hit.index].post.as_ref().expect("postings loaded");
                    let offset = hit.value as usize;
                    let len = post
                        .get(offset + size_of::<u32>()..offset + 2 * size_of::<u32>())
                        .ok_or_else(|| {
                            SegmentedIndexError::Io(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "postings out of bounds in a merge source",
                            ))
                        })?;
                    let len = u32::from_le_bytes(len.try_into().unwrap()) as usize;
                    let start = offset + 2 * size_of::<u32>();
                    let encoded = post.get(start..start + len).ok_or_else(|| {
                        SegmentedIndexError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "postings out of bounds in a merge source",
                        ))
                    })?;
                    let table = &remap[hit.index];

                    for old in DeltaLeb128Iterator::new(encoded) {
                        match table.get(old as usize) {
                            Some(&new) if new != u32::MAX => ids.push(new),
                            Some(_) => {}
                            None => {
                                return Err(SegmentedIndexError::Io(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "postings mapped to a missing posting",
                                )));
                            }
                        }
                    }
                }

                // Every document with this token was dropped by the merge
                if ids.is_empty() {
                    continue;
                }

                // K-way merge of sources via ascending source IDs
                ids.sort_unstable();
                bytes.clear();
                let mut last: u32 = 0;

                for &id in &ids {
                    crate::leb128::push_leb128(&mut bytes, id - last);
                    last = id;
                }
                post_writer.write_all(&(ids.len() as u32).to_le_bytes())?;
                post_writer.write_all(&(bytes.len() as u32).to_le_bytes())?;
                post_writer.write_all(&bytes)?;
                seg_builder
                    .insert(token, current_post_offset)
                    .map_err(SegmentedIndexError::Fst)?;
                current_post_offset += (2 * size_of::<u32>() as u64) + bytes.len() as u64;
            }
        } else {
            for (fst_loop_counter, (token, (count, _, compressed_buffer))) in
                (0_usize..).zip(inverted_index)
            {
                if fst_loop_counter.is_multiple_of(1000) {
                    crate::sync::thread::yield_now();
                }
                post_writer.write_all(&count.to_le_bytes())?;
                post_writer.write_all(&(compressed_buffer.len() as u32).to_le_bytes())?;
                post_writer.write_all(&compressed_buffer)?;

                seg_builder
                    .insert(token, current_post_offset)
                    .map_err(SegmentedIndexError::Fst)?;

                current_post_offset +=
                    (2 * size_of::<u32>() as u64) + compressed_buffer.len() as u64;
            }
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
    segment: &'a Segment,
    cursor: usize,
}

impl<'a> DocumentIterator<'a> {
    fn new(segment: &'a Segment, cursor: usize) -> Self {
        Self { segment, cursor }
    }
}

impl Iterator for DocumentIterator<'_> {
    type Item = (String, String, IndexEntry);

    fn next(&mut self) -> Option<Self::Item> {
        let data = self.segment.data.as_ref().expect("expected data");
        if let Some(dict) = &self.segment.dict {
            if self.cursor + size_of::<u32>() > data.len() {
                return None;
            }
            let compressed_len = u32::from_le_bytes(
                data[self.cursor..self.cursor + size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            ) as usize;
            self.cursor += size_of::<u32>();
            if self.cursor + compressed_len > data.len() {
                return None;
            }
            let compressed_data = &data[self.cursor..self.cursor + compressed_len];
            self.cursor += compressed_len;

            // Fast block decompression
            let mut decompressed = vec![0u8; 8 * 1024];
            let size = zstd::bulk::Decompressor::with_dictionary(dict)
                .ok()?
                .decompress_to_buffer(compressed_data, &mut decompressed)
                .ok()?;
            decompressed.truncate(size);

            Segment::parse_document_owned(&decompressed, 0).map(|(p, v, e, _)| (p, v, e))
        } else {
            let (path, volume, entry, new_cursor) =
                Segment::parse_document_owned(data, self.cursor)?;
            self.cursor = new_cursor;

            Some((path, volume, entry))
        }
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

        SegmentedIndex::build_segment_files(&seg_path, entries.clone(), false, None)?;

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

    fn entry(opstamp: Opstamp) -> IndexEntry {
        IndexEntry {
            opstamp,
            kind: Kind::File,
            last_modified: 100,
            last_accessed: 100,
            category: 0,
            volume_type: VolumeType::Local,
        }
    }

    fn doc(path: String, opstamp: Opstamp) -> (String, String, IndexEntry) {
        (path, "vol1".to_string(), entry(opstamp))
    }

    fn assert_same_segment_files(a: &std::path::Path, b: &std::path::Path) -> std::io::Result<()> {
        let a = Segment::to_paths(a);
        let b = Segment::to_paths(b);
        for (a, b) in [(a.0, b.0), (a.1, b.1), (a.2, b.2), (a.3, b.3)] {
            assert_eq!(
                std::fs::read(&a)?,
                std::fs::read(&b)?,
                "{} differs",
                a.display()
            );
        }
        Ok(())
    }

    #[test]
    fn test_tokenizer_version_is_read_from_the_postings_header() {
        let current = crate::tokenizer::TOKENIZER_VERSION;
        let stamped = [
            POSTINGS_HEADER_MAGIC.as_slice(),
            &current.to_le_bytes(),
            &[3, 0, 0, 0],
        ]
        .concat();
        assert_eq!(Segment::read_postings_header(&stamped), Some(current));
        // A segment from before the header starts with its first token's posting count.
        let legacy = [3u32.to_le_bytes(), 2u32.to_le_bytes()].concat();
        assert_eq!(Segment::read_postings_header(&legacy), None);
        assert_eq!(Segment::read_postings_header(&[]), None);
    }

    /// The merge remaps the sources' postings instead of re-tokenizing, so its output must match a segment built
    /// from scratch out of the surviving documents: newer duplicates win, deletions and tombstoned paths drop out.
    #[test]
    fn test_merge_postings_match_a_fresh_build() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = std::env::temp_dir().join(format!("minidex_test_merge_post_{}", rand_id()));
        std::fs::create_dir_all(&temp_dir)?;
        let sep = std::path::MAIN_SEPARATOR;

        let seg1_path = temp_dir.join("1");
        SegmentedIndex::build_segment_files(
            &seg1_path,
            vec![
                doc(format!("/bar{sep}e.txt"), Opstamp::insertion(1)),
                doc(format!("/baz{sep}x.txt"), Opstamp::insertion(1)),
                doc(format!("/foo{sep}a.txt"), Opstamp::insertion(1)),
                doc(format!("/foo{sep}b.txt"), Opstamp::insertion(1)),
            ],
            false,
            None,
        )?;
        let seg2_path = temp_dir.join("2");
        SegmentedIndex::build_segment_files(
            &seg2_path,
            vec![
                doc(format!("/foo{sep}a.txt"), Opstamp::insertion(2)),
                doc(format!("/foo{sep}b.txt"), Opstamp::deletion(2)),
                doc(format!("/foo{sep}d report.pdf"), Opstamp::insertion(2)),
            ],
            false,
            None,
        )?;
        let s1 = Arc::new(Segment::load(seg1_path)?);
        let s2 = Arc::new(Segment::load(seg2_path)?);
        assert!(s1.has_current_tokens() && s2.has_current_tokens());

        let merged_path = temp_dir.join("merged");
        let tombstones = vec![(Some("vol1".to_string()), "/baz".to_string(), 50)];
        compactor::merge_segments(&[s1.clone(), s2], Arc::new(tombstones), merged_path.clone())?;

        let fresh_path = temp_dir.join("fresh");
        SegmentedIndex::build_segment_files(
            &fresh_path,
            vec![
                doc(format!("/bar{sep}e.txt"), Opstamp::insertion(1)),
                doc(format!("/foo{sep}a.txt"), Opstamp::insertion(2)),
                doc(format!("/foo{sep}d report.pdf"), Opstamp::insertion(2)),
            ],
            true,
            s1.dict.as_deref(),
        )?;
        assert_same_segment_files(&merged_path, &fresh_path)?;

        std::fs::remove_dir_all(temp_dir)?;
        Ok(())
    }

    /// A source stamped with another tokenizer version must be re-tokenized, never remapped. Its postings are zeroed
    /// here, so reusing them would lose documents from the merged postings.
    #[test]
    fn test_merge_retokenizes_sources_of_another_tokenizer_version()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = std::env::temp_dir().join(format!("minidex_test_merge_stamp_{}", rand_id()));
        std::fs::create_dir_all(&temp_dir)?;
        let sep = std::path::MAIN_SEPARATOR;

        let seg1_path = temp_dir.join("1");
        SegmentedIndex::build_segment_files(
            &seg1_path,
            vec![doc(format!("/bar{sep}e.txt"), Opstamp::insertion(1))],
            false,
            None,
        )?;
        let seg2_path = temp_dir.join("2");
        SegmentedIndex::build_segment_files(
            &seg2_path,
            vec![doc(
                format!("/foo{sep}old tokens.txt"),
                Opstamp::insertion(2),
            )],
            false,
            None,
        )?;
        let post_path = Segment::to_paths(&seg2_path).2;
        let mut post = std::fs::read(&post_path)?;
        let header = POSTINGS_HEADER_LEN as usize;
        post[size_of::<u32>()..header]
            .copy_from_slice(&(crate::tokenizer::TOKENIZER_VERSION - 1).to_le_bytes());
        post[header..].fill(0);
        std::fs::write(&post_path, post)?;

        let s1 = Arc::new(Segment::load(seg1_path)?);
        let s2 = Arc::new(Segment::load(seg2_path)?);
        assert!(s1.has_current_tokens());
        assert!(!s2.has_current_tokens());

        let merged_path = temp_dir.join("merged");
        compactor::merge_segments(&[s1.clone(), s2], Arc::new(vec![]), merged_path.clone())?;

        let fresh_path = temp_dir.join("fresh");
        SegmentedIndex::build_segment_files(
            &fresh_path,
            vec![
                doc(format!("/bar{sep}e.txt"), Opstamp::insertion(1)),
                doc(format!("/foo{sep}old tokens.txt"), Opstamp::insertion(2)),
            ],
            true,
            s1.dict.as_deref(),
        )?;
        assert_same_segment_files(&merged_path, &fresh_path)?;

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
