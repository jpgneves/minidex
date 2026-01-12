use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::Arc,
};

use fst::{MapBuilder, Streamer as _, map::OpBuilder};

use crate::{entry::IndexEntry, segmented_index::SegmentedIndexError};

use super::{DATA_EXT, SEGMENT_EXT, Segment};

pub struct CompactorConfig {
    pub min_merge_count: usize,
    max_size_ratio: f32,
    memory_threshold: usize,
    deletion_threshold: usize,
}

impl Default for CompactorConfig {
    fn default() -> Self {
        CompactorConfigBuilder::default().build()
    }
}

pub struct CompactorConfigBuilder {
    min_merge_count: usize,
    max_size_ratio: f32,
    memory_threshold: usize,
    deletion_threshold: usize,
}

impl Default for CompactorConfigBuilder {
    fn default() -> Self {
        Self {
            min_merge_count: 4,
            max_size_ratio: 1.5,
            memory_threshold: 100 * 1024 * 1024, // Default to 100MB usage
            deletion_threshold: 1000,            // Trigger compaction on 1000 deletes
        }
    }
}

impl CompactorConfigBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn min_merge_count(self, min_merge_count: usize) -> Self {
        Self {
            min_merge_count,
            ..self
        }
    }

    pub fn max_size_ratio(self, max_size_ratio: f32) -> Self {
        Self {
            max_size_ratio,
            ..self
        }
    }

    pub fn memory_threshold(self, memory_threshold: usize) -> Self {
        Self {
            memory_threshold,
            ..self
        }
    }

    pub fn deletion_threshold(self, deletion_threshold: usize) -> Self {
        Self {
            deletion_threshold,
            ..self
        }
    }

    pub fn build(self) -> CompactorConfig {
        CompactorConfig {
            min_merge_count: self.min_merge_count,
            max_size_ratio: self.max_size_ratio,
            memory_threshold: self.memory_threshold,
            deletion_threshold: self.deletion_threshold,
        }
    }
}

pub fn merge_segments(segments: &[Arc<Segment>], out: PathBuf) -> Result<u64, SegmentedIndexError> {
    let mut union_builder = OpBuilder::new();
    for seg in segments {
        union_builder.push(seg.map.stream());
    }

    let mut stream = union_builder.union();

    let seg_path = out.with_extension(SEGMENT_EXT);
    let dat_path = out.with_extension(DATA_EXT);

    let mut dat_writer = BufWriter::new(File::create(&dat_path)?);
    let mut seg_writer = BufWriter::new(File::create(&seg_path)?);
    let mut seg_builder = MapBuilder::new(&mut seg_writer).map_err(SegmentedIndexError::Fst)?;

    let mut current_offset = 0u64;
    let mut written = 0;

    while let Some((key, indexed_values)) = stream.next() {
        let mut highest_opstamp: Option<IndexEntry> = None;

        for iv in indexed_values {
            let segment = &segments[iv.index];
            let offset = iv.value;

            if let Some(entry) = segment.get_entry(offset) {
                if let Some(highest) = highest_opstamp {
                    let current_seq = highest.opstamp.sequence();
                    let new_seq = entry.opstamp.sequence();
                    if new_seq > current_seq {
                        highest_opstamp = Some(entry);
                    }
                } else {
                    highest_opstamp = Some(entry)
                }
            }
        }

        if let Some(highest) = highest_opstamp {
            if highest.opstamp.is_deletion() {
                // Skip if latest change in segment is a deletion
                continue;
            }

            let bytes = highest.to_bytes();
            dat_writer.write_all(&bytes)?;

            seg_builder
                .insert(key, current_offset)
                .map_err(SegmentedIndexError::Fst)?;

            current_offset += bytes.len() as u64;
            written += 1;
        }
    }

    dat_writer
        .into_inner()
        .map_err(|e| SegmentedIndexError::Io(e.into_error()))?
        .sync_all()
        .map_err(SegmentedIndexError::Io)?;
    seg_builder.finish().map_err(SegmentedIndexError::Fst)?;
    seg_writer
        .into_inner()
        .map_err(|e| SegmentedIndexError::Io(e.into_error()))?
        .sync_all()
        .map_err(SegmentedIndexError::Io)?;

    Ok(written)
}
