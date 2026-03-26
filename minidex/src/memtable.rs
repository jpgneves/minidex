use std::collections::{BTreeMap, HashMap};

use crate::{Kind, entry::IndexEntry, segmented_index::SegmentedIndex};

/// The in-memory data structures, containing
/// an inverted index for fast lookups
#[derive(Default)]
pub(crate) struct MemTable {
    next_id: u32,

    pub id_to_data: HashMap<u32, (String, String, IndexEntry)>,
    pub path_to_id: HashMap<String, u32>,

    // B-Tree inverted index: Token -> [Path ID]
    pub inverted_index: BTreeMap<String, Vec<u32>>,

    // FST staging area (sorted for flusher to work)
    pub entries: BTreeMap<String, (String, IndexEntry)>,

    // Mapping to u128 metadata for pre-filtering
    pub metadata: Vec<u128>,
}

impl MemTable {
    pub fn insert(&mut self, path: String, volume: String, entry: IndexEntry) {
        if let Some(id) = self.path_to_id.get(&path) {
            self.id_to_data
                .insert(*id, (path.clone(), volume.clone(), entry));
            self.entries.insert(path, (volume, entry));
            return;
        }

        let id = self.next_id;
        self.next_id += 1;

        self.path_to_id.insert(path.clone(), id);
        self.id_to_data
            .insert(id, (path.clone(), volume.clone(), entry));
        let depth = path
            .bytes()
            .filter(|&b| b == std::path::MAIN_SEPARATOR as u8)
            .count() as u16;
        let meta = SegmentedIndex::pack_u128(
            id as u64, // This is okay, because it's only used for tie-breaking
            entry.last_modified,
            entry.last_accessed,
            depth,
            entry.kind == Kind::Directory,
            entry.category,
            entry.volume_type as u8,
        );

        self.metadata.push(meta);

        let tokens = crate::tokenizer::tokenize(&path);
        for token in tokens {
            self.inverted_index.entry(token).or_default().push(id);
        }

        self.entries.insert(path, (volume, entry));
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}
