use std::collections::HashMap;

use crate::{common::is_tombstoned, entry::IndexEntry};

pub(crate) struct LsmCollector<'a> {
    candidates: HashMap<String, (String, IndexEntry)>,
    active_tombstones: &'a [(String, u64)],
}

impl<'a> LsmCollector<'a> {
    pub(crate) fn new(active_tombstones: &'a [(String, u64)]) -> Self {
        Self {
            candidates: HashMap::new(),
            active_tombstones,
        }
    }

    #[inline]
    pub(crate) fn insert(&mut self, path: String, volume: String, entry: IndexEntry) {
        if is_tombstoned(
            path.as_bytes(),
            entry.opstamp.sequence(),
            self.active_tombstones,
        ) {
            return;
        }

        self.candidates
            .entry(path)
            .and_modify(|(current_volume, current_entry)| {
                if entry.opstamp.sequence() > current_entry.opstamp.sequence() {
                    *current_entry = entry;
                    *current_volume = volume.clone();
                }
            })
            .or_insert((volume, entry));
    }

    #[inline]
    pub(crate) fn finish(self) -> impl Iterator<Item = (String, String, IndexEntry)> {
        self.candidates
            .into_iter()
            .filter(|(_, (_, entry))| !entry.opstamp.is_deletion())
            .map(|(path, (volume, entry))| (path, volume, entry))
    }
}
