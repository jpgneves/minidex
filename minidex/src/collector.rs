use std::collections::HashMap;

use crate::{common::is_tombstoned, entry::IndexEntry};

pub(crate) struct LsmCollector<'a> {
    candidates: HashMap<String, (String, IndexEntry)>,
    active_tombstones: &'a [(Option<String>, String, u64)],
}

impl<'a> LsmCollector<'a> {
    pub(crate) fn new(active_tombstones: &'a [(Option<String>, String, u64)]) -> Self {
        Self {
            candidates: HashMap::new(),
            active_tombstones,
        }
    }

    #[inline]
    pub(crate) fn insert(&mut self, path: String, volume: String, entry: IndexEntry) {
        if is_tombstoned(
            &volume,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Kind;
    use crate::common::VolumeType;
    use crate::opstamp::Opstamp;

    #[test]
    fn test_collector_basic_insertion() {
        let mut collector = LsmCollector::new(&[]);
        let entry = IndexEntry {
            opstamp: Opstamp::insertion(10),
            kind: Kind::File,
            last_modified: 100,
            last_accessed: 100,
            category: 0,
            volume_type: VolumeType::Local,
        };
        collector.insert("/a".to_string(), "vol1".to_string(), entry);

        let results: Vec<_> = collector.finish().collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "/a");
        assert_eq!(results[0].1, "vol1");
        assert_eq!(results[0].2.opstamp.sequence(), 10);
    }

    #[test]
    fn test_collector_version_resolution() {
        let mut collector = LsmCollector::new(&[]);
        let entry1 = IndexEntry {
            opstamp: Opstamp::insertion(10),
            kind: Kind::File,
            last_modified: 100,
            last_accessed: 100,
            category: 0,
            volume_type: VolumeType::Local,
        };
        let entry2 = IndexEntry {
            opstamp: Opstamp::insertion(20),
            kind: Kind::File,
            last_modified: 200,
            last_accessed: 200,
            category: 0,
            volume_type: VolumeType::Local,
        };

        // Out-of-order insertion
        collector.insert("/a".to_string(), "vol1".to_string(), entry2);
        collector.insert("/a".to_string(), "vol1".to_string(), entry1);

        let results: Vec<_> = collector.finish().collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].2.opstamp.sequence(), 20); // Version 20 should win
    }

    #[test]
    fn test_collector_prefix_tombstone() {
        let tombstones = vec![(None, "/foo".to_string(), 50)];
        let mut collector = LsmCollector::new(&tombstones);

        let entry_dead = IndexEntry {
            opstamp: Opstamp::insertion(10),
            kind: Kind::File,
            last_modified: 100,
            last_accessed: 100,
            category: 0,
            volume_type: VolumeType::Local,
        };
        let entry_alive = IndexEntry {
            opstamp: Opstamp::insertion(100),
            kind: Kind::File,
            last_modified: 100,
            last_accessed: 100,
            category: 0,
            volume_type: VolumeType::Local,
        };

        collector.insert("/foo/bar".to_string(), "vol1".to_string(), entry_dead);
        collector.insert("/foo/baz".to_string(), "vol1".to_string(), entry_alive);
        collector.insert("/other".to_string(), "vol1".to_string(), entry_dead);

        let results: Vec<_> = collector.finish().collect();
        assert_eq!(results.len(), 2);
        let mut paths: Vec<_> = results.iter().map(|(p, _, _)| p.as_str()).collect();
        paths.sort();
        assert_eq!(paths, vec!["/foo/baz", "/other"]);
    }

    #[test]
    fn test_collector_deletion_resolution() {
        let mut collector = LsmCollector::new(&[]);
        let entry1 = IndexEntry {
            opstamp: Opstamp::insertion(10),
            kind: Kind::File,
            last_modified: 100,
            last_accessed: 100,
            category: 0,
            volume_type: VolumeType::Local,
        };
        let entry2 = IndexEntry {
            opstamp: Opstamp::deletion(20),
            kind: Kind::File,
            last_modified: 0,
            last_accessed: 0,
            category: 0,
            volume_type: VolumeType::Local,
        };

        collector.insert("/a".to_string(), "vol1".to_string(), entry1);
        collector.insert("/a".to_string(), "vol1".to_string(), entry2);

        let results: Vec<_> = collector.finish().collect();
        assert_eq!(results.len(), 0); // Deletion (version 20) should win and be filtered by finish()
    }
}
