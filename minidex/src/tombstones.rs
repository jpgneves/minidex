use std::{collections::HashMap, sync::OnceLock};

pub type Tombstone = (Option<String>, String, u64);

/// The set of prefix tombstones that have not been applied
/// by a compaction.
/// Provides lookup by ancestor, where a tombstone matches a path
/// when its prefix is the whole path or ends at one of the paths'
/// separators.
/// When a tombstone's path prefix is covered by another's, it is
/// redundant and dropped on insert.
#[derive(Debug, Default)]
pub(crate) struct TombstoneSet {
    entries: Vec<Tombstone>,
    lookup: OnceLock<Lookup>,
}

impl TombstoneSet {
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn iter(&self) -> std::slice::Iter<'_, Tombstone> {
        self.entries.iter()
    }

    pub(crate) fn insert(&mut self, tombstone: Tombstone) {
        self.lookup = OnceLock::new();
        if self
            .entries
            .iter()
            .any(|existing| covers(existing, &tombstone))
        {
            return;
        }
        self.entries
            .retain(|existing| !covers(&tombstone, existing));
        self.entries.push(tombstone);
    }

    pub(crate) fn extend(&mut self, tombstones: impl IntoIterator<Item = Tombstone>) {
        for tombstone in tombstones {
            self.insert(tombstone);
        }
    }

    pub(crate) fn retain(&mut self, keep: impl FnMut(&Tombstone) -> bool) {
        self.lookup = OnceLock::new();
        self.entries.retain(keep);
    }

    pub(crate) fn clear(&mut self) {
        self.lookup = OnceLock::new();
        self.entries.clear();
    }

    /// Whether a document written at `sequence` is hidden by a tombstone written after it
    #[inline]
    pub(crate) fn is_tombstoned(&self, volume: &str, path_bytes: &[u8], sequence: u64) -> bool {
        if self.entries.is_empty() {
            return false;
        }
        self.lookup
            .get_or_init(|| Lookup::build(&self.entries))
            .is_tombstoned(volume, path_bytes, sequence)
    }
}

impl Clone for TombstoneSet {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
            lookup: OnceLock::new(),
        }
    }
}

impl From<Vec<Tombstone>> for TombstoneSet {
    fn from(tombstones: Vec<Tombstone>) -> Self {
        let mut set = Self::default();
        set.extend(tombstones);
        set
    }
}

fn covers(outer: &Tombstone, inner: &Tombstone) -> bool {
    let (outer_volume, outer_prefix, outer_stamp) = outer;
    let (inner_volume, inner_prefix, inner_stamp) = inner;
    outer_stamp >= inner_stamp
        && (outer_volume.is_none() || outer_volume == inner_volume)
        && prefix_matches(outer_prefix.as_bytes(), inner_prefix.as_bytes())
}

fn prefix_matches(prefix: &[u8], path: &[u8]) -> bool {
    path.len() >= prefix.len()
        && path[..prefix.len()].eq_ignore_ascii_case(prefix)
        && (path.len() == prefix.len() || path[prefix.len()] == std::path::MAIN_SEPARATOR as u8)
}

#[derive(Debug, Default)]
struct Lookup {
    by_prefix: HashMap<Box<[u8]>, Stamps>,
    max_stamp: u64,
    max_prefix_len: usize,
}

impl Lookup {
    fn build(entries: &[Tombstone]) -> Self {
        let mut lookup = Lookup::default();

        for (volume, prefix, stamp) in entries {
            let key: Box<[u8]> = prefix.as_bytes().to_ascii_lowercase().into_boxed_slice();
            lookup.max_stamp = lookup.max_stamp.max(*stamp);
            lookup.max_prefix_len = lookup.max_prefix_len.max(key.len());

            let stamps = lookup.by_prefix.entry(key).or_default();

            match volume {
                None => stamps.any_volume = stamps.any_volume.max(*stamp),
                Some(volume) => match stamps.volumes.iter_mut().find(|(v, _)| v == volume) {
                    Some((_, existing)) => *existing = (*existing).max(*stamp),
                    None => stamps.volumes.push((volume.clone(), *stamp)),
                },
            }
        }

        lookup
    }

    fn is_tombstoned(&self, volume: &str, path_bytes: &[u8], sequence: u64) -> bool {
        if sequence >= self.max_stamp {
            return false;
        }

        let sep = std::path::MAIN_SEPARATOR as u8;
        let scanned = &path_bytes[..path_bytes.len().min(self.max_prefix_len)];
        let lowered = scanned.to_ascii_lowercase();
        let hides = |end: usize| {
            self.by_prefix.get(&lowered[..end]).is_some_and(|stamps| {
                sequence < stamps.any_volume
                    || stamps
                        .volumes
                        .iter()
                        .any(|(v, stamp)| sequence < *stamp && v == volume)
            })
        };
        (0..=lowered.len())
            .any(|end| (end == path_bytes.len() || path_bytes[end] == sep) && hides(end))
    }
}

#[derive(Debug, Default)]
struct Stamps {
    any_volume: u64,
    volumes: Vec<(String, u64)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sep() -> char {
        std::path::MAIN_SEPARATOR
    }

    fn path(parts: &[&str]) -> String {
        parts.iter().map(|p| format!("{}{}", sep(), p)).collect()
    }

    #[test]
    fn nested_tombstone_is_subsumed_by_its_parent() {
        let mut set = TombstoneSet::default();
        set.insert((None, path(&["a", "b"]), 10));
        set.insert((None, path(&["a"]), 20));
        assert_eq!(set.len(), 1);
        assert_eq!(set.iter().next().unwrap().1, path(&["a"]));
    }

    #[test]
    fn older_parent_does_not_subsume_a_newer_child() {
        let mut set = TombstoneSet::default();
        set.insert((None, path(&["a"]), 10));
        set.insert((None, path(&["a", "b"]), 20));
        assert_eq!(
            set.len(),
            2,
            "documents under a/b written between 10 and 20 need the child"
        );
    }

    #[test]
    fn a_newer_parent_absorbs_an_older_child_inserted_later() {
        // WAL recovery re-inserts old tombstones after live ones.
        let mut set = TombstoneSet::default();
        set.insert((None, path(&["a"]), 50));
        set.insert((None, path(&["a", "b"]), 5));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn volume_scoped_parent_does_not_subsume_an_all_volume_child() {
        let mut set = TombstoneSet::default();
        set.insert((None, path(&["a", "b"]), 10));
        set.insert((Some("vol".to_string()), path(&["a"]), 20));
        assert_eq!(set.len(), 2);
        set.insert((None, path(&["a"]), 30));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn sibling_with_shared_prefix_is_not_subsumed() {
        let mut set = TombstoneSet::default();
        set.insert((None, path(&["ab"]), 10));
        set.insert((None, path(&["a"]), 20));
        assert_eq!(set.len(), 2);
        assert!(!set.is_tombstoned("v", path(&["ab", "x"]).as_bytes(), 15));
        assert!(set.is_tombstoned("v", path(&["ab", "x"]).as_bytes(), 5));
    }

    #[test]
    fn lookup_is_rebuilt_after_mutation() {
        let mut set = TombstoneSet::default();
        set.insert((None, path(&["a"]), 10));
        assert!(set.is_tombstoned("v", path(&["a", "x"]).as_bytes(), 1));
        set.retain(|_| false);
        assert!(!set.is_tombstoned("v", path(&["a", "x"]).as_bytes(), 1));
        set.insert((None, path(&["b"]), 10));
        assert!(set.is_tombstoned("v", path(&["b"]).as_bytes(), 1));
    }
}
