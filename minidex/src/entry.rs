use std::path::PathBuf;

use crate::{common::Kind, opstamp::Opstamp};

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct IndexEntry {
    pub(crate) opstamp: Opstamp,
    pub(crate) kind: Kind,
    pub(crate) last_modified: u64,
    pub(crate) last_accessed: u64,
}

impl IndexEntry {
    pub(crate) const SIZE: usize = std::mem::size_of::<Self>();

    pub(crate) fn to_bytes(self) -> [u8; Self::SIZE] {
        unsafe { std::mem::transmute(self) }
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Self {
        let array: [u8; Self::SIZE] = bytes.try_into().expect("invalid entry size");
        unsafe { std::mem::transmute(array) }
    }
}

/// A filesystem entry in Minidex, containing information extracted
/// from files, directories or symlinks by systems populating the index.
pub struct FilesystemEntry {
    /// Path of the entry
    pub path: PathBuf,
    /// Volume mount where the entry exists. On Windows this can be a
    /// letter drive, or a UNC path prefix. On UNIX this should be the
    /// volume mount path
    pub volume: String,
    /// Entry kind (File, Directory or Symlink)
    pub kind: Kind,
    /// Last modified timestamp
    pub last_modified: u64,
    /// Last accessed timestamp
    pub last_accessed: u64,
}
