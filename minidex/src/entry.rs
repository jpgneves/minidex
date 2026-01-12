use std::path::PathBuf;

use crate::{common::Kind, opstamp::Opstamp};

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct IndexEntry {
    pub(crate) opstamp: Opstamp,
    pub(crate) kind: Kind,
    pub(crate) content_type: u32,
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

pub struct FilesystemEntry {
    pub path: PathBuf,
    pub kind: Kind,
    pub last_modified: u64,
    pub last_accessed: u64,
}
