#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    File,
    Directory,
    Symlink,
}

impl From<u8> for Kind {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::File,
            1 => Self::Directory,
            2 => Self::Symlink,
            _ => unreachable!(),
        }
    }
}

impl From<Kind> for u8 {
    fn from(val: Kind) -> Self {
        match val {
            Kind::File => 0,
            Kind::Directory => 1,
            Kind::Symlink => 2,
        }
    }
}

#[inline]
pub(crate) fn is_tombstoned(
    path_bytes: &[u8],
    sequence: u64,
    active_tombstones: &[(String, u64)],
) -> bool {
    active_tombstones.iter().any(|(prefix, stamp)| {
        let prefix_bytes = prefix.as_bytes();
        path_bytes.len() >= prefix_bytes.len()
            && path_bytes[..prefix_bytes.len()].eq_ignore_ascii_case(prefix_bytes)
            && sequence < *stamp
    })
}

pub mod category {
    pub const OTHER: u16 = 0;
    pub const ARCHIVE: u16 = 1 << 0;
    pub const DOCUMENT: u16 = 1 << 1;
    pub const IMAGE: u16 = 1 << 2;
    pub const VIDEO: u16 = 1 << 3;
    pub const AUDIO: u16 = 1 << 4;
    pub const TEXT: u16 = 1 << 5;
}
