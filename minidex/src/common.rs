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
