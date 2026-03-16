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
    volume: &str,
    path_bytes: &[u8],
    sequence: u64,
    active_tombstones: &[(Option<String>, String, u64)],
) -> bool {
    active_tombstones
        .iter()
        .any(|(tombstone_volume, prefix, stamp)| {
            let prefix_bytes = prefix.as_bytes();
            path_bytes.len() >= prefix_bytes.len()
                && tombstone_volume.as_ref().is_none_or(|v| v == volume)
                && path_bytes[..prefix_bytes.len()].eq_ignore_ascii_case(prefix_bytes)
                && (path_bytes.len() == prefix_bytes.len()
                    || path_bytes[prefix_bytes.len()] == std::path::MAIN_SEPARATOR as u8)
                && sequence < *stamp
        })
}

pub mod category {
    pub const OTHER: u8 = 0;
    pub const ARCHIVE: u8 = 1 << 0;
    pub const DOCUMENT: u8 = 1 << 1;
    pub const IMAGE: u8 = 1 << 2;
    pub const VIDEO: u8 = 1 << 3;
    pub const AUDIO: u8 = 1 << 4;
    pub const TEXT: u8 = 1 << 5;
}

/// Volume type, used to distinguish local volumes
/// from remote and network volumes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum VolumeType {
    Local = 0,
    Network = 1,
    Removable = 2,
    Unknown = 3,
}

impl From<u8> for VolumeType {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Local,
            1 => Self::Network,
            2 => Self::Removable,
            _ => Self::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kind_conversions() {
        assert_eq!(Kind::from(0), Kind::File);
        assert_eq!(Kind::from(1), Kind::Directory);
        assert_eq!(Kind::from(2), Kind::Symlink);
        assert_eq!(u8::from(Kind::File), 0);
        assert_eq!(u8::from(Kind::Directory), 1);
        assert_eq!(u8::from(Kind::Symlink), 2);
    }

    #[test]
    fn test_is_tombstoned() {
        let sep = std::path::MAIN_SEPARATOR;
        let active_tombstones = vec![
            (None, "/foo".to_string(), 100),
            (Some("vol1".to_string()), "/bar".to_string(), 200),
        ];

        // Match prefix (None volume), sequence < stamp, has separator
        assert!(is_tombstoned(
            "volX",
            format!("/foo{}abc", sep).as_bytes(),
            50,
            &active_tombstones
        ));

        // Match exact prefix (None volume), sequence < stamp
        assert!(is_tombstoned("volX", b"/foo", 50, &active_tombstones));

        // Match prefix (vol1 volume), sequence < stamp, has separator
        assert!(is_tombstoned(
            "vol1",
            format!("/bar{}abc", sep).as_bytes(),
            50,
            &active_tombstones
        ));

        // Volume mismatch
        assert!(!is_tombstoned(
            "vol2",
            format!("/bar{}abc", sep).as_bytes(),
            50,
            &active_tombstones
        ));

        // Sequence >= stamp
        assert!(!is_tombstoned(
            "vol1",
            format!("/bar{}abc", sep).as_bytes(),
            200,
            &active_tombstones
        ));

        // No match prefix (different word)
        assert!(!is_tombstoned(
            "vol1",
            b"/foobar/abc",
            50,
            &active_tombstones
        ));

        // Case-insensitive match on prefix (as per implementation)
        assert!(is_tombstoned(
            "volX",
            format!("/FOO{}abc", sep).as_bytes(),
            50,
            &active_tombstones
        ));
    }
}
