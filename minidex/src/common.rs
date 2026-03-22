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
    if active_tombstones.is_empty() {
        return false;
    }

    let sep = std::path::MAIN_SEPARATOR as u8;

    for (tombstone_volume, prefix, stamp) in active_tombstones {
        if sequence >= *stamp {
            continue;
        }

        let prefix_bytes = prefix.as_bytes();
        if path_bytes.len() < prefix_bytes.len() {
            continue;
        }

        if let Some(v) = tombstone_volume
            && v != volume
        {
            continue;
        }

        if path_bytes[..prefix_bytes.len()].eq_ignore_ascii_case(prefix_bytes)
            && (path_bytes.len() == prefix_bytes.len() || path_bytes[prefix_bytes.len()] == sep)
        {
            return true;
        }
    }
    false
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
        let sep = std::path::MAIN_SEPARATOR_STR;
        let active_tombstones = vec![
            (None, format!("{}foo", sep), 100),
            (Some("vol1".to_string()), format!("{}bar", sep), 200),
        ];

        // Match prefix (None volume), sequence < stamp, has separator
        assert!(is_tombstoned(
            "volX",
            format!("{}foo{}abc", sep, sep).as_bytes(),
            50,
            &active_tombstones
        ));

        // Match exact prefix (None volume), sequence < stamp
        assert!(is_tombstoned(
            "volX",
            format!("{}foo", sep).as_bytes(),
            50,
            &active_tombstones
        ));

        // Match prefix (vol1 volume), sequence < stamp, has separator
        assert!(is_tombstoned(
            "vol1",
            format!("{}bar{}abc", sep, sep).as_bytes(),
            50,
            &active_tombstones
        ));

        // Volume mismatch
        assert!(!is_tombstoned(
            "vol2",
            format!("{}bar{}abc", sep, sep).as_bytes(),
            50,
            &active_tombstones
        ));

        // Sequence >= stamp
        assert!(!is_tombstoned(
            "vol1",
            format!("{}bar{}abc", sep, sep).as_bytes(),
            200,
            &active_tombstones
        ));

        // No match prefix (different word)
        assert!(!is_tombstoned(
            "vol1",
            format!("{}foobar{}abc", sep, sep).as_bytes(),
            50,
            &active_tombstones
        ));

        // Case-insensitive match on prefix (as per implementation)
        assert!(is_tombstoned(
            "volX",
            format!("{}FOO{}abc", sep, sep).as_bytes(),
            50,
            &active_tombstones
        ));
    }

    #[test]
    #[cfg(windows)]
    fn test_is_tombstoned_windows_paths() {
        let active_tombstones = vec![
            (None, "c:\\users\\joao".to_string(), 100),
            (None, "\\\\?\\c:\\windows".to_string(), 200),
            (None, "\\\\server\\share\\docs".to_string(), 300),
        ];

        // Drive letter match
        assert!(is_tombstoned(
            "vol",
            b"C:\\Users\\joao\\file.txt",
            50,
            &active_tombstones
        ));
        assert!(is_tombstoned(
            "vol",
            b"c:\\users\\joao",
            50,
            &active_tombstones
        ));

        // UNC path match (long path prefix)
        assert!(is_tombstoned(
            "vol",
            b"\\\\?\\C:\\Windows\\System32",
            50,
            &active_tombstones
        ));

        // UNC server/share match
        assert!(is_tombstoned(
            "vol",
            b"\\\\server\\share\\docs\\report.pdf",
            50,
            &active_tombstones
        ));

        // No match (different drive or share)
        assert!(!is_tombstoned(
            "vol",
            b"D:\\Users\\joao",
            50,
            &active_tombstones
        ));
        assert!(!is_tombstoned(
            "vol",
            b"\\\\other\\share\\docs",
            50,
            &active_tombstones
        ));
    }
}
