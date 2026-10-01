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
}
