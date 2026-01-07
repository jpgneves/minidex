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

impl Into<u8> for Kind {
    fn into(self) -> u8 {
        match self {
            Kind::File => 0,
            Kind::Directory => 1,
            Kind::Symlink => 2,
        }
    }
}
