/// Basic opstamp implementation.
/// We use the most significant bit as a tombstone to indicate if
/// the opstamp refers to an insertion or deletion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Opstamp(u64);

impl Opstamp {
    const TOMBSTONE_BIT: u64 = 1 << 63;
    const SEQ_MASK: u64 = !Self::TOMBSTONE_BIT;

    #[inline]
    pub(crate) fn deletion(seq: u64) -> Self {
        Self(seq | Self::TOMBSTONE_BIT)
    }

    #[inline]
    pub(crate) fn insertion(seq: u64) -> Self {
        Self(seq & Self::SEQ_MASK)
    }

    #[inline]
    pub(crate) fn is_deletion(&self) -> bool {
        (self.0 & Self::TOMBSTONE_BIT) != 0
    }

    #[inline]
    pub(crate) fn sequence(&self) -> u64 {
        self.0 & Self::SEQ_MASK
    }
}

impl From<u64> for Opstamp {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl std::ops::Deref for Opstamp {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
