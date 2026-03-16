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

    #[inline]
    pub(crate) fn as_bytes(&self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    #[inline]
    pub(crate) fn from_bytes(bytes: &[u8]) -> Self {
        Self(u64::from_le_bytes(
            bytes.try_into().expect("bad binary format for opstamp"),
        ))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_opstamp_insertion() {
        let op = Opstamp::insertion(42);
        assert!(!op.is_deletion());
        assert_eq!(op.sequence(), 42);
        assert_eq!(*op, 42);
    }

    #[test]
    fn test_opstamp_deletion() {
        let op = Opstamp::deletion(42);
        assert!(op.is_deletion());
        assert_eq!(op.sequence(), 42);
        assert!(*op > Opstamp::TOMBSTONE_BIT);
    }

    #[test]
    fn test_opstamp_serialization() {
        let op = Opstamp::deletion(12345);
        let bytes = op.as_bytes();
        let op2 = Opstamp::from_bytes(&bytes);
        assert_eq!(op, op2);
        assert!(op2.is_deletion());
        assert_eq!(op2.sequence(), 12345);
    }

    #[test]
    fn test_opstamp_ordering() {
        let op1 = Opstamp::insertion(10);
        let op2 = Opstamp::insertion(20);
        let op3 = Opstamp::deletion(5);

        assert!(op1 < op2);
        // Deletions have MSB set, so they are always "larger" than insertions numerically
        assert!(op1 < op3);
        assert!(op2 < op3);
    }
}
