//! Reusable search buffers
//! Because minidex is meant for applications where each
//! keystroke can run a query, and the used memory is mostly
//! determined by how many matches a given token results in,
//! constantly freeing and reallocating buffers can cause system
//! allocators to hold many hundreds of megabytes of freed blocks
//! in pages waiting to be reclaimed, thus inflating the application's
//! memory usage.
//! To support concurrent searches, we use one buffer pool per index.

use crate::sync::Mutex;

pub(crate) const RETAINED_BYTES_PER_BUFFER: usize = 16 * 1024 * 1024;
pub(crate) const MAX_POOLED_BUFFERS: usize = 4;

/// The working buffers for one search, used across in-memory and on-disk
/// paths.
#[derive(Default)]
pub(crate) struct SearchScratch {
    pub(crate) prefiltered: Vec<(u64, u32)>,
    pub(crate) token_docs: Vec<u32>,
    pub(crate) current_matches: Vec<u32>,
    pub(crate) intersect: Vec<u32>,
    pub(crate) sortable: Vec<(u64, u32)>,
    pub(crate) exact_words: Vec<u64>,
    pub(crate) extension_words: Vec<u64>,
}

impl SearchScratch {
    fn trim(&mut self) {
        fn do_trim<T>(buffer: &mut Vec<T>) {
            buffer.clear();
            let retained = RETAINED_BYTES_PER_BUFFER / size_of::<T>().max(1);
            if buffer.capacity() > retained {
                buffer.shrink_to(retained);
            }
        }

        do_trim(&mut self.prefiltered);
        do_trim(&mut self.token_docs);
        do_trim(&mut self.current_matches);
        do_trim(&mut self.intersect);
        do_trim(&mut self.sortable);
        do_trim(&mut self.exact_words);
        do_trim(&mut self.extension_words);
    }
}

#[derive(Default)]
pub(crate) struct ScratchPool {
    pooled: Mutex<Vec<SearchScratch>>,
}

impl ScratchPool {
    pub(crate) fn take(&self) -> SearchScratch {
        self.pooled
            .lock()
            .ok()
            .and_then(|mut pooled| pooled.pop())
            .unwrap_or_default()
    }

    pub(crate) fn give(&self, mut scratch: SearchScratch) {
        scratch.trim();
        if let Ok(mut pooled) = self.pooled.lock()
            && pooled.len() < MAX_POOLED_BUFFERS
        {
            pooled.push(scratch);
        }
    }

    #[cfg(test)]
    pub(crate) fn pooled(&self) -> usize {
        self.pooled.lock().map(|pooled| pooled.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returned_scratch_keeps_at_most_the_retained_capacity() {
        let pool = ScratchPool::default();
        let mut scratch = pool.take();
        scratch.token_docs = Vec::with_capacity(RETAINED_BYTES_PER_BUFFER); // 4x the retained bytes
        scratch.prefiltered = Vec::with_capacity(1024);
        pool.give(scratch);

        let scratch = pool.take();
        assert!(scratch.token_docs.capacity() * size_of::<u32>() <= RETAINED_BYTES_PER_BUFFER);
        assert!(
            scratch.prefiltered.capacity() >= 1024,
            "small buffers keep their capacity"
        );
    }

    #[test]
    fn pool_keeps_a_bounded_number_of_scratches() {
        let pool = ScratchPool::default();
        let taken: Vec<_> = (0..MAX_POOLED_BUFFERS + 3).map(|_| pool.take()).collect();
        for scratch in taken {
            pool.give(scratch);
        }
        assert_eq!(pool.pooled(), MAX_POOLED_BUFFERS);
    }
}
