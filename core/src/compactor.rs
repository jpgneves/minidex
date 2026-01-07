pub struct CompactorConfig {
    min_merge_count: usize,
    max_size_ratio: f32,
    memory_threshold: usize,
    deletion_threshold: usize,
}

impl Default for CompactorConfig {
    fn default() -> Self {
        CompactorConfigBuilder::default().build()
    }
}

pub struct CompactorConfigBuilder {
    min_merge_count: usize,
    max_size_ratio: f32,
    memory_threshold: usize,
    deletion_threshold: usize,
}

impl Default for CompactorConfigBuilder {
    fn default() -> Self {
        Self {
            min_merge_count: 4,
            max_size_ratio: 1.5,
            memory_threshold: 100 * 1024 * 1024, // Default to 100MB usage
            deletion_threshold: 1000,            // Trigger compaction on 1000 deletes
        }
    }
}

impl CompactorConfigBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn min_merge_count(self, min_merge_count: usize) -> Self {
        Self {
            min_merge_count,
            ..self
        }
    }

    pub fn max_size_ratio(self, max_size_ratio: f32) -> Self {
        Self {
            max_size_ratio,
            ..self
        }
    }

    pub fn memory_threshold(self, memory_threshold: usize) -> Self {
        Self {
            memory_threshold,
            ..self
        }
    }

    pub fn deletion_threshold(self, deletion_threshold: usize) -> Self {
        Self {
            deletion_threshold,
            ..self
        }
    }

    pub fn build(self) -> CompactorConfig {
        CompactorConfig {
            min_merge_count: self.min_merge_count,
            max_size_ratio: self.max_size_ratio,
            memory_threshold: self.memory_threshold,
            deletion_threshold: self.deletion_threshold,
        }
    }
}
