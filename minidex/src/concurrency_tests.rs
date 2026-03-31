#[cfg(all(test, feature = "shuttle"))]
mod _concurrency_tests {
    use crate::sync::Arc;
    use crate::*;
    use shuttle::check_random;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn setup_index() -> (TempDir, Index) {
        let temp_dir = TempDir::new().unwrap();
        let config = IndexConfig {
            compactor_config: CompactorConfig {
                flush_threshold: 2,
                min_merge_count: 2,
                ..Default::default()
            },
            search_threads: 2,
        };
        let index = Index::open_with_config(temp_dir.path(), config).unwrap();
        (temp_dir, index)
    }

    #[test]
    fn test_concurrent_insert_search() {
        check_random(
            || {
                let (_temp, index) = setup_index();
                let index = Arc::new(index);
                let num_threads = 2;
                let ops_per_thread = 3;

                let mut handles = vec![];

                // Inserter threads
                for t in 0..num_threads {
                    let index_c = Arc::clone(&index);
                    handles.push(crate::sync::thread::spawn(move || {
                        for i in 0..ops_per_thread {
                            let path = format!("/file_{}_{}.txt", t, i);
                            index_c
                                .insert(FilesystemEntry {
                                    path: PathBuf::from(path),
                                    volume: "vol1".to_string(),
                                    kind: Kind::File,
                                    last_modified: 100,
                                    last_accessed: 100,
                                    category: 0,
                                    volume_type: VolumeType::Local,
                                })
                                .unwrap();
                        }
                    }));
                }

                // Searcher thread
                let index_s = Arc::clone(&index);
                handles.push(crate::sync::thread::spawn(move || {
                    for _ in 0..(num_threads * ops_per_thread) {
                        let _ = index_s
                            .search("file", 10, 0, SearchOptions::default())
                            .unwrap();
                        crate::sync::thread::yield_now();
                    }
                }));

                for handle in handles {
                    handle.join().unwrap();
                }
            },
            100,
        );
    }

    #[test]
    fn test_concurrent_delete_search() {
        check_random(
            || {
                let (_temp, index) = setup_index();
                let index = Arc::new(index);

                // Pre-fill
                for i in 0..5 {
                    index
                        .insert(FilesystemEntry {
                            path: PathBuf::from(format!("/pre_{}.txt", i)),
                            volume: "vol1".to_string(),
                            kind: Kind::File,
                            last_modified: 100,
                            last_accessed: 100,
                            category: 0,
                            volume_type: VolumeType::Local,
                        })
                        .unwrap();
                }

                let mut handles = vec![];

                // Deleter thread
                let index_d = Arc::clone(&index);
                handles.push(crate::sync::thread::spawn(move || {
                    for i in 0..5 {
                        let path = PathBuf::from(format!("/pre_{}.txt", i));
                        let _ = index_d.delete(&path);
                    }
                }));

                // Searcher thread
                let index_s = Arc::clone(&index);
                handles.push(crate::sync::thread::spawn(move || {
                    for _ in 0..10 {
                        let _ = index_s
                            .search("pre", 10, 0, SearchOptions::default())
                            .unwrap();
                        crate::sync::thread::yield_now();
                    }
                }));

                for handle in handles {
                    handle.join().unwrap();
                }
            },
            100,
        );
    }

    #[test]
    fn test_concurrent_flush_compaction() {
        check_random(
            || {
                let (_temp, index) = setup_index();
                let index = Arc::new(index);

                let mut handles = vec![];

                // Heavy inserter to trigger flushes
                let index_i = Arc::clone(&index);
                handles.push(crate::sync::thread::spawn(move || {
                    for i in 0..10 {
                        index_i
                            .insert(FilesystemEntry {
                                path: PathBuf::from(format!("/file_{}.txt", i)),
                                volume: "vol1".to_string(),
                                kind: Kind::File,
                                last_modified: 100,
                                last_accessed: 100,
                                category: 0,
                                volume_type: VolumeType::Local,
                            })
                            .unwrap();
                    }
                }));

                // Manual compaction trigger
                let index_c = Arc::clone(&index);
                handles.push(crate::sync::thread::spawn(move || {
                    for _ in 0..2 {
                        let _ = index_c.force_compact_all();
                        crate::sync::thread::yield_now();
                    }
                }));

                for handle in handles {
                    handle.join().unwrap();
                }
            },
            100,
        );
    }

    #[test]
    fn test_concurrent_prefix_delete() {
        check_random(
            || {
                let (_temp, index) = setup_index();
                let index = Arc::new(index);

                let mut handles = vec![];

                // Inserter
                let index_i = Arc::clone(&index);
                handles.push(crate::sync::thread::spawn(move || {
                    for i in 0..5 {
                        index_i
                            .insert(FilesystemEntry {
                                path: PathBuf::from(format!("/dir1/file_{}.txt", i)),
                                volume: "vol1".to_string(),
                                kind: Kind::File,
                                last_modified: 100,
                                last_accessed: 100,
                                category: 0,
                                volume_type: VolumeType::Local,
                            })
                            .unwrap();
                    }
                }));

                // Prefix deleter
                let index_d = Arc::clone(&index);
                handles.push(crate::sync::thread::spawn(move || {
                    let _ = index_d.delete_prefix("/dir1");
                }));

                // Searcher
                let index_s = Arc::clone(&index);
                handles.push(crate::sync::thread::spawn(move || {
                    for _ in 0..5 {
                        let _ = index_s
                            .search("file", 10, 0, SearchOptions::default())
                            .unwrap();
                    }
                }));

                for handle in handles {
                    handle.join().unwrap();
                }
            },
            100,
        );
    }
}
