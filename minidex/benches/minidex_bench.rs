use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use minidex::{
    CompactorConfigBuilder, FilesystemEntry, Index, Kind, SearchOptions, VolumeType, category,
    tokenize,
};
use std::hint::black_box;
use std::path::PathBuf;
use tempfile::tempdir;

fn bench_tokenizer(c: &mut Criterion) {
    let inputs = [
        ("simple word", "hello"),
        (
            "long path",
            "/usr/local/bin/some_extremely_long_filename_with_underscores_and_numbers_2024_report.pdf",
        ),
        ("cjk string", "これは日本語のテストです。"),
        (
            "camelCase",
            "MySuperLongCamelCaseIdentifierForTestingTokenizerPerformance",
        ),
    ];

    let mut group = c.benchmark_group("tokenizer");
    for (name, input) in inputs {
        group.bench_with_input(BenchmarkId::new("tokenize", name), input, |b, i| {
            b.iter(|| tokenize(black_box(i)))
        });
    }
    group.finish();
}

fn create_entry(i: usize) -> FilesystemEntry {
    FilesystemEntry {
        path: PathBuf::from(format!("/foo/bar_{}.txt", i)),
        volume: "vol1".to_string(),
        kind: Kind::File,
        last_modified: 1000,
        last_accessed: 1000,
        category: category::TEXT,
        volume_type: VolumeType::Local,
    }
}

fn populate_index(index: &Index, count: usize) {
    for i in 0..count {
        index.insert(create_entry(i)).expect("failed to insert");
    }
}

fn bench_index_insert(c: &mut Criterion) {
    let sizes = [500, 1000, 10000, 50000, 100000];
    let mut group = c.benchmark_group("index_insert");

    for size in sizes {
        let dir = tempdir().expect("failed to create temp dir");
        let index = Index::open(dir.path()).expect("failed to open index");
        populate_index(&index, size);

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            let mut i = size;
            b.iter(|| {
                index.insert(create_entry(i)).expect("failed to insert");
                i += 1;
            })
        });
    }
    group.finish();
}

fn bench_index_insert_batch(c: &mut Criterion) {
    let sizes = [1000, 10000, 50000];
    let chunk_sizes = [100, 1000, 5000];
    let mut group = c.benchmark_group("index_insert_batch");

    for size in sizes {
        for chunk_size in chunk_sizes {
            if chunk_size > size {
                continue;
            }
            let dir = tempdir().expect("failed to create temp dir");
            let index = Index::open(dir.path()).expect("failed to open index");

            group.bench_with_input(
                BenchmarkId::new(format!("size_{}", size), chunk_size),
                &chunk_size,
                |b, &cs| {
                    b.iter_with_setup(
                        || (0..size).map(create_entry).collect::<Vec<_>>(),
                        |entries| {
                            index
                                .insert_batch(entries, cs)
                                .expect("failed to insert batch");
                        },
                    );
                },
            );
        }
    }
    group.finish();
}

fn bench_index_search(c: &mut Criterion) {
    let sizes = [500, 1000, 10000, 50000, 100000];
    let mut group = c.benchmark_group("index_search");

    for size in sizes {
        let dir = tempdir().expect("failed to create temp dir");
        let config = CompactorConfigBuilder::new().flush_threshold(2000).build();
        
        // Single-threaded index
        let st_config = minidex::IndexConfig {
            compactor_config: config,
            search_threads: 1,
        };
        let index_st = Index::open_with_config(dir.path(), st_config).expect("failed to open index");

        populate_index(&index_st, size);

        // Wait for background flushes to finish
        std::thread::sleep(std::time::Duration::from_millis(500));

        group.bench_with_input(BenchmarkId::new("disk_search_hit_st", size), &size, |b, _| {
            b.iter(|| {
                let _ = index_st
                    .search(black_box("bar_50"), 10, 0, SearchOptions::default())
                    .expect("search failed");
            })
        });

        group.bench_with_input(
            BenchmarkId::new("disk_search_prefix_st", size),
            &size,
            |b, _| {
                b.iter(|| {
                    let _ = index_st
                        .search(black_box("bar"), 10, 0, SearchOptions::default())
                        .expect("search failed");
                })
            },
        );

        // Multi-threaded index (default)
        let mt_config = minidex::IndexConfig {
            compactor_config: config,
            search_threads: 0,
        };
        let index_mt = Index::open_with_config(dir.path(), mt_config).expect("failed to open index");
        // It's the same physical index, we just want to test different search pools.
        // Actually, we should probably reopen it.
        
        group.bench_with_input(BenchmarkId::new("disk_search_hit_mt", size), &size, |b, _| {
            b.iter(|| {
                let _ = index_mt
                    .search(black_box("bar_50"), 10, 0, SearchOptions::default())
                    .expect("search failed");
            })
        });

        group.bench_with_input(
            BenchmarkId::new("disk_search_prefix_mt", size),
            &size,
            |b, _| {
                b.iter(|| {
                    let _ = index_mt
                        .search(black_box("bar"), 10, 0, SearchOptions::default())
                        .expect("search failed");
                })
            },
        );

        group.bench_with_input(BenchmarkId::new("mem_search_hit", size), &size, |b, _| {
            let path = format!("/foo/mem_only_entry_{}.txt", size);
            index_st
                .insert(FilesystemEntry {
                    path: PathBuf::from(&path),
                    volume: "vol1".to_string(),
                    kind: Kind::File,
                    last_modified: 1000,
                    last_accessed: 1000,
                    category: category::TEXT,
                    volume_type: VolumeType::Local,
                })
                .expect("failed to insert");

            let search_term = format!("mem_only_entry_{}", size);
            b.iter(|| {
                let _ = index_st
                    .search(black_box(&search_term), 10, 0, SearchOptions::default())
                    .expect("search failed");
            })
        });
    }

    group.finish();
}

fn bench_index_delete(c: &mut Criterion) {
    let sizes = [500, 1000, 10000, 50000, 100000];
    let mut group = c.benchmark_group("index_delete");

    for size in sizes {
        let dir = tempdir().expect("failed to create temp dir");
        let index = Index::open(dir.path()).expect("failed to open index");
        populate_index(&index, size);

        group.bench_with_input(BenchmarkId::new("delete_path", size), &size, |b, _| {
            let mut i = 0;
            b.iter(|| {
                let path = PathBuf::from(format!("/foo/bar_{}.txt", i));
                index.delete(black_box(&path)).expect("delete failed");
                i = (i + 1) % size;
            })
        });

        group.bench_with_input(BenchmarkId::new("delete_prefix", size), &size, |b, _| {
            let mut i = 0;
            b.iter(|| {
                let prefix = format!("/foo/bar_{}", i);
                index
                    .delete_prefix(black_box(&prefix))
                    .expect("delete_prefix failed");
                i = (i + 1) % size;
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_tokenizer,
    bench_index_insert,
    bench_index_insert_batch,
    bench_index_search,
    bench_index_delete
);
criterion_main!(benches);
