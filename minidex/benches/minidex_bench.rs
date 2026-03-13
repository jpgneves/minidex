use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use minidex::{
    CompactorConfigBuilder, FilesystemEntry, Index, Kind, SearchOptions, VolumeType, category,
    tokenize,
};
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

fn bench_index_insert(c: &mut Criterion) {
    let dir = tempdir().expect("failed to create temp dir");
    let index = Index::open(dir.path()).expect("failed to open index");

    c.bench_function("index_insert", |b| {
        let mut i = 0;
        b.iter(|| {
            let entry = FilesystemEntry {
                path: PathBuf::from(format!("/foo/bar_{}.txt", i)),
                volume: "vol1".to_string(),
                kind: Kind::File,
                last_modified: 1000,
                last_accessed: 1000,
                category: category::TEXT,
                volume_type: VolumeType::Local,
            };
            index.insert(entry).expect("failed to insert");
            i += 1;
        })
    });
}

fn bench_index_search(c: &mut Criterion) {
    let dir = tempdir().expect("failed to create temp dir");
    let config = CompactorConfigBuilder::new().flush_threshold(100).build();
    let index = Index::open_with_config(dir.path(), config).expect("failed to open index");

    // Populate index with 500 items to trigger multiple flushes
    for i in 0..500 {
        index
            .insert(FilesystemEntry {
                path: PathBuf::from(format!("/foo/bar_{}.txt", i)),
                volume: "vol1".to_string(),
                kind: Kind::File,
                last_modified: 1000,
                last_accessed: 1000,
                category: category::TEXT,
                volume_type: VolumeType::Local,
            })
            .expect("failed to insert");
    }

    // Wait for background flushes to finish
    std::thread::sleep(std::time::Duration::from_millis(500));

    let mut group = c.benchmark_group("index_search");

    group.bench_function("mem_search_hit", |b| {
        // These will be in mem table (the last ones)
        let _ = index
            .insert(FilesystemEntry {
                path: PathBuf::from("/foo/mem_only_entry.txt"),
                volume: "vol1".to_string(),
                kind: Kind::File,
                last_modified: 1000,
                last_accessed: 1000,
                category: category::TEXT,
                volume_type: VolumeType::Local,
            })
            .expect("failed to insert");

        b.iter(|| {
            let _ = index
                .search(black_box("mem_only_entry"), 10, 0, SearchOptions::default())
                .expect("search failed");
        })
    });

    group.bench_function("disk_search_hit", |b| {
        // "bar_50" is definitely in a segment because we inserted 500 with threshold 100
        b.iter(|| {
            let _ = index
                .search(black_box("bar_50"), 10, 0, SearchOptions::default())
                .expect("search failed");
        })
    });

    group.bench_function("disk_search_prefix", |b| {
        b.iter(|| {
            let _ = index
                .search(black_box("bar"), 10, 0, SearchOptions::default())
                .expect("search failed");
        })
    });

    group.finish();
}

fn bench_index_delete(c: &mut Criterion) {
    let dir = tempdir().expect("failed to create temp dir");
    let index = Index::open(dir.path()).expect("failed to open index");

    // Populate index with some data to delete
    for i in 0..1000 {
        index
            .insert(FilesystemEntry {
                path: PathBuf::from(format!("/foo/bar_{}.txt", i)),
                volume: "vol1".to_string(),
                kind: Kind::File,
                last_modified: 1000,
                last_accessed: 1000,
                category: category::TEXT,
                volume_type: VolumeType::Local,
            })
            .expect("failed to insert");
    }

    let mut group = c.benchmark_group("index_delete");

    group.bench_function("delete_path", |b| {
        let mut i = 0;
        b.iter(|| {
            let path = PathBuf::from(format!("/foo/bar_{}.txt", i));
            index.delete(black_box(&path)).expect("delete failed");
            i += 1;
        })
    });

    group.bench_function("delete_prefix", |b| {
        let mut i = 0;
        b.iter(|| {
            let prefix = format!("/prefix_{}", i);
            index
                .delete_prefix(black_box(&prefix))
                .expect("delete_prefix failed");
            i += 1;
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_tokenizer,
    bench_index_insert,
    bench_index_search,
    bench_index_delete
);
criterion_main!(benches);
