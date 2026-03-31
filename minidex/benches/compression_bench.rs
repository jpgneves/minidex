use criterion::{Criterion, criterion_group, criterion_main};
use minidex::segmented_index::SegmentedIndex;
use minidex::{IndexEntry, Kind, VolumeType, opstamp::Opstamp};
use tempfile::tempdir;

fn bench_dat_compression(c: &mut Criterion) {
    let counts = [1000, 10000];
    let mut group = c.benchmark_group("dat_compression");

    for count in counts {
        group.bench_function(format!("build_segment_{}", count), |b| {
            b.iter_with_setup(
                || {
                    let dir = tempdir().expect("failed to create temp dir");
                    let out_path = dir.path().join("bench_seg");
                    let entries: Vec<_> = (0..count)
                        .map(|i| {
                            (
                                format!("/some/path/to/a/file/that/is/somewhat/long_{}.txt", i),
                                "volume_name_here".to_string(),
                                IndexEntry {
                                    opstamp: Opstamp::insertion(i as u64),
                                    kind: Kind::File,
                                    last_modified: 1000,
                                    last_accessed: 1000,
                                    category: 1,
                                    volume_type: VolumeType::Local,
                                },
                            )
                        })
                        .collect();
                    (dir, out_path, entries)
                },
                |(_dir, out_path, entries)| {
                    SegmentedIndex::build_segment_files(&out_path, entries, false, None)
                        .expect("build failed");
                },
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_dat_compression);
criterion_main!(benches);
