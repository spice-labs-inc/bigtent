//! The four-path benchmark suite: the
//! data paths for the scan, full-item, and seek shapes across item
//! sizes. The `cursor_original` numbers are the recorded baseline the
//! later phases' ratio gates compare against.
//!
//! Paths:
//! - `cursor_original`: Sansho's byte cursor over the item's original
//!   CBOR bytes (the single-cluster baseline).
//! - `reencode_cursor`: materialized item → CBOR bytes → the cursor
//!   (the "bytes at the end" approach; the timed region includes the
//!   re-encode).
//! - `json_tree`: materialized item → JSON value → the materialized
//!   backend (the "values at the front" approach; the timed region
//!   includes the conversion).
//! - `view_direct`: the reference-view walk over the in-memory value —
//!   **scratch**: runs the mini evaluator until the
//!   production view lands.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

/// The whole public path: evaluate the program, then materialize the result at
/// the boundary. This is what a consumer pays for one query, so it is what
/// these benchmarks measure.
fn run<'a, T: sansho::SanshoTrait<'a>>(
    program: &sansho::Program,
    document: T,
) -> Result<serde_json::Value, sansho::SanshoError> {
    sansho::evaluate(program, document).and_then(|value| sansho::materialize(&value))
}


/// The shared fixture builder (tests/common/mod.rs, included via
/// #[path] — benches cannot import integration-test modules).
#[path = "../src/tests_common.rs"]
mod common;

use common::appendix_a;

/// The scan shape: the north_purls emit —
/// `connections."alias:from"[?starts_with(@, 'pkg:')]` (one row per
/// matched PURL under the flatten form).
fn scan_program() -> std::sync::Arc<sansho::Program> {
    let parsed = sansho::parse("connections.\"alias:from\"[?starts_with(@, 'pkg:')]")
        .expect("parses");
    sansho::compile(&parsed, None).expect("compiles")
}

/// The seek shape: single-field access — `body.file_size`.
fn seek_program() -> std::sync::Arc<sansho::Program> {
    let parsed = sansho::parse("body.file_size").expect("parses");
    sansho::compile(&parsed, None).expect("compiles")
}

/// The full-item shape: `@` (the whole document materialized).
fn full_item_program() -> std::sync::Arc<sansho::Program> {
    let parsed = sansho::parse("@").expect("parses");
    sansho::compile(&parsed, None).expect("compiles")
}

/// The mini view-walker (scratch): evaluates the
/// scan and seek shapes over the in-memory value tree by reference.
mod mini {
    use serde_json::Value as J;

    /// Field lookup: `@.name` on an object (reference view).
    pub fn field<'a>(value: &'a J, name: &str) -> Option<&'a J> {
        value.as_object().and_then(|m| m.get(name))
    }

    /// `connections."alias:from"` then the pkg: prefix filter, counting
    /// matches (no allocation beyond the kept strings).
    pub fn scan_count(value: &J) -> usize {
        let connections = field(value, "connections");
        let alias = connections.and_then(|c| field(c, "alias:from"));
        alias
            .and_then(|a| a.as_array())
            .map(|arr| {
                arr.iter()
                    .filter(|s| {
                        s.as_str().map(|s| s.starts_with("pkg:")).unwrap_or(false)
                    })
                    .count()
            })
            .unwrap_or(0)
    }

    /// `body.file_size` — the seek shape.
    pub fn seek(value: &J) -> Option<&J> {
        field(value, "body").and_then(|b| field(b, "file_size"))
    }
}

fn bench_paths(c: &mut Criterion) {
    // the log4shell-scale shape: the
    // 500k-connection item — the size the High Wire spike promised
    // and no plan committed
    let sizes: [usize; 5] = [10, 1_000, 25_000, 100_000, 500_000];
    for n in sizes {
        let item = appendix_a(n, n / 2);
        let original_bytes = serde_cbor::to_vec(&item).expect("serializes");
        let mut group = c.benchmark_group("source_paths");
        group.throughput(criterion::Throughput::Bytes(original_bytes.len() as u64));

        // ---- scan shape ----
        let program = scan_program();
        group.bench_with_input(
            BenchmarkId::new("scan/byte_source", n),
            &original_bytes,
            |b, bytes| {
                b.iter(|| {
                    black_box(
                        run(black_box(&program), black_box(bytes.as_slice()))
                            .expect("evaluates"),
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("scan/reencode_cursor", n),
            &item,
            |b, item| {
                b.iter(|| {
                    let bytes = serde_cbor::to_vec(black_box(item)).expect("serializes");
                    black_box(
                        run(black_box(&program), black_box(bytes.as_slice()))
                            .expect("evaluates"),
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("scan/json_tree", n),
            &item,
            |b, item| {
                b.iter(|| {
                    black_box(
                        run(black_box(&program), black_box(item))
                            .expect("evaluates"),
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("scan/view_direct", n),
            &item,
            |b, item| {
                b.iter(|| black_box(mini::scan_count(black_box(item))))
            },
        );
        // the zero-copy byte source: the scan via
        // the new source — the T1.10 gate arm (≤ 50% of the recorded
        // scan/cursor_original median)
        group.bench_with_input(
            BenchmarkId::new("scan/cbor_node", n),
            &original_bytes,
            |b, bytes| {
                let prog = scan_program();
                let slice: &[u8] = bytes;
                b.iter(|| black_box(sansho::evaluate(black_box(&prog), black_box(slice))))
            },
        );

        // ---- seek shape ----
        let program = seek_program();
        let program_ref = &program;
        group.bench_with_input(
            BenchmarkId::new("seek/byte_source", n),
            &original_bytes,
            |b, bytes| {
                b.iter(|| {
                    black_box(
                        run(black_box(program_ref), black_box(bytes.as_slice()))
                            .expect("evaluates"),
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("seek/reencode_cursor", n),
            &item,
            |b, item| {
                b.iter(|| {
                    let bytes = serde_cbor::to_vec(black_box(item)).expect("serializes");
                    black_box(
                        run(black_box(&program), black_box(bytes.as_slice()))
                            .expect("evaluates"),
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("seek/json_tree", n),
            &item,
            |b, item| {
                b.iter(|| {
                    black_box(
                        run(black_box(&program), black_box(item))
                            .expect("evaluates"),
                    )
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("seek/view_direct", n),
            &item,
            |b, item| b.iter(|| black_box(mini::seek(black_box(item)))),
        );
        group.bench_with_input(
            BenchmarkId::new("seek/cbor_node", n),
            &original_bytes,
            |b, bytes| {
                let prog = seek_program();
                let slice: &[u8] = bytes;
                b.iter(|| black_box(sansho::evaluate(black_box(&prog), black_box(slice))))
            },
        );

        // ---- full-item shape ----
        // At 100k connections the full item is ~150k nodes and the
        // evaluation result is large; the full-item shape is measured
        // at the three smaller sizes (10 / 1k / 25k) where the result
        // stays within the bench's practical bounds.
        if n <= 25_000 {
            let program = full_item_program();
            group.bench_with_input(
                BenchmarkId::new("full/cursor_original", n),
                &original_bytes,
                |b, bytes| {
                    b.iter(|| {
                        black_box(
                            run(black_box(&program), black_box(bytes.as_slice()))
                                .expect("evaluates"),
                        )
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("full/json_tree", n),
                &item,
                |b, item| {
                    b.iter(|| {
                        black_box(
                            run(black_box(&program), black_box(item))
                                .expect("evaluates"),
                        )
                    })
                },
            );
        }
        group.finish();
    }
}

criterion_group!(benches, bench_paths);
criterion_main!(benches);
