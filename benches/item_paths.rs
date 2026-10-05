//! The real-Item benchmark: how fast evaluation works against the
//! three input shapes — raw CBOR binary, a real `Item`, and a
//! `serde_json::Value` in Item shape — through the trait surface
//! (`sansho::lookup_value` over the raw types: `&[u8]`, `&Item`,
//! `&Value`).
//!
//! The timed region is the full evaluation INCLUDING the emit-boundary
//! materialization (the walk's row boundary): the reference shape is
//! internal to the engine by design, so the measured form is `lookup`
//! collapsed at the boundary (`as_value`); the row count comes from
//! the materialized result.
//!
//! The three inputs carry the SAME logical content:
//! - `raw_cbor`: the item's CBOR bytes (`&[u8]` — the byte source);
//! - `item`: the real in-memory `Item` struct (`&Item` — via this
//!   module's trait implementations);
//! - `json_shape`: `serde_json::to_value(&item)` (`&Value`).
//!
//! Shapes: scan (`connections."alias:from"[?starts_with(@, 'pkg:')]`),
//! seek (`body.file_size`), full-item (`@`).

use bigtent::item::Item;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use std::collections::BTreeSet;

/// The row count of a materialized result (a projection's elements, or
/// a single value).
fn row_count(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Array(items) => items.len(),
        _ => 1,
    }
}

/// A REAL `Item` struct with `n_connections` alias targets (every other
/// one a pURL, the rest gitoids — the scan shape) and `n_file_names`
/// body entries.
fn real_item(n_connections: usize, n_file_names: usize) -> Item {
    let mut alias_from: BTreeSet<String> = BTreeSet::new();
    for i in 0..n_connections {
        if i % 2 == 0 {
            alias_from.insert(format!("pkg:maven/org.example/art{i}@1.0.{i}"));
        } else {
            alias_from.insert(format!("gitoid:blob:sha1:{i:040x}"));
        }
    }
    let file_names: Vec<String> = (0..n_file_names)
        .map(|i| {
            if i % 3 == 0 {
                format!("gitoid:blob:sha256:{i:064x}!$org/example/Thing{i}.java")
            } else {
                format!("org/example/Thing{i}.java")
            }
        })
        .collect();
    Item {
        identifier: format!("gitoid:blob:sha1:{}", "30e6".repeat(10)),
        connections: {
            let mut map = std::collections::BTreeMap::new();
            map.insert("alias:from".to_string(), alias_from);
            map.insert(
                "contained:up".to_string(),
                ["gitoid:blob:sha1:parent".to_string()].into_iter().collect(),
            );
            map
        },
        body_mime_type: Some("application/vnd.cc.goatrodeo".to_string()),
        body: Some(
            serde_cbor::value::to_value(serde_json::json!({
                "extra": {},
                "file_names": file_names,
                "file_size": 3050u64 + n_file_names as u64,
                "mime_type": ["text/x-java-source"]
            }))
            .expect("the JSON body converts to a CBOR value"),
        ),
    }
}

fn scan_program() -> sansho::Program {
    let parsed = sansho::parse("connections.\"alias:from\"[?starts_with(@, 'pkg:')]")
        .expect("parses");
    sansho::compile(&parsed).expect("compiles")
}

fn seek_program() -> sansho::Program {
    let parsed = sansho::parse("body.file_size").expect("parses");
    sansho::compile(&parsed).expect("compiles")
}

fn full_item_program() -> sansho::Program {
    let parsed = sansho::parse("@").expect("parses");
    sansho::compile(&parsed).expect("compiles")
}

fn bench_item_paths(c: &mut Criterion) {
    let sizes: [usize; 5] = [10, 1_000, 25_000, 100_000, 500_000];
    for n in sizes {
        let item = real_item(n, n / 2);
        let bytes = serde_cbor::to_vec(&item).expect("the real item serializes");
        let json = serde_json::to_value(&item).expect("the real item converts to JSON");
        let mut group = c.benchmark_group("item_paths");
        group.throughput(criterion::Throughput::Bytes(bytes.len() as u64));

        // ---- scan shape ----
        let program = scan_program();
        group.bench_with_input(
            BenchmarkId::new("scan/raw_cbor", n),
            &bytes,
            |b, bytes| {
                let slice: &[u8] = &bytes;
                b.iter(|| {
                    let value =
                        sansho::lookup_value(black_box(&slice), black_box(&program))
                            .expect("evaluates");
                    black_box(row_count(&value))
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("scan/item", n),
            &item,
            |b, item| {
                b.iter(|| {
                    let value =
                        sansho::lookup_value(black_box(item), black_box(&program))
                            .expect("evaluates");
                    black_box(row_count(&value))
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("scan/json_shape", n),
            &json,
            |b, json| {
                b.iter(|| {
                    let value =
                        sansho::lookup_value(black_box(json), black_box(&program))
                            .expect("evaluates");
                    black_box(row_count(&value))
                })
            },
        );

        // ---- seek shape ----
        let program = seek_program();
        group.bench_with_input(
            BenchmarkId::new("seek/raw_cbor", n),
            &bytes,
            |b, bytes| {
                let slice: &[u8] = &bytes;
                b.iter(|| {
                    let value =
                        sansho::lookup_value(black_box(&slice), black_box(&program))
                            .expect("evaluates");
                    black_box(row_count(&value))
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("seek/item", n),
            &item,
            |b, item| {
                b.iter(|| {
                    let value =
                        sansho::lookup_value(black_box(item), black_box(&program))
                            .expect("evaluates");
                    black_box(row_count(&value))
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("seek/json_shape", n),
            &json,
            |b, json| {
                b.iter(|| {
                    let value =
                        sansho::lookup_value(black_box(json), black_box(&program))
                            .expect("evaluates");
                    black_box(row_count(&value))
                })
            },
        );

        // ---- full-item shape ---- (at the three smaller sizes, where
        // the result stays within the bench's practical bounds)
        if n <= 25_000 {
            let program = full_item_program();
            group.bench_with_input(
                BenchmarkId::new("full/raw_cbor", n),
                &bytes,
                |b, bytes| {
                    let slice: &[u8] = &bytes;
                    b.iter(|| {
                        let value =
                            sansho::lookup_value(black_box(&slice), black_box(&program))
                                .expect("evaluates");
                        black_box(row_count(&value))
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("full/item", n),
                &item,
                |b, item| {
                    b.iter(|| {
                        let value =
                            sansho::lookup_value(black_box(item), black_box(&program))
                                .expect("evaluates");
                        black_box(row_count(&value))
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("full/json_shape", n),
                &json,
                |b, json| {
                    b.iter(|| {
                        let value =
                            sansho::lookup_value(black_box(json), black_box(&program))
                                .expect("evaluates");
                        black_box(row_count(&value))
                    })
                },
            );
        }
    }
}

criterion_group!(benches, bench_item_paths);
criterion_main!(benches);