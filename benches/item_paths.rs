//! The real-Item benchmark: how fast the pure zero-copy, zero-`Value`
//! evaluation works against the three input shapes — raw CBOR binary,
//! a real `Item`, and a `serde_json::Value` in Item shape.
//!
//! Every arm evaluates to the reference-shaped intermediate
//! representation ([`sansho::Flow`]) via `evaluate_flow` — NOTHING is
//! materialized to `serde_json::Value` inside the timed region: the
//! scan counts its reference rows (a `Proj` of `One` references) without
//! emitting; seek and full-item yield a single reference position.
//! The three inputs carry the SAME logical content:
//! - `raw_cbor`: the item's CBOR bytes, walked by `CborNode`;
//! - `item_node`: the real in-memory `Item` struct, walked by `ItemNode`;
//! - `json_shape`: `serde_json::to_value(&item)`, walked by
//!   `MaterializedNode` (the value source).
//!
//! Shapes: scan (`connections."alias:from"[?starts_with(@, 'pkg:')]`),
//! seek (`body.file_size`), full-item (`@`).

use bigtent::item::{Connections, Item};
use bigtent::sansho_seam::ItemNode;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use sansho::materialized::MaterializedNode;
use sansho::source::CborNode;
use std::collections::BTreeSet;

/// Count the reference rows in a flow without materializing anything
/// (a projection's elements, or a single position/scalar).
fn flow_row_count<N>(flow: &sansho::Flow<N>) -> usize {
    match flow {
        sansho::Flow::Proj(items) => items.len(),
        sansho::Flow::One(_) | sansho::Flow::Value(_) => 1,
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
        connections: Connections::from_iter(
            [("alias:from".to_string(), alias_from)]
                .into_iter()
                .flat_map(|(edge, targets)| {
                    targets.into_iter().map(move |t| (edge.clone(), t))
                })
                .chain([("contained:up".to_string(), "gitoid:blob:sha1:parent".to_string())]),
        ),
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
                b.iter(|| {
                    let root = CborNode::root(black_box(bytes));
                    let flow =
                        sansho::eval::evaluate_flow(black_box(&program), &root).expect("evaluates");
                    black_box(flow_row_count(&flow))
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("scan/item_node", n),
            &item,
            |b, item| {
                b.iter(|| {
                    let node = ItemNode::wrap(black_box(item));
                    let flow =
                        sansho::eval::evaluate_flow(black_box(&program), &node).expect("evaluates");
                    black_box(flow_row_count(&flow))
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("scan/json_shape", n),
            &json,
            |b, json| {
                b.iter(|| {
                    let root = MaterializedNode::root(black_box(json));
                    let flow =
                        sansho::eval::evaluate_flow(black_box(&program), &root).expect("evaluates");
                    black_box(flow_row_count(&flow))
                })
            },
        );

        // ---- seek shape ----
        let program = seek_program();
        group.bench_with_input(
            BenchmarkId::new("seek/raw_cbor", n),
            &bytes,
            |b, bytes| {
                b.iter(|| {
                    let root = CborNode::root(black_box(bytes));
                    let flow =
                        sansho::eval::evaluate_flow(black_box(&program), &root).expect("evaluates");
                    black_box(flow_row_count(&flow))
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("seek/item_node", n),
            &item,
            |b, item| {
                b.iter(|| {
                    let node = ItemNode::wrap(black_box(item));
                    let flow =
                        sansho::eval::evaluate_flow(black_box(&program), &node).expect("evaluates");
                    black_box(flow_row_count(&flow))
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("seek/json_shape", n),
            &json,
            |b, json| {
                b.iter(|| {
                    let root = MaterializedNode::root(black_box(json));
                    let flow =
                        sansho::eval::evaluate_flow(black_box(&program), &root).expect("evaluates");
                    black_box(flow_row_count(&flow))
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
                    b.iter(|| {
                        let root = CborNode::root(black_box(bytes));
                        let flow =
                            sansho::eval::evaluate_flow(black_box(&program), &root)
                                .expect("evaluates");
                        black_box(flow_row_count(&flow))
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("full/item_node", n),
                &item,
                |b, item| {
                    b.iter(|| {
                        let node = ItemNode::wrap(black_box(item));
                        let flow =
                            sansho::eval::evaluate_flow(black_box(&program), &node)
                                .expect("evaluates");
                        black_box(flow_row_count(&flow))
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("full/json_shape", n),
                &json,
                |b, json| {
                    b.iter(|| {
                        let root = MaterializedNode::root(black_box(json));
                        let flow =
                            sansho::eval::evaluate_flow(black_box(&program), &root)
                                .expect("evaluates");
                        black_box(flow_row_count(&flow))
                    })
                },
            );
        }
        group.finish();
    }
}

criterion_group!(benches, bench_item_paths);
criterion_main!(benches);