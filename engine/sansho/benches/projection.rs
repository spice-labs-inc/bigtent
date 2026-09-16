//! The projection benchmarks (SPEC-0001 §2's motivation made measured):
//! selective evaluation over the CBOR byte slice versus the
//! full-materialization baseline, on the Appendix-A-shaped item class
//! scaled up to real sizes (thousands of file_names entries).
//!
//! Run: `cargo bench -p sansho` (criterion; the results land under
//! target/criterion/). The GATE lives in the test-suite
//! (`tests/phase5_gates.rs`'s benchmark gate) — the thresholds:
//! the prefix-filter expression materializes at most one tenth of the
//! nodes the full path materializes, and is never slower by more than
//! fifteen percent than the baseline on trivial projections.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use sansho::{compile, evaluate_cbor, evaluate_json, parse};

/// The Appendix-A-shaped item, scaled: `file_names` entries with the
/// gitoid-prefixed merge-disambiguation form mixed among plain paths.
fn scaled_item(entries: usize) -> serde_json::Value {
    let file_names: Vec<String> = (0..entries)
        .map(|i| {
            if i % 3 == 0 {
                format!(
                    "gitoid:blob:sha256:{:064x}!$org/apache/logging/log4j/core/lookup/JndiLookup.java",
                    i
                )
            } else {
                format!("logging-log4j2-log4j-2.10.0/log4j-core/src/main/java/org/apache/logging/log4j/core/lookup/JndiLookup.java{i}")
            }
        })
        .collect();
    serde_json::json!({
        "identifier": "gitoid:blob:sha1:30e65ad24f4b4d799e52cfd70fcbebc0490b7343",
        "connections": [["alias:from", "md5:4d921bf0ce238d1a96bdf65000fde565"]],
        "body_mime_type": "application/vnd.cc.goatrodeo",
        "body": {
            "extra": {},
            "file_names": file_names,
            "file_size": 3050,
            "mime_type": ["text/x-java-source"]
        }
    })
}

fn bench_projection(criterion: &mut Criterion) {
    for entries in [1_000, 4_000] {
        let document = scaled_item(entries);
        let bytes = serde_cbor::to_vec(&document).unwrap();

        let prefix = compile(&parse("body.file_names[?starts_with(@, 'gitoid:')]").unwrap())
            .expect("compiles");
        let full = compile(&parse("@").unwrap()).expect("compiles");
        let none = compile(&parse("body.extra").unwrap()).expect("compiles");

        let mut group = criterion.benchmark_group("projection");
        group.throughput(criterion::Throughput::Bytes(bytes.len() as u64));

        group.bench_with_input(
            format!("prefix_filter/cursor/{entries}_entries"),
            &bytes,
            |bench, bytes| bench.iter(|| evaluate_cbor(black_box(&prefix), black_box(bytes))),
        );
        group.bench_with_input(
            format!("prefix_filter/materialized/{entries}_entries"),
            &document,
            |bench, doc| bench.iter(|| evaluate_json(black_box(&prefix), black_box(doc))),
        );
        group.bench_with_input(
            format!("full_item/cursor/{entries}_entries"),
            &bytes,
            |bench, bytes| bench.iter(|| evaluate_cbor(black_box(&full), black_box(bytes))),
        );
        group.bench_with_input(
            format!("no_selection/cursor/{entries}_entries"),
            &bytes,
            |bench, bytes| bench.iter(|| evaluate_cbor(black_box(&none), black_box(bytes))),
        );
        group.finish();
    }
}

criterion_group!(benches, bench_projection);
criterion_main!(benches);
