//! The pipeline-comparison benchmarks (the owner's request): large
//! serialized-CBOR documents at four sizes — turn the CBOR into a serde
//! intermediate representation and query it with the `jmespath` crate
//! versus running Sansho (both its direct cursor path and its
//! materialized backend, to isolate what the cursor buys).
//!
//! The outputs of all paths are the SAME — asserted before the timing
//! (the `path_outputs_agree` test). The interest is the relative timing.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use sansho::{compile, evaluate_cbor, evaluate_json, parse};
use serde_json::Value as J;

/// The document shape: the Appendix-A-like item (file_names-heavy, the
/// gitoid-prefixed merge-disambiguation entries mixed among plain
/// paths), scaled to a TARGET SERIALIZED-CBOR size.
// NOTE: the equality-test lives in tests/comparison_equality.rs (the
// bench-targets do not run #[test]s); the generator is duplicated there.

fn scaled_item_to_size(target_bytes: usize) -> (J, Vec<u8>, usize) {
    // the approximate per-entry contribution (~130 bytes of CBOR per
    // entry at this shape) — the entry-count that lands near the target
    let entry_estimate = (target_bytes / 130).max(1);
    let mut entries = entry_estimate;
    let mut bytes;
    // refine once (the estimate is close; one correction suffices)
    for _ in 0..2 {
        let document = build_item(entries);
        bytes = serde_cbor::to_vec(&document).unwrap();
        if bytes.len() >= target_bytes * 95 / 100 && bytes.len() <= target_bytes * 105 / 100 {
            return (document, bytes, entries);
        }
        entries = entries * target_bytes / bytes.len().max(1);
    }
    let document = build_item(entries);
    bytes = serde_cbor::to_vec(&document).unwrap();
    (document, bytes, entries)
}

fn build_item(entries: usize) -> J {
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
        "connections": [
            ["alias:from", "md5:4d921bf0ce238d1a96bdf65000fde565"],
            ["alias:from", "sha1:e9f38efe19210f3b63a72699a69eb261176d251e"]
        ],
        "body_mime_type": "application/vnd.cc.goatrodeo",
        "body": {
            "extra": {},
            "file_names": file_names,
            "file_size": 3050,
            "mime_type": ["text/x-java-source"]
        }
    })
}

/// The comparison's three paths, per expression:
/// - `materialized_jmespath`: the CBOR → the serde intermediate
///   representation (serde_cbor → serde_json::Value) → the `jmespath`
///   crate (the real-world third-party JMESPath engine);
/// - `materialized_sansho`: the CBOR → the serde intermediate
///   representation → Sansho's materialized backend;
/// - `sansho_cursor`: the CBOR byte slice → Sansho's cursor directly
///   (no intermediate representation).


fn bench_comparison(criterion: &mut Criterion) {
    for target in [10_000, 100_000, 1_000_000, 10_000_000] {
        let (document, bytes, entries) = scaled_item_to_size(target);
        let jmespath_variable = jmespath::Variable::from_serializable(&document).unwrap();
        let size_label = format!("{}/{} entries/{} bytes", target, entries, bytes.len());

        let mut group = criterion.benchmark_group("cbor_pipeline_comparison");
        group.throughput(Throughput::Bytes(bytes.len() as u64));

        for (expression, slug) in [
            ("body.file_names[?starts_with(@, 'gitoid:')]", "prefix_filter"),
            ("length(body.file_names)", "length_fn"),
            ("identifier", "identifier"),
        ] {
            let jmespath_expression = jmespath::compile(expression).unwrap();
            let program = compile(&parse(expression).unwrap()).unwrap();

            group.bench_with_input(
                BenchmarkId::new(
                    format!("materialized_jmespath/{slug}"),
                    &size_label,
                ),
                &(bytes.as_slice(), &jmespath_variable, &jmespath_expression),
                |bench, (bytes, variable, expression)| {
                    // path A INCLUDES the materialization: the bytes →
                    // the serde intermediate → the query (the pipeline
                    // the owner described)
                    bench.iter(|| {
                        let parsed =
                            serde_cbor::from_slice::<J>(black_box(bytes)).unwrap();
                        let _ = &parsed;
                        black_box(
                            expression
                                .search(black_box(variable))
                                .expect("evaluates"),
                        )
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new(format!("materialized_sansho/{slug}"), &size_label),
                &(bytes.as_slice(), &program),
                |bench, (bytes, program)| {
                    bench.iter(|| {
                        let parsed =
                            serde_cbor::from_slice::<J>(black_box(bytes)).unwrap();
                        black_box(evaluate_json(program, &parsed).expect("evaluates"))
                    });
                },
            );

            group.bench_with_input(
                BenchmarkId::new(format!("sansho_cursor/{slug}"), &size_label),
                &(bytes.as_slice(), &program),
                |bench, (bytes, program)| {
                    bench.iter(|| {
                        black_box(evaluate_cbor(program, black_box(bytes)).expect("evaluates"))
                    });
                },
            );
        }
        group.finish();
    }
}

criterion_group!(benches, bench_comparison);
criterion_main!(benches);
