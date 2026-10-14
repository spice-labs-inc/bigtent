//! The source-trait surface benchmarks: the bare [`SanshoTrait`] entry
//! (`sansho::evaluate` alone, over each raw source type) measured
//! against the whole public path (`run`: evaluate, then materialize at
//! the boundary) on the same documents and programs.
//!
//! The point of the delta: the trait surface must not cost anything —
//! one evaluator, monomorphized per source. If the bare entry shows up
//! in these numbers, the abstraction has a cost and the design is wrong.
//!
//! Run: `cargo bench -p sansho --bench trait_surface`.

use criterion::{black_box, criterion_group, criterion_main, Criterion};

/// The whole public path: evaluate the program, then materialize the result at
/// the boundary. This is what a consumer pays for one query, so it is what
/// these benchmarks measure.
fn run<'a, T: sansho::SanshoTrait<'a>>(
    program: &sansho::Program,
    document: T,
) -> Result<serde_json::Value, sansho::SanshoError> {
    sansho::evaluate(program, document).and_then(|value| sansho::materialize(&value))
}

/// The Appendix-A-shaped document (the scan shape's subject), served
/// three ways: raw CBOR bytes, an in-memory JSON value, and an
/// in-memory CBOR value (the mixed-structure case the walk will hit).
fn fixtures() -> (Vec<u8>, serde_json::Value, serde_cbor::Value) {
    let item = serde_json::json!({
        "identifier": "gitoid:blob:sha1:30e65ad24f4b4d799e52cfd70fcbebc0490b7343",
        "connections": [["alias:from", "md5:4d921bf0ce238d1a96bdf65000fde565"]],
        "body_mime_type": "application/vnd.cc.goatrodeo",
        "body": {
            "extra": {},
            "file_names": (0..1000)
                .map(|i| {
                    if i % 3 == 0 {
                        format!("gitoid:blob:sha256:{:064x}!$org/app/Thing{i}.java", i)
                    } else {
                        format!("logging-log4j2/log4j-core/src/main/java/org/apache/logging/log4j/core/lookup/JndiLookup.java{i}")
                    }
                })
                .collect::<Vec<_>>(),
            "file_size": 3050u64,
            "mime_type": ["text/x-java-source"]
        }
    });
    let bytes = serde_cbor::to_vec(&item).expect("the fixture encodes");
    let value = serde_cbor::value::to_value(item.clone()).expect("the fixture converts");
    (bytes, item, value)
}

/// The scan shape: the prefix-filter over file_names (the measure of
/// selective materialization; a thousand entries).
fn scan_program() -> std::sync::Arc<sansho::Program> {
    let parsed = sansho::parse("body.file_names[?starts_with(@, 'gitoid:')]").expect("parses");
    sansho::compile(&parsed, None).expect("compiles")
}

/// The seek shape: single-field access.
fn seek_program() -> std::sync::Arc<sansho::Program> {
    let parsed = sansho::parse("body.file_size").expect("parses");
    sansho::compile(&parsed, None).expect("compiles")
}

fn trait_surface_benchmarks(c: &mut Criterion) {
    let (bytes, json, cbor_value) = fixtures();
    let scan = scan_program();
    let seek = seek_program();

    // the byte source: the pre-existing entry vs the trait entry
    let mut group = c.benchmark_group("byte_source_entry");
    group.throughput(criterion::Throughput::Elements(1));
    group.bench_function("materialized_scan", |b| {
        b.iter(|| run(black_box(&scan), black_box(bytes.as_slice())).unwrap())
    });
    group.bench_function("lookup_scan", |b| {
        let slice: &[u8] = &bytes;
        b.iter(|| sansho::evaluate(black_box(&scan), black_box(slice)).unwrap())
    });
    group.bench_function("materialized_seek", |b| {
        b.iter(|| run(black_box(&seek), black_box(bytes.as_slice())).unwrap())
    });
    group.bench_function("lookup_seek", |b| {
        let slice: &[u8] = &bytes;
        b.iter(|| sansho::evaluate(black_box(&seek), black_box(slice)).unwrap())
    });
    group.finish();

    // the JSON source: the pre-existing entry vs the trait entry
    let mut group = c.benchmark_group("json_source_entry");
    group.throughput(criterion::Throughput::Elements(1));
    group.bench_function("materialized_scan", |b| {
        b.iter(|| run(black_box(&scan), black_box(&json)).unwrap())
    });
    group.bench_function("lookup_scan", |b| {
        b.iter(|| sansho::evaluate(black_box(&scan), black_box(&json)).unwrap())
    });
    group.bench_function("lookup_seek", |b| {
        b.iter(|| sansho::evaluate(black_box(&seek), black_box(&json)).unwrap())
    });
    group.finish();

    // the in-memory CBOR value source (the trait surface's new source)
    let mut group = c.benchmark_group("cbor_value_source_entry");
    group.throughput(criterion::Throughput::Elements(1));
    group.bench_function("lookup_scan", |b| {
        b.iter(|| sansho::evaluate(black_box(&scan), black_box(&cbor_value)).unwrap())
    });
    group.bench_function("lookup_seek", |b| {
        b.iter(|| sansho::evaluate(black_box(&seek), black_box(&cbor_value)).unwrap())
    });
    group.bench_function("root_probe_kind", |b| {
        // the trait is implemented for `&serde_cbor::Value`, so the receiver
        // is a reference to the reference
        let reference: &serde_cbor::Value = &cbor_value;
        b.iter(|| {
            black_box(sansho::view::SanshoTrait::kind(&reference));
        })
    });
    group.finish();
}

criterion_group!(benches, trait_surface_benchmarks);
criterion_main!(benches);
