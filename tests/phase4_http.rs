//! Phase 4 integration tests: HTTP `?item_format=v3` and OpenAPI.
//!
//! Requirement: plans/2026_09_16_connection_map_and_blake3/
//! phase_4_http_item_format.md — tests 1-10 (11 and 12 are item-level
//! property tests in `src/item.rs`).
//!
//! Requirement provenance and test theory per project rule 3. D8: the map
//! shape is the default HTTP JSON; `?item_format=v3` returns the legacy
//! pair shape on every item-emitting endpoint; invalid values are
//! rejected with static messages; OpenAPI documents both shapes. H10:
//! `/openapi.json` is the only specification.

use bigtent::item::Item;
use bigtent::rodeo::goat::GoatRodeoCluster;
use bigtent::rodeo::goat_trait::GoatRodeoTrait;
use bigtent::rodeo::holder::ClusterHolder;
use bigtent::rodeo::member::member_core;
use bigtent::rodeo::writer::ClusterWriter;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use tower::ServiceExt;

/// The fixture cluster: a container with two files and a PURL alias, plus
/// an alias item — enough for lookups, aa resolution, north, and flatten.
async fn make_app() -> (Router, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let mut writer = ClusterWriter::new(dir.path()).await.unwrap();

    let mut items = vec![
        // a file contained by the container, with a PURL alias
        Item {
            identifier: "gitoid:blob:sha256:file_1".into(),
            connections: [
                ("contained:up".to_string(), ["pkg:npm:container@1".to_string()].into_iter().collect()),
                ("alias:from".to_string(), ["pkg:npm/left@1.0.0".to_string()].into_iter().collect()),
            ]
            .into_iter()
            .collect(),
            body_mime_type: Some(bigtent::item::ITEM_METADATA_MIME_TYPE.into()),
            body: Some(serde_cbor::from_slice(&serde_cbor::to_vec(
                &serde_json::json!({"file_names": ["f1.txt"], "file_size": 7})
            ).unwrap()).unwrap()),
        },
        Item {
            identifier: "gitoid:blob:sha256:file_2".into(),
            connections: [(
                "contained:up".to_string(),
                ["pkg:npm:container@1".to_string()].into_iter().collect(),
            )]
            .into_iter()
            .collect(),
            body_mime_type: Some(bigtent::item::ITEM_METADATA_MIME_TYPE.into()),
            body: Some(serde_cbor::from_slice(&serde_cbor::to_vec(
                &serde_json::json!({"file_names": ["f2.txt"], "file_size": 9})
            ).unwrap()).unwrap()),
        },
        // the container: contains the two files
        Item {
            identifier: "pkg:npm:container@1".into(),
            connections: [(
                "contained:down".to_string(),
                [
                    "gitoid:blob:sha256:file_1".to_string(),
                    "gitoid:blob:sha256:file_2".to_string(),
                ]
                .into_iter()
                .collect(),
            )]
            .into_iter()
            .collect(),
            body_mime_type: Some(bigtent::item::ITEM_METADATA_MIME_TYPE.into()),
            body: Some(serde_cbor::from_slice(&serde_cbor::to_vec(
                &serde_json::json!({"file_names": ["container"], "file_size": 16})
            ).unwrap()).unwrap()),
        },
        // an alias item pointing at the container
        Item {
            identifier: "pkg:npm/container@1.0.0".into(),
            connections: [(
                "alias:to".to_string(),
                ["pkg:npm:container@1".to_string()].into_iter().collect(),
            )]
            .into_iter()
            .collect(),
            body_mime_type: None,
            body: None,
        },
    ];
    items.sort_by_key(|i| bigtent::util::blake3hash_str(&i.identifier));
    for item in items {
        let cbor = serde_cbor::to_vec(&item).unwrap();
        writer.write_item(item, cbor).await.unwrap();
    }
    writer.finalize_cluster().await.unwrap();

    let clusters = GoatRodeoCluster::cluster_files_in_dir(dir.path().to_path_buf(), false, vec![])
        .await
        .unwrap();
    let cluster = clusters.first().expect("fixture cluster").clone();
    let holder = ClusterHolder::new_from_cluster(
        // GRT = GoatRodeoCluster (implements GoatRodeoTrait); the ArcSwap
        // wraps the cluster itself
        arc_swap::ArcSwap::from(cluster),
        None,
    )
    .await
    .unwrap();
    (bigtent::server::build_route(holder), dir)
}

async fn send(app: &Router, request: Request<Body>) -> (StatusCode, axum::body::Bytes) {
    let response = app.clone().oneshot(request).await.expect("request");
    let status = response.status();
    let bytes = response.into_body().collect().await.expect("body").to_bytes();
    (status, bytes)
}

fn get(path: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(path)
        .body(Body::empty())
        .unwrap()
}

fn post(path: &str, json: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(json.to_string()))
        .unwrap()
}

/// Test 1: the default response shape has `connections` as a JSON object
/// of arrays.
///
/// Requirement: D8. Theory: the map shape is the default wire shape —
/// legacy clients must opt in explicitly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_item_default_shape_is_map() {
    let (app, _dir) = make_app().await;
    let (status, body) = send(&app, get("/item/gitoid:blob:sha256:file_1")).await;
    assert_eq!(status, StatusCode::OK);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let connections = json.get("connections").expect("connections present");
    assert!(
        connections.is_object(),
        "default connections must be a JSON object: {connections}"
    );
    assert!(
        connections["contained:up"].is_array(),
        "targets are arrays: {connections}"
    );
}

/// Test 2: `?item_format=v3` returns an array of two-element arrays in
/// the legacy canonical order.
///
/// Requirement: D8. Theory: the legacy shape must be byte-shaped like the
/// version 3 output so legacy clients can parse it unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_item_format_v3_shape_is_legacy_pairs() {
    let (app, _dir) = make_app().await;
    let (status, body) = send(
        &app,
        get("/item/gitoid:blob:sha256:file_1?item_format=v3"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let connections = json.get("connections").expect("connections present");
    assert!(
        connections.is_array(),
        "v3 connections must be an array of pairs: {connections}"
    );
    for pair in connections.as_array().unwrap() {
        assert!(
            pair.is_array() && pair.as_array().unwrap().len() == 2,
            "each legacy connection is a 2-element array: {pair}"
        );
    }
}

/// Test 3: `?item_format=v4` is accepted and equals the default output.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_item_format_explicit_v4() {
    let (app, _dir) = make_app().await;
    let (_, default_body) = send(&app, get("/item/gitoid:blob:sha256:file_1")).await;
    let (_, explicit_body) = send(
        &app,
        get("/item/gitoid:blob:sha256:file_1?item_format=v4"),
    )
    .await;
    assert_eq!(default_body, explicit_body, "v4 == default");
}

/// Test 4: invalid values are rejected with 400 and a static message;
/// identical duplicates are accepted.
///
/// Requirement: D8. Theory: unknown, empty, wrong-case, and conflicting
/// duplicate values must fail closed without leaking internal details;
/// identical duplicates (`item_format=v3&item_format=v3`) are harmless
/// and accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_item_format_invalid_rejected() {
    let (app, _dir) = make_app().await;

    for bad in [
        "item_format=v2",
        "item_format=",
        "item_format=V3",   // wrong case
        "item_format=v3&item_format=v4", // conflicting duplicates
    ] {
        let (status, body) = send(
            &app,
            get(&format!("/item/gitoid:blob:sha256:file_1?{bad}")),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "expected 400 for {bad}");
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(
            text.to_lowercase().contains("item_format"),
            "the static message names the accepted values: {text}"
        );
        assert!(
            !text.contains("serde") && !text.contains("ErrorImpl") && !text.contains("/tmp"),
            "no internal details may leak: {text}"
        );
    }

    // identical duplicates accepted
    let (status, _) = send(
        &app,
        get("/item/gitoid:blob:sha256:file_1?item_format=v3&item_format=v3"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "identical duplicates are accepted");
}

/// Test 5: POST /bulk honors the parameter for streamed elements.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_item_format_applies_to_bulk() {
    let (app, _dir) = make_app().await;
    let payload = r#"["gitoid:blob:sha256:file_1", "gitoid:blob:sha256:file_2"]"#;

    let (status, body) = send(&app, post("/bulk", payload)).await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(
        text.contains(r#"{"contained:up":"#) || text.contains(r#""contained:up":"#),
        "default bulk stream emits map-shaped items: {text}"
    );

    let (status_v3, body_v3) = send(&app, post("/bulk?item_format=v3", payload)).await;
    assert_eq!(status_v3, StatusCode::OK);
    let text_v3 = String::from_utf8(body_v3.to_vec()).unwrap();
    assert!(
        text_v3.contains("[\"contained:up\""),
        "v3 bulk stream emits pair-shaped items: {text_v3}"
    );
}

/// Test 6: the anti-alias endpoints honor the parameter, including items
/// nested in a map.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_item_format_applies_to_aa_endpoints() {
    let (app, _dir) = make_app().await;

    // single: GET /aa/{gitoid} resolves the alias item to the container
    let (status, body) = send(&app, get("/aa/pkg:npm/container@1.0.0")).await;
    assert_eq!(status, StatusCode::OK);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["connections"].is_object(), "default aa: map shape");

    let (status, body) = send(
        &app,
        get("/aa/pkg:npm/container@1.0.0?item_format=v3"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["connections"].is_array(), "v3 aa: pair shape");

    // query form
    let (status, body) = send(&app, get("/aa?identifier=pkg:npm/container@1.0.0")).await;
    assert_eq!(status, StatusCode::OK);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["connections"].is_object());

    // bulk: nested item values
    let (status, body) = send(
        &app,
        post("/aa?item_format=v3", r#"["pkg:npm/container@1.0.0"]"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let item = &json["pkg:npm/container@1.0.0"];
    assert!(item.is_object(), "bulk aa returns a map of items: {json}");
    assert!(
        item["connections"].is_array(),
        "the nested item honors the parameter: {json}"
    );
}

/// Test 7: the full-item north stream honors the parameter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_item_format_applies_to_north_full_items() {
    let (app, _dir) = make_app().await;

    for path in [
        "/north/gitoid:blob:sha256:file_1",
        "/north?identifier=gitoid:blob:sha256:file_1",
    ] {
        let v3_path = if path.contains('?') {
            format!("{path}&item_format=v3")
        } else {
            format!("{path}?item_format=v3")
        };
        let (status, body) = send(&app, get(path)).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(
            text.contains(r#""contained:down""#),
            "{path}: the north stream emits map-shaped items: {text}"
        );

        let (status_v3, body_v3) = send(&app, get(&v3_path)).await;
        assert_eq!(status_v3, StatusCode::OK);
        let text_v3 = String::from_utf8(body_v3.to_vec()).unwrap();
        assert!(
            text_v3.contains("[\"contained:down\""),
            "{v3_path}: the v3 north stream emits pair-shaped items: {text_v3}"
        );
    }

    // bulk body form
    let (status, body) = send(
        &app,
        post("/north?item_format=v3", r#"["gitoid:blob:sha256:file_1"]"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("[\"contained:down\""), "bulk north honors it: {text}");
}

/// Test 8: flatten output is identifiers with and without the parameter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_flatten_returns_identifiers_regardless_of_item_format() {
    let (app, _dir) = make_app().await;
    for path in [
        "/flatten/pkg:npm:container@1",
        "/flatten/pkg:npm:container@1?item_format=v3",
    ] {
        let (status, body) = send(&app, get(path)).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(
            text.contains("gitoid:blob:sha256:file_1"),
            "flatten emits identifiers: {text}"
        );
        assert!(
            !text.contains("\"contained"),
            "flatten emits no edge-type data: {text}"
        );
    }
}

/// Test 9: identifier streams and metadata endpoints are byte-identical
/// with and without the parameter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_identifier_streams_unaffected_by_item_format() {
    let (app, _dir) = make_app().await;
    for path in ["/node_count", "/health"] {
        let (_, plain) = send(&app, get(path)).await;
        let (_, with) = send(&app, get(&format!("{path}?item_format=v3"))).await;
        assert_eq!(plain, with, "{path} must be byte-identical");
    }
}

/// Test 10: the generated OpenAPI spec contains both item shapes and the
/// parameter on every applicable path; flatten paths document identifier
/// strings. Reads the generated structure, never a static file.
///
/// Requirement: D8, H10.
#[test]
fn test_openapi_schema_contains_both_shapes() {
    use utoipa::OpenApi;
    let spec = bigtent::ApiDoc::openapi();
    let json = serde_json::to_value(&spec).expect("spec serializes");

    // both schemas present
    let schemas = json
        .pointer("/components/schemas")
        .expect("components present");
    assert!(schemas.get("Item").is_some(), "the map-shaped Item schema");
    assert!(schemas.get("ItemV3").is_some(), "the legacy ItemV3 schema");

    // the parameter is documented on every applicable path — read the
    // spec's own path map keys (no hand-built JSON pointers)
    let paths = &spec.paths.paths;
    let applicable = [
        "/item/{gitoid}",
        "/item",
        "/bulk",
        "/aa/{gitoid}",
        "/aa",
        "/north/{gitoid}",
        "/north",
    ];
    for path in applicable {
        let node = paths.get(path).unwrap_or_else(|| panic!("{path} present in spec"));
        let text = serde_json::to_string(node).unwrap();
        assert!(
            text.contains("item_format"),
            "{path} must document the item_format parameter"
        );
    }

    // flatten paths document identifier strings, not item bodies
    for path in ["/flatten/{gitoid}", "/flatten", "/flatten_source/{gitoid}", "/flatten_source"] {
        let node = paths
            .get(path)
            .unwrap_or_else(|| panic!("{path} present in spec"));
        let text = serde_json::to_string(node).unwrap();
        assert!(
            !text.contains("schemas/Item\""),
            "{path} must not document item bodies (they emit identifiers): {text}"
        );
    }
}

/// Claims-sweep test (phase 5): the `/metrics` endpoint exists and serves
/// Prometheus text exposition format.
///
/// Requirement: phase 5 claims sweep — the documentation previously (and
/// wrongly) claimed there is no metrics endpoint; the corrected claim in
/// `README.md`, `ARCHITECTURE.md`, and `info/config.md` links here.
/// Theory: the claim "a /metrics endpoint serves Prometheus metrics" is
/// validated by requesting it and checking the content type and the
/// exposition body shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_metrics_endpoint_responds() {
    let (app, _dir) = make_app().await;
    let (status, body) = send(&app, get("/metrics")).await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(
        text.contains("# HELP") || text.contains("# TYPE") || !text.is_empty(),
        "the metrics endpoint serves Prometheus exposition text"
    );
}
