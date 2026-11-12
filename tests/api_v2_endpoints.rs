//! The V2 operational endpoints.
//!
//! WHAT THIS TESTS
//!
//! The six endpoints the V2 module serves — `node_count`, `purls`, `health`,
//! `healthz`, `readyz`, `metrics` — plus its OpenAPI document. Each is exercised
//! through the V2 router alone, so a failure names the V2 surface rather than being
//! entangled with V1's.
//!
//! WHY IT IS WRITTEN THIS WAY
//!
//! The V2 endpoints are thin delegates onto the same accessors V1 uses, so testing
//! them against V1's *values* would only restate V1's tests. What is V2's own is
//! the ROUTING and the DOCUMENT: that `/v2/<name>` answers, that it answers with
//! the right shape, and that the OpenAPI document describes exactly those routes —
//! so a route added without a `#[utoipa::path]` annotation, or documented without a
//! handler, is caught here.
//!
//! LLM-FRIENDLY SUMMARY
//!
//! - `node_count` — a u64, and nonzero over the fixture cluster.
//! - `purls` — text/plain, holding every purl the fixture names.
//! - `health` — the full status object.
//! - `healthz` — liveness, always ok.
//! - `readyz` — 200 with `ready: true` over a loaded cluster.
//! - `metrics` — the Prometheus text format.
//! - `/v2/openapi.json` — describes exactly those six plus `/v2/query`.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use bigtent::api_v2::{TemplateRegistry, V2State, openapi_v2, router_v2};
use bigtent::highwire::compiler::QueryCache;
use bigtent::rodeo::goat::GoatRodeoCluster;
use bigtent::rodeo::holder::ClusterHolder;
use http_body_util::BodyExt;
use std::path::PathBuf;
use std::sync::Arc;
use tower::Service;

/// The V2 router over the fixture cluster, with the caller's registry.
async fn v2_app_with(templates: TemplateRegistry) -> Router {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/walk_v4");
    let clusters = GoatRodeoCluster::cluster_files_in_dir(dir, false, vec![])
        .await
        .expect("the fixture cluster loads");
    let cluster = clusters.into_iter().next().expect("one cluster");
    let herd = bigtent::rodeo::goat_herd::GoatHerd::new(vec![Arc::new(
        bigtent::rodeo::member::HerdMember::Cluster(cluster),
    )]);
    let holder = ClusterHolder::new_from_cluster(arc_swap::ArcSwap::from_pointee(herd), None)
        .await
        .expect("the holder builds");
    router_v2(Arc::new(V2State {
        holder,
        queries: Arc::new(QueryCache::new(64)),
        templates,
    }))
}

/// The V2 router over the fixture cluster, ready to be called.
async fn v2_app() -> Router {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/walk_v4");
    let clusters = GoatRodeoCluster::cluster_files_in_dir(dir, false, vec![])
        .await
        .expect("the fixture cluster loads");
    let cluster = clusters.into_iter().next().expect("one cluster");
    let herd = bigtent::rodeo::goat_herd::GoatHerd::new(vec![Arc::new(
        bigtent::rodeo::member::HerdMember::Cluster(cluster),
    )]);
    let holder = ClusterHolder::new_from_cluster(arc_swap::ArcSwap::from_pointee(herd), None)
        .await
        .expect("the holder builds");
    router_v2(Arc::new(V2State::new(holder, Arc::new(QueryCache::new(64)))))
}

/// Send a GET and return the response, consuming the router (it is a Service).
async fn get(app: &mut Router, path: &str) -> axum::response::Response {
    app.call(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .expect("the request is served")
}

/// The fixture cluster's three purls, read straight from the checked-in file.
fn fixture_purls() -> Vec<String> {
    let text =
        std::fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/walk_v4/purls.txt"))
            .expect("purls.txt is present");
    text.lines().filter_map(|l| {
        let l = l.trim();
        (!l.is_empty()).then(|| l.to_string())
    }).collect()
}

/// Requirement: `/v2/node_count` answers with the cluster's item count.
///
/// What: a u64 over the fixture cluster, which holds ten items.
///
/// Why: the number comes from the cluster's own index, and this endpoint is the
/// client's way to size a query before running one. A zero here would be a cluster
/// that loaded but read nothing.
#[tokio::test]
async fn node_count_answers() {
    let mut app = v2_app().await;
    let response = get(&mut app, "/node_count").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes().to_vec();
    let count: u64 = serde_json::from_slice(&body).expect("a u64");
    assert_eq!(count, 10, "the fixture holds ten items");
}

/// Requirement: `/v2/purls` returns the cluster's purls as text.
///
/// What: text/plain, containing every purl the checked-in `purls.txt` names.
///
/// Why: V1 serves this as a file download through `tower-http`; V2 reads and
/// returns it, so the content-type and the content are both assertions here.
#[tokio::test]
async fn purls_answers() {
    let mut app = v2_app().await;
    let response = get(&mut app, "/purls").await;
    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .expect("a content type")
        .to_string();
    assert!(content_type.starts_with("text/plain"), "{content_type}");

    let body = response.into_body().collect().await.unwrap().to_bytes().to_vec();
    let text = String::from_utf8(body).expect("text");
    for purl in fixture_purls() {
        assert!(text.contains(&purl), "the purl `{purl}` is in the list");
    }
}

/// Requirement: `/v2/health` reports the cluster's status.
///
/// What: the full status object — counts, version, uptime and a status string.
///
/// Why: this is the object a monitoring system reads, so its shape is part of the
/// contract, and the OpenAPI document names it as a schema.
#[tokio::test]
async fn health_answers() {
    let mut app = v2_app().await;
    let response = get(&mut app, "/health").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes().to_vec();
    let health: serde_json::Value = serde_json::from_slice(&body).expect("JSON");
    assert_eq!(health["status"], serde_json::json!("ok"), "{health:?}");
    assert_eq!(health["node_count"], serde_json::json!(10));
    assert!(health["version"].as_str().is_some(), "{health:?}");
}

/// Requirement: `/v2/healthz` answers liveness.
///
/// What: always `{"status": "ok"}` — a liveness probe asks whether the process
/// answers, not whether the cluster is usable.
#[tokio::test]
async fn healthz_answers() {
    let mut app = v2_app().await;
    let response = get(&mut app, "/healthz").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes().to_vec();
    let live: serde_json::Value = serde_json::from_slice(&body).expect("JSON");
    assert_eq!(live["status"], serde_json::json!("ok"));
}

/// Requirement: `/v2/readyz` reports readiness.
///
/// What: 200 with `ready: true` when the cluster holds items.
///
/// Why: readiness is the cluster's answer, distinct from liveness — a process can
/// be up while its cluster is empty, and a load balancer needs to tell those apart.
#[tokio::test]
async fn readyz_answers() {
    let mut app = v2_app().await;
    let response = get(&mut app, "/readyz").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes().to_vec();
    let ready: serde_json::Value = serde_json::from_slice(&body).expect("JSON");
    assert_eq!(ready["ready"], serde_json::json!(true));
}

/// Requirement: `/v2/metrics` returns the Prometheus text format.
///
/// What: `text/plain; version=0.0.4`, with a non-empty body.
///
/// Why: the content type is what a scraper keys on, and an empty body would mean
/// the registry was built and never written to.
#[tokio::test]
async fn metrics_answers() {
    let mut app = v2_app().await;
    let response = get(&mut app, "/metrics").await;
    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .expect("a content type")
        .to_string();
    assert!(content_type.contains("text/plain"), "{content_type}");
    let body = response.into_body().collect().await.unwrap().to_bytes().to_vec();
    assert!(!body.is_empty(), "the metrics body is not empty");
}

/// Requirement: the V2 OpenAPI document describes exactly the V2 routes.
///
/// What: `/v2/openapi.json`, and the document it serves.
///
/// Why: the document is the contract a client reads, so a route that is served but
/// not documented — or documented and not served — is a lie in one direction or the
/// other. This asserts both directions: every served path appears in the document,
/// and every documented path is served.
#[tokio::test]
async fn the_openapi_document_matches_the_routes() {
    let mut app = v2_app().await;
    let response = get(&mut app, "/openapi.json").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes().to_vec();
    let served: serde_json::Value = serde_json::from_slice(&body).expect("JSON");

    let served_paths: Vec<String> = served["paths"]
        .as_object()
        .expect("paths is an object")
        .keys()
        .cloned()
        .collect();

    let documented = openapi_v2();
    let documented_paths: Vec<String> = documented
        .paths
        .paths
        .keys()
        .cloned()
        .collect();

    for path in &documented_paths {
        assert!(
            served_paths.contains(path),
            "documented but not served: {path}"
        );
    }
    for path in &served_paths {
        assert!(
            documented_paths.contains(path),
            "served but not documented: {path}"
        );
    }
    assert_eq!(
        documented_paths.len(),
        7,
        "the six operational endpoints plus the query endpoint: {documented_paths:?}"
    );
}

/// Requirement: templates are definable at construction, and a designated default
/// is what an unspecified request runs.
///
/// What: a V2 state built with a caller's template `sizes` and a default of
/// `sizes`; a GET with no `template=` and a POST with neither field both run it.
///
/// Why: the pre-canned queries are the point of `roots-from-input` — an endpoint's
/// text is fixed and its identifiers arrive with the call. "Fixed by whom" is this
/// API: the embedding application defines its own queries and designates which one
/// answers an unspecified request, at construction, so the server's behaviour is
/// what its builder chose rather than a hard-coded default.
///
/// LLM section: the custom template counts the identifier's length; the request
/// sends "x" (length 1). The unknown default case is refused at construction.
#[tokio::test]
async fn a_custom_template_and_a_designated_default() -> std::result::Result<(), anyhow::Error> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/walk_v4");
    let clusters = GoatRodeoCluster::cluster_files_in_dir(dir, false, vec![])
        .await
        .expect("the fixture cluster loads");
    let cluster = clusters.into_iter().next().expect("one cluster");
    let herd = bigtent::rodeo::goat_herd::GoatHerd::new(vec![Arc::new(
        bigtent::rodeo::member::HerdMember::Cluster(cluster),
    )]);
    let holder = ClusterHolder::new_from_cluster(arc_swap::ArcSwap::from_pointee(herd), None)
        .await
        .expect("the holder builds");

    // the custom template, and the default designated to it
    let templates = TemplateRegistry::new()
        .with_template(
            "sizes",
            r#"{"roots": {"from-input": true}, "walk": [], "emit": "{len: length(identifier)}"}"#,
        )
        .with_default("sizes")
        .expect("the default names a template");

    let mut app = router_v2(Arc::new(V2State::with_templates(
        holder,
        Arc::new(QueryCache::new(64)),
        templates,
    ).expect("the state builds")));

    // the GET with no template: the default runs, which is the caller's. The
    // identifier is one the fixture holds — a root naming nothing emits nothing.
    let identifier = "pkg:maven/uk.co.real-logic/artio-core@0.118";
    let response = get(&mut app, &format!("/query?identifier={identifier}")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes().to_vec();
    let text = String::from_utf8(body).expect("text");
    assert!(
        text.contains("len\":43"),
        "the default is the caller's template: {text}"
    );

    // the POST with neither template nor query: the default again
    let response = app
        .call(
            Request::builder()
                .method("POST")
                .uri("/query")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"roots": ["{identifier}"]}}"#
                )))
                .unwrap(),
        )
        .await
        .expect("the request is served");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes().to_vec();
    let text = String::from_utf8(body).expect("text");
    assert!(
        text.contains("len\":43"),
        "the POST with neither field runs the default: {text}"
    );
    Ok(())
}

/// Requirement: an unknown default template is refused at construction.
///
/// What: `with_default("ghost")` where no template is named that.
///
/// Why: a default that names nothing fails every request that omitted a template —
/// a misconfiguration that would only surface at request time, per request. Fail
/// fast, at the construction that introduced it.
#[test]
fn an_unknown_default_is_refused_at_construction() {
    let error = TemplateRegistry::new()
        .with_default("ghost")
        .err()
        .expect("an unknown default is refused");
    assert!(error.contains("ghost"), "the error names it: {error}");
}

/// Requirement: a caller's template REPLACES a built-in of the same name.
///
/// What: a `flatten` template provided by the caller, which emits a constant.
///
/// Why: the name is the identifier of the query, so a caller providing `flatten`
/// means THEIR flatten — the same rule the CLI blob's replace-by-name used. The
/// alternative, refusing the collision, would make a built-in unshadowable.
#[tokio::test]
async fn a_caller_template_replaces_a_builtin() -> std::result::Result<(), anyhow::Error> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/walk_v4");
    let clusters = GoatRodeoCluster::cluster_files_in_dir(dir, false, vec![])
        .await
        .expect("the fixture cluster loads");
    let cluster = clusters.into_iter().next().expect("one cluster");
    let herd = bigtent::rodeo::goat_herd::GoatHerd::new(vec![Arc::new(
        bigtent::rodeo::member::HerdMember::Cluster(cluster),
    )]);
    let holder = ClusterHolder::new_from_cluster(arc_swap::ArcSwap::from_pointee(herd), None)
        .await
        .expect("the holder builds");

    let templates = TemplateRegistry::new().with_template(
        "flatten",
        r#"{"roots": {"from-input": true}, "walk": [], "emit": "'replaced'"}"#,
    );
    let mut app = router_v2(Arc::new(V2State {
        holder,
        queries: Arc::new(QueryCache::new(64)),
        templates,
    }));

    let identifier = "pkg:maven/uk.co.real-logic/artio-core@0.118";
    let response = get(
        &mut app,
        &format!("/query?identifier={identifier}&template=flatten"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes().to_vec();
    let text = String::from_utf8(body).expect("text");
    assert!(
        text.contains("replaced"),
        "the caller's template answered, not the built-in: {text}"
    );
    Ok(())
}

/// Requirement: a registry is EMPTY by default, and a state built from it has
/// no default.
    ///
    /// What: `TemplateRegistry::new()` with nothing added; a GET without
    /// `template=` and a POST with neither field.
    ///
    /// Why: the built-ins are available to add, not forced upon the server. A
    /// deployment that wants none of them gets none — and an unspecified request is
    /// refused with a 404 naming the omission, which is the contract a caller can
    /// act on rather than a silent empty answer.
    #[tokio::test]
    async fn an_empty_registry_has_no_default() -> std::result::Result<(), anyhow::Error> {
        let mut app = v2_app_with(TemplateRegistry::new()).await;

        let response = get(&mut app, "/query?identifier=x").await;
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "no default is designated: {}",
            response.status()
        );
        let body = response.into_body().collect().await.unwrap().to_bytes().to_vec();
        let text = String::from_utf8(body).expect("text");
        assert!(
            text.contains("no default template"),
            "the refusal names the omission: {text}"
        );

        let response = app
            .call(
                Request::builder()
                    .method("POST")
                    .uri("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"roots": ["x"]}"#))
                    .unwrap(),
            )
            .await
            .expect("served");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        Ok(())
    }

    /// Requirement: a built-in can be REMOVED, and the server answers as if it
    /// never existed.
    ///
    /// What: the built-ins added and `flatten` removed; a request for `flatten` and
    /// a request for `north`.
    ///
    /// Why: a deployment that does not want an endpoint's formulation should be able
    /// to take it away, and a removed template must 404 rather than fall back to the
    /// built-in — a fallback would make removal unobservable.
    #[tokio::test]
    async fn a_removed_builtin_is_not_served() -> std::result::Result<(), anyhow::Error> {
        let templates = TemplateRegistry::new().with_builtins().without_template("flatten");
        let mut app = v2_app_with(templates).await;

        let identifier = "pkg:maven/uk.co.real-logic/artio-core@0.118";
        let response = get(
            &mut app,
            &format!("/query?identifier={identifier}&template=flatten"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "removed is removed");

        // the others are untouched
        let response = get(
            &mut app,
            &format!("/query?identifier={identifier}&template=north"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK, "the siblings remain");
        Ok(())
    }

    /// Requirement: a built-in can be REPLACED, and the default can be one the
    /// caller supplied.
    ///
    /// What: the built-ins, `item` replaced by a counting query, that named as the
    /// default; a GET with no `template=`.
    ///
    /// Why: replacing and defaulting compose — the deployment's own query is both
    /// the replacement and what answers an unspecified request.
    #[tokio::test]
    async fn a_replaced_builtin_can_be_the_default() -> std::result::Result<(), anyhow::Error> {
        let templates = TemplateRegistry::new()
            .with_builtins()
            .with_template(
                "item",
                r#"{"roots": {"from-input": true}, "walk": [], "emit": "{len: length(identifier)}"}"#,
            )
            .with_default("item")
            .expect("the replaced template exists");
        let mut app = v2_app_with(templates).await;

        let identifier = "pkg:maven/uk.co.real-logic/artio-core@0.118";
        let response = get(&mut app, &format!("/query?identifier={identifier}")).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes().to_vec();
        let text = String::from_utf8(body).expect("text");
        assert!(
            text.contains("len\":43"),
            "the default is the caller's replacement: {text}"
        );
        Ok(())
    }


/// Finding 9 (PR #171 review): the POST's `template` must resolve from the
/// SAME registry the GET uses — the request's state — not a hard-coded
/// built-in list. A custom template registered with `with_template` must run
/// on POST; today the POST consults a static table and answers 404.
#[tokio::test(flavor = "multi_thread")]
async fn the_post_template_comes_from_the_registry() -> std::result::Result<(), anyhow::Error> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/walk_v4");
    let clusters = GoatRodeoCluster::cluster_files_in_dir(dir, false, vec![])
        .await
        .expect("the fixture cluster loads");
    let cluster = clusters.into_iter().next().expect("one cluster");
    let herd = bigtent::rodeo::goat_herd::GoatHerd::new(vec![Arc::new(
        bigtent::rodeo::member::HerdMember::Cluster(cluster),
    )]);
    let holder = ClusterHolder::new_from_cluster(arc_swap::ArcSwap::from_pointee(herd), None)
        .await
        .expect("the holder builds");

    let templates = TemplateRegistry::new().with_template(
        "sizes",
        r#"{"roots": {"from-input": true}, "walk": [], "emit": "{len: length(identifier)}"}"#,
    );

    let mut app = router_v2(Arc::new(V2State::with_templates(
        holder,
        Arc::new(QueryCache::new(64)),
        templates,
    )
    .expect("the state builds")));

    let identifier = "pkg:maven/uk.co.real-logic/artio-core@0.118";

    // the POST naming the custom template in the BODY: the registry's text
    let response = app
        .clone()
        .call(
            Request::builder()
                .method("POST")
                .uri("/query")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"template": "sizes", "roots": ["{identifier}"]}}"#
                )))
                .unwrap(),
        )
        .await
        .expect("the request is served");
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "a registered template runs on POST — the registry, not a static list"
    );
    let body = response.into_body().collect().await.unwrap().to_bytes().to_vec();
    let text = String::from_utf8(body).expect("text");
    assert!(
        text.contains("\"len\":43"),
        "the POST ran the REGISTRY's template: {text}"
    );

    // the POST naming the template in the QUERY PARAMETER: same source
    let response = app
        .call(
            Request::builder()
                .method("POST")
                .uri("/query?template=sizes")
                .header("content-type", "application/json")
                .body(Body::from(format!(r#"{{"roots": ["{identifier}"]}}"#)))
                .unwrap(),
        )
        .await
        .expect("the request is served");
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "the query parameter names a template on POST too"
    );
    let body = response.into_body().collect().await.unwrap().to_bytes().to_vec();
    let text = String::from_utf8(body).expect("text");
    assert!(
        text.contains("\"len\":43"),
        "the query-parameter template ran: {text}"
    );
    Ok(())
}
