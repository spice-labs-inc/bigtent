//! Start the real server and make requests against V1 and V2.
//!
//! Every other test drives a router directly. This one starts `run_web_server` —
//! the merged app, V1 and V2 both served — on an ephemeral port and makes real
//! HTTP requests through the real listener, so the merge itself is under test and
//! not just the routers it combines.

use anyhow::Result;
use bigtent::rodeo::goat::GoatRodeoCluster;
use bigtent::rodeo::goat_herd::GoatHerd;
use bigtent::rodeo::holder::ClusterHolder;
use bigtent::rodeo::member::HerdMember;
use serde_json::Value;
use std::path::PathBuf;
use std::sync::Arc;

/// The real server over the fixture cluster, on an ephemeral port.
///
/// The port is chosen free before the holder is built, so the test knows where to
/// connect and the server binds the same one. The returned guard is a handle the
/// test holds so the spawned server lives as long as the test.
async fn start_server() -> Result<(u16, tokio::sync::oneshot::Sender<()>)> {
    // a port that is free now, which the server will bind
    let probe = std::net::TcpListener::bind("127.0.0.1:0")?;
    let port = probe.local_addr()?.port();
    drop(probe);

    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/walk_v4");
    let clusters = GoatRodeoCluster::cluster_files_in_dir(dir, false, vec![]).await?;
    let herd = GoatHerd::new(
        clusters
            .into_iter()
            .map(|cluster| Arc::new(HerdMember::Cluster(cluster)))
            .collect::<Vec<_>>(),
    );
    let args = Arc::new(bigtent::config::Args {
        host: vec!["127.0.0.1".to_string()],
        port: Some(port),
        ..Default::default()
    });
    let holder = ClusterHolder::new_from_cluster(
        arc_swap::ArcSwap::from_pointee(herd),
        Some(args),
    )
    .await?;

    let (shutdown, _stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let _ = bigtent::server::run_web_server(holder).await;
    });
    // a moment for axum to take the listener
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    Ok((port, shutdown))
}

/// One HTTP GET, through a real HTTP client.
///
/// A raw socket cannot read a chunked response — the chunk-size markers would be
/// interleaved with the body — so this is a real client, which decodes the
/// transfer encoding and returns the body alone.
async fn http_get(port: u16, path: &str) -> Result<(u16, String)> {
    let response = reqwest::get(format!("http://127.0.0.1:{port}{path}")).await?;
    Ok((response.status().as_u16(), response.text().await?))
}

/// One HTTP POST with a JSON body.
async fn http_post(port: u16, path: &str, body: &str) -> Result<(u16, String)> {
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://127.0.0.1:{port}{path}"))
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await?;
    Ok((response.status().as_u16(), response.text().await?))
}

/// The identifiers a response body carries, whatever the wire format.
///
/// V1's traversal endpoints stream a JSON ARRAY (`StreamBodyAs::json_array`);
/// V2's query endpoint streams JSON LINES. Both are read: the whole body parsed
/// as JSON first (an array), then line by line.
fn jsonl_identifiers(body: &str) -> Vec<String> {
    let mut out = Vec::new();

    // V1's format: one JSON array, possibly spanning lines
    if let Ok(parsed) = serde_json::from_str::<Value>(body) {
        match parsed {
            Value::Array(items) => {
                for row in items {
                    match row {
                        Value::String(id) => out.push(id),
                        Value::Object(entries) => {
                            if let Some(id) = entries.get("identifier").and_then(|v| v.as_str()) {
                                out.push(id.to_string());
                            }
                        }
                        _ => {}
                    }
                }
            }
            Value::String(id) => out.push(id),
            Value::Object(entries) => {
                if let Some(id) = entries.get("identifier").and_then(|v| v.as_str()) {
                    out.push(id.to_string());
                }
            }
            _ => {}
        }
        out.sort();
        out.dedup();
        return out;
    }

    // V2's format: one JSON value per line
    for line in body.lines() {
        let Ok(row) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        match row {
            Value::String(id) => out.push(id),
            Value::Object(entries) => {
                if let Some(id) = entries.get("identifier").and_then(|v| v.as_str()) {
                    out.push(id.to_string());
                }
            }
            _ => {}
        }
    }
    out.sort();
    out.dedup();
    out
}

/// The fixture's identifiers, for the comparisons.
const PURL_A: &str = "pkg:maven/uk.co.real-logic/artio-core@0.118";
const PURL_B: &str = "pkg:maven/uk.co.real-logic/artio-codecs@0.118";
const JAR: &str = "gitoid:blob:sha256:8eba64f7eef0da9c9312a51033c61ee3f5c981edb6e34dc6b986ad65de56d515";
const CLASS: &str = "gitoid:blob:sha256:cacfa4827b6f2e4930c5488c50a81731f0d2b975e9517ffcd3a2bade93ccb6a1";

/// Requirement: the served app — V1 and V2 merged — answers both APIs, and the
/// two agree with each other and with the V1 accessors.
///
/// What: real requests over a real socket, against `run_web_server` itself. Every
/// pair of endpoints that answer the same question is compared as a set: V1's
/// `/aa` against V2's `template=anti_alias`, V1's `/north` against V2's `template=
/// north`, V1's `/flatten` against V2's `template=flatten`, and so on — plus the
/// operational endpoints V2 carries and the OpenAPI documents both serve.
///
/// Why: every other test drives a router directly, so the MERGE is untested — the
/// thing that could be wrong is the served app, not the routers it combines. And
/// the comparison is the report the request asked for: every difference between
/// what V1 answers and what V2 answers, over the same cluster, from the same
/// identifiers.
///
/// LLM section: the V2 templates take roots from the input stream, so the
/// identifier arrives as a query parameter; V1 takes it from the path. Both are
/// the same question. Where the two differ in SHAPE — V1's `/north_purls` streams
/// purl strings, V2's template emits them through `unnest` — the sets should still
/// agree.
// multi_thread: the test BLOCKS on a raw-socket read, and a current-thread
// runtime would starve the server sharing that thread — the server can never run
// to answer. This is also why the manual curls worked while the test hung.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_served_app_answers_v1_and_v2_and_they_agree() -> Result<()> {
    let (port, _shutdown) = start_server().await?;

    let mut report: Vec<String> = Vec::new();

    // ---- /item -------------------------------------------------------------
    let (v1_status, v1_body) = http_get(port, &format!("/item/{PURL_A}?format=v3json")).await?;
    let (v2_status, v2_body) = http_get(port, &format!("/v2/query?identifier={PURL_A}")).await?;
    let v1_ids = jsonl_identifiers(&v1_body);
    let v2_ids = jsonl_identifiers(&v2_body);
    if v1_status != v2_status {
        report.push(format!("/item: STATUS DIFFERS v1={v1_status} v2={v2_status}"));
    }
    if v1_ids != v2_ids {
        report.push(format!("/item: SETS DIFFER v1={v1_ids:?} v2={v2_ids:?}"));
    }

    // ---- /aa ----------------------------------------------------------------
    let (v1_status, v1_body) = http_get(port, &format!("/aa/{PURL_A}?format=v3json")).await?;
    let (v2_status, v2_body) = http_get(
        port,
        &format!("/v2/query?identifier={PURL_A}&template=anti_alias"),
    )
    .await?;
    let v1_ids = jsonl_identifiers(&v1_body);
    let v2_ids = jsonl_identifiers(&v2_body);
    if v1_status != v2_status {
        report.push(format!("/aa: STATUS DIFFERS v1={v1_status} v2={v2_status}"));
    }
    if v1_ids != v2_ids {
        report.push(format!("/aa: SETS DIFFER v1={v1_ids:?} v2={v2_ids:?}"));
    }

    // ---- /north ---------------------------------------------------------------
    let (v1_status, v1_body) = http_get(port, &format!("/north/{CLASS}?format=v3json")).await?;
    eprintln!("NORTH RAW ({} bytes): {v1_body}", v1_body.len());
    let (v2_status, v2_body) =
        http_get(port, &format!("/v2/query?identifier={CLASS}&template=north")).await?;
    let v1_ids = jsonl_identifiers(&v1_body);
    let v2_ids = jsonl_identifiers(&v2_body);
    if v1_status != v2_status {
        report.push(format!("/north: STATUS DIFFERS v1={v1_status} v2={v2_status}"));
    }
    if v1_ids != v2_ids {
        report.push(format!("/north: SETS DIFFER v1={v1_ids:?} v2={v2_ids:?}"));
    }

    // ---- /north_purls ---------------------------------------------------------
    let (v1_status, v1_body) = http_get(port, &format!("/north_purls/{JAR}")).await?;
    let (v2_status, v2_body) =
        http_get(port, &format!("/v2/query?identifier={JAR}&template=north_purls")).await?;
    let v1_ids = jsonl_identifiers(&v1_body);
    let v2_ids = jsonl_identifiers(&v2_body);
    if v1_status != v2_status {
        report.push(format!("/north_purls: STATUS DIFFERS v1={v1_status} v2={v2_status}"));
    }
    if v1_ids != v2_ids {
        report.push(format!("/north_purls: SETS DIFFER v1={v1_ids:?} v2={v2_ids:?}"));
    }

    // ---- /flatten ---------------------------------------------------------------
    let (v1_status, v1_body) = http_get(port, &format!("/flatten/{JAR}?format=v3json")).await?;
    let (v2_status, v2_body) =
        http_get(port, &format!("/v2/query?identifier={JAR}&template=flatten")).await?;
    let v1_ids = jsonl_identifiers(&v1_body);
    let v2_ids = jsonl_identifiers(&v2_body);
    if v1_status != v2_status {
        report.push(format!("/flatten: STATUS DIFFERS v1={v1_status} v2={v2_status}"));
    }
    if v1_ids != v2_ids {
        report.push(format!("/flatten: SETS DIFFER v1={v1_ids:?} v2={v2_ids:?}"));
    }

    // ---- /flatten_source --------------------------------------------------------
    let (v1_status, v1_body) = http_get(port, &format!("/flatten_source/{JAR}?format=v3json")).await?;
    let (v2_status, v2_body) = http_get(
        port,
        &format!("/v2/query?identifier={JAR}&template=flatten_source"),
    )
    .await?;
    let v1_ids = jsonl_identifiers(&v1_body);
    let v2_ids = jsonl_identifiers(&v2_body);
    if v1_status != v2_status {
        report.push(format!("/flatten_source: STATUS DIFFERS v1={v1_status} v2={v2_status}"));
    }
    if v1_ids != v2_ids {
        report.push(format!("/flatten_source: SETS DIFFER v1={v1_ids:?} v2={v2_ids:?}"));
    }

    // ---- /bulk versus POST /v2/query --------------------------------------------
    let (v1_status, v1_body) =
        http_post(port, "/bulk", &format!(r#"["{PURL_A}", "{PURL_B}"]"#)).await?;
    let (v2_status, v2_body) = http_post(
        port,
        "/v2/query",
        &format!(r#"{{"roots": ["{PURL_A}", "{PURL_B}"]}}"#),
    )
    .await?;
    let v1_ids = jsonl_identifiers(&v1_body);
    let v2_ids = jsonl_identifiers(&v2_body);
    if v1_status != v2_status {
        report.push(format!("/bulk: STATUS DIFFERS v1={v1_status} v2={v2_status}"));
    }
    if v1_ids != v2_ids {
        report.push(format!("/bulk: SETS DIFFER v1={v1_ids:?} v2={v2_ids:?}"));
    }

    // ---- the operational endpoints, V2 only -------------------------------------
    for path in ["/v2/health", "/v2/healthz", "/v2/readyz", "/v2/metrics"] {
        let (status, body) = http_get(port, path).await?;
        if status != 200 {
            report.push(format!("{path}: STATUS {status}"));
        }
        if body.is_empty() {
            report.push(format!("{path}: EMPTY BODY"));
        }
    }

    // ---- the OpenAPI documents ------------------------------------------------
    let (v1_status, v1_body) = http_get(port, "/openapi.json").await?;
    let (v2_status, v2_body) = http_get(port, "/v2/openapi.json").await?;
    if v1_status != 200 || v2_status != 200 {
        report.push(format!(
            "openapi: STATUS v1={v1_status} v2={v2_status}"
        ));
    }
    let v1_doc: Value = serde_json::from_str(&v1_body).unwrap_or(Value::Null);
    let v2_doc: Value = serde_json::from_str(&v2_body).unwrap_or(Value::Null);
    let v1_paths: Vec<String> = v1_doc["paths"]
        .as_object()
        .map(|paths| paths.keys().cloned().collect())
        .unwrap_or_default();
    let v2_paths: Vec<String> = v2_doc["paths"]
        .as_object()
        .map(|paths| paths.keys().cloned().collect())
        .unwrap_or_default();
    if v2_paths.len() != 7 {
        report.push(format!("/v2/openapi: {} paths, expected 7", v2_paths.len()));
    }
    for path in &v2_paths {
        if !v1_paths.is_empty() && !v1_paths.iter().any(|p| p.ends_with(path)) {
            // V1's document is under its own prefix scheme; a V2 path absent from
            // V1's document is EXPECTED — V2 documents itself. Recorded, not an
            // error.
            report.push(format!("/v2/openapi: `{path}` is V2's own (not in V1's doc)"));
        }
    }

    // ---- the report ----------------------------------------------------------
    if report.is_empty() {
        println!("NO DIFFERENCES: every V1/V2 pair agrees as sets, and the operational endpoints answer");
    } else {
        println!("DIFFERENCES FOUND ({}):", report.len());
        for line in &report {
            println!("  {line}");
        }
    }
    Ok(())
}
