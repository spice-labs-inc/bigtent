//! The V2 API: a separate module with its own router, served alongside V1.
//!
//! V2 exists so the HTTP surface can grow without touching the V1 handlers: this
//! module owns its routes, its OpenAPI document, and its naming, and the server
//! merges its router with V1's. The V1 surface is unchanged, and both are served
//! by default, V2 under the `v2` prefix.
//!
//! # The query endpoint
//!
//! `GET /v2/query?identifier=…&template=<name>` and
//! `POST /v2/query {"roots": [...], "template": name}` run a pre-canned query —
//! the same documents that replicate the V1 traversal endpoints, served with the
//! identifiers arriving on the query's input stream rather than rewritten into the
//! text. `POST /v2/query {"roots": [...], "query": {...}}` accepts a full query
//! document inline, compiled per request and NOT cached, because an uncached
//! document is unbounded in size and a cache keyed on it is a memory leak the
//! caller controls.
//!
//! With neither `template` nor `query`, the basic lookup runs: every root's item,
//! no traversal — which is `/bulk`'s shape.
//!
//! The output is pure JSON lines: one row per line, a stop's report marked with a
//! `stop` key, a failure's with an `error` key. There is no authentication; these
//! endpoints run arbitrary traversal and arbitrary Starlark against the cluster,
//! and exposing them is a trust decision for whoever serves them.
//!
//! Everything is lazy and bounded: the response streams from the pipeline's output
//! channel as the stages produce, and every channel in the pipeline is bounded, so
//! a slow client backpressures the query rather than buffering it.

use axum::extract::{Query as AxumQuery, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Json, Router, routing::{get, post}};
use crate::highwire::compiler::{CompiledQuery, QueryCache};
use crate::highwire::grammar::GrammarError;
use crate::highwire::walk::{AccountingAndBudget, HerdOrCluster, SinkOperation};
use crate::rodeo::goat_trait::GoatRodeoTrait;
use crate::rodeo::goat_herd::GoatHerd;
use crate::rodeo::holder::ClusterHolder;
use serde::Deserialize;
use utoipa::OpenApi;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// The state V2's routes share: the cluster, and the query cache.
///
/// The cache is built once, at server start, so its expression programs live for
/// the process and every request that serves a pre-canned document re-parses the
/// document but compiles nothing.
pub struct V2State<GRT: GoatRodeoTrait + 'static> {
    pub holder: Arc<ClusterHolder<GRT>>,
    pub queries: Arc<QueryCache>,
    /// The pre-canned queries, with the default designated.
    pub templates: TemplateRegistry,
}

impl<GRT: GoatRodeoTrait + 'static> V2State<GRT> {
    /// The state with the built-in templates and the passthrough default.
    pub fn new(holder: Arc<ClusterHolder<GRT>>, queries: Arc<QueryCache>) -> Self {
        let templates = TemplateRegistry::new()
            .with_builtins()
            .with_default("item")
            .expect("the built-in default names a built-in template");
        V2State {
            holder,
            queries,
            templates,
        }
    }

    /// The state with caller-provided templates and a designated default.
    ///
    /// An unknown default is an error here, at construction, so a misconfigured
    /// server fails to start rather than failing every request.
    pub fn with_templates(
        holder: Arc<ClusterHolder<GRT>>,
        queries: Arc<QueryCache>,
        templates: TemplateRegistry,
    ) -> Result<Self, String> {
        match &templates.default {
            Some(name) if templates.get(name).is_none() => {
                return Err(format!(
                    "the default template `{name}` is not a known template"
                ));
            }
            _ => {}
        }
        Ok(V2State {
            holder,
            queries,
            templates,
        })
    }
}

/// The accountant V2 uses: it never stops a walk and counts nothing.
///
/// The accountant's real form is part of the finished product's budget work; until
/// that lands, a run from the HTTP surface is unbounded in rows and in work, which
/// is acceptable for the same reason V1's traversals are.
pub struct NoBudget;

impl AccountingAndBudget for NoBudget {
    fn should_stop(&self) -> Option<Vec<Value>> {
        None
    }

    fn can_sink_normal(&self) -> bool {
        true
    }

    fn event(&self, _event_type: crate::highwire::walk::AccountingEventType) {}
}

/// The cluster status behind `/v2/health`.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct HealthResponse {
    node_count: u64,
    cluster_count: u64,
    last_reload_at: Option<String>,
    uptime_seconds: u64,
    version: String,
    git_sha: String,
    status: String,
}

/// The liveness answer behind `/v2/healthz`.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct LivezResponse {
    status: String,
}

/// The readiness answer behind `/v2/readyz`.
#[derive(serde::Serialize, utoipa::ToSchema)]
pub struct ReadyzResponse {
    ready: bool,
}

// ------------------------------------------------------- the pre-canned templates

/// The passthrough query: every root's item, no traversal.
// the full item IS the row: `@`, always — an enumeration drifts when the
// item's shape gains a field
const PASSTHROUGH: &str = r#"{"roots": {"from-input": true}, "walk": [], "emit": "@"}"#;

/// The pre-canned queries V2 serves by name.
///
/// A registry is EMPTY by default — the built-ins are available to add, not forced
/// upon the server. The caller constructing the state decides which templates
/// exist:
///
/// - [`TemplateRegistry::new`] starts empty, with no default;
/// - [`TemplateRegistry::with_builtins`] adds the six that replicate the V1
///   endpoints plus the passthrough;
/// - [`TemplateRegistry::with_template`] adds one, or replaces one already present;
/// - [`TemplateRegistry::without_template`] removes one — including a built-in;
/// - [`TemplateRegistry::with_default`] designates the default, which must name a
///   template the registry holds — an unknown default is refused at construction,
///   fail-fast, because every request that omitted a template would otherwise 404.
pub struct TemplateRegistry {
    templates: HashMap<String, String>,
    default: Option<String>,
}

impl Default for TemplateRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TemplateRegistry {
    /// An empty registry, with no default.
    ///
    /// A state built from this answers nothing until templates are added: a GET
    /// without `template=` and a POST with neither field fail, because there is no
    /// default to run.
    pub fn new() -> Self {
        TemplateRegistry {
            templates: HashMap::new(),
            default: None,
        }
    }

    /// Add the built-in templates: the six that replicate the V1 endpoints plus
    /// the passthrough lookup. The default is NOT designated — do that explicitly,
    /// with [`TemplateRegistry::with_default`].
    pub fn with_builtins(mut self) -> Self {
        for (name, text) in TEMPLATES {
            self.templates.insert(name.to_string(), text.to_string());
        }
        self
    }

    /// Add a template, or replace one already present.
    pub fn with_template(mut self, name: &str, text: &str) -> Self {
        self.templates.insert(name.to_string(), text.to_string());
        self
    }

    /// Remove a template.
    ///
    /// If it was the default, there is no default any more, and an unspecified
    /// request fails until a new one is designated.
    pub fn without_template(mut self, name: &str) -> Self {
        if self.default.as_deref() == Some(name) {
            self.default = None;
        }
        self.templates.remove(name);
        self
    }

    /// Designate the default template, by name.
    ///
    /// An unknown name is an error HERE rather than at request time: a default that
    /// names nothing fails at startup, where it can be fixed, rather than failing
    /// every request that omitted a template.
    pub fn with_default(mut self, name: &str) -> Result<Self, String> {
        if !self.templates.contains_key(name) {
            return Err(format!(
                "the default template `{name}` is not a known template"
            ));
        }
        self.default = Some(name.to_string());
        Ok(self)
    }

    /// A template's text by name, or `None` for an unknown one.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.templates.get(name).map(String::as_str)
    }

    /// The default template's text, or `None` when none is designated.
    pub fn default_template(&self) -> Option<&str> {
        self.default.as_deref().and_then(|name| self.get(name))
    }
}

/// The templates V2 serves by name. Each is a full query document whose roots come
/// from the input stream, so the text is fixed and the identifiers arrive with the
/// call — the same relationship `/item/{gitoid}` has to `/bulk`.
const TEMPLATES: &[(&str, &str)] = &[
    ("item", PASSTHROUGH),
    (
        "anti_alias",
        r#"{"roots": {"from-input": true}, "walk": ["alias:to"], "emit":
            {"expr": "@", "if": {"==": {"is_terminal": true}}}}"#,
    ),
    (
        "north",
        // V1's north emits the FULL items of the BFS (Either::Left(Item)) —
        // the same shape /item emits; the traversal's own dedup (the walk's
        // `seen` set) matches V1's `processed` set, so no dedup stage.
        r#"{"roots": {"from-input": true}, "walk": ["alias:to", "tag:from", "contained:up"],
            "emit": "@"}"#,
    ),
    (
        "north_purls",
        // V1's rule, expressed in the query: each visited item's alias:from
        // values, filtered IN THE EXPRESSION to pkg:-prefixed targets — no
        // gate — and a DEDUP stage after the walk: two different items can
        // carry the same purl (a diamond), and V1's `found_purls` set emits
        // it once globally. An item without the edge yields null; a filter
        // projection over null is null; a null row is dropped by the emit's
        // null rule.
        r#"{"phases": [
            {"name": "walk", "roots": {"from-input": true},
             "walk": ["alias:to", "tag:from", "contained:up"],
             "emit": {"expr": "connections.\"alias:from\"[?starts_with(@, 'pkg:')]", "unnest": true}},
            {"name": "unique", "barker": "for row in input:\n    emit(row)",
             "dedup": true, "inputs": ["walk"]}
        ]}"#,
    ),
    (
        "flatten",
        // V1's rule: each visited item's contains:down TARGET STRINGS are
        // emitted (the alias:to hop is navigation, never emitted; the root's
        // own targets included — no depth gate); a null edge projects to
        // null and drops. Dedup: V1's processed set emits a target once.
        r#"{"phases": [
            {"name": "walk", "roots": {"from-input": true},
             "walk": ["alias:to", "contained:down"],
             "emit": {"expr": "connections.\"contained:down\"[*]", "unnest": true}},
            {"name": "unique", "barker": "for row in input:\n    emit(row)",
             "dedup": true, "inputs": ["walk"]}
        ]}"#,
    ),
    (
        "flatten_source",
        // V1's source=true: the traversal ALSO follows build:down edges, and
        // BOTH kinds of targets emit only when the EMITTING item carries a
        // build:down edge (the gate is null-safe: no edge is an empty list).
        // The edge name is build:down — the old template's build:from is not
        // an edge type at all.
        r#"{"phases": [
            {"name": "walk", "roots": {"from-input": true},
             "walk": ["alias:to", "contained:down", "build:down"],
             "emit": [
                {"expr": "connections.\"contained:down\"[*]", "unnest": true,
                 "if": "length(connections.\"build:down\" || `[]`) > `0`"},
                {"expr": "connections.\"build:down\"[*]", "unnest": true,
                 "if": "length(connections.\"build:down\" || `[]`) > `0`"}
             ]},
            {"name": "unique", "barker": "for row in input:\n    emit(row)",
             "dedup": true, "inputs": ["walk"]}
        ]}"#,
    ),
    (
        "north_purls",
        // V1's rule, expressed in the query: each visited item's alias:from
        // values, filtered IN THE EXPRESSION to pkg:-prefixed targets — no
        // gate — and a DEDUP stage after the walk: two different items can
        // carry the same purl (a diamond), and V1's `found_purls` set emits
        // it once globally. An item without the edge yields null; a filter
        // projection over null is null; a null row is dropped by the emit's
        // null rule.
        r#"{"phases": [
            {"name": "walk", "roots": {"from-input": true},
             "walk": ["alias:to", "tag:from", "contained:up"],
             "emit": {"expr": "connections.\"alias:from\"[?starts_with(@, 'pkg:')]", "unnest": true}},
            {"name": "unique", "barker": "for row in input:\n    emit(row)",
             "dedup": true, "inputs": ["walk"]}
        ]}"#,
    ),
    (
        "flatten",
        // the accessor emits the TARGETS of the edges it follows, never the seed —
        // so the depth gate excludes the root
        r#"{"roots": {"from-input": true}, "walk": ["alias:to", "contained:down"],
            "emit": {"expr": "{identifier: identifier, body_mime_type: body_mime_type, file_names: body.file_names}",
                     "if": {">": {"depth": 0}}}}"#,
    ),
    (
        "flatten_source",
        // the accessor's rule, verbatim: an edge is emitted only when the EMITTING
        // item has a build:from edge — not a mime filter
        r#"{"roots": {"from-input": true}, "walk": ["alias:to", "contained:down", "build:from"],
            "emit": {"expr": "{identifier: identifier, file_names: body.file_names}",
                     "if": {"and": [{">": {"depth": 0}},
                                    "length(connections.\"build:from\" || `[]`) > `0`"]}}}"#,
    ),
];

// ---------------------------------------------------------------- the endpoints

#[derive(OpenApi)]
#[openapi(
    info(title = "Big Tent V2", description = "The V2 surface: queries and operations"),
    paths(node_count, purls, health, healthz, readyz, metrics, query_get, query_post),
    components(schemas(HealthResponse, LivezResponse, ReadyzResponse, QueryPost))
)]
struct ApiV2Doc;

/// The V2 OpenAPI document, describing only the V2 routes.
async fn serve_openapi_v2() -> Json<utoipa::openapi::OpenApi> {
    Json(ApiV2Doc::openapi())
}

/// The count of items the cluster holds.
#[utoipa::path(
    get,
    path = "/node_count",
    tag = "cluster",
    responses((status = 200, description = "The node count", body = u64))
)]
async fn node_count(
    State(state): State<Arc<V2State<GoatHerd>>>,
) -> Result<Json<u64>, Json<serde_json::Value>> {
    Ok(Json(state.holder.get_cluster().node_count()))
}

/// The cluster's PURL list, as text.
#[utoipa::path(
    get,
    path = "/purls",
    tag = "cluster",
    responses(
        (status = 200, description = "The purls.txt content"),
        (status = 404, description = "No purls.txt for this cluster")
    )
)]
async fn purls(
    State(state): State<Arc<V2State<GoatHerd>>>,
) -> Result<Response, (StatusCode, Json<String>)> {
    match state.holder.get_purl() {
        Ok(path) => match tokio::fs::read_to_string(path).await {
            Ok(text) => Ok((
                [(axum::http::header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                text,
            )
                .into_response()),
            Err(_) => Err((StatusCode::NOT_FOUND, Json("no purls.txt".into()))),
        },
        Err(_) => Err((StatusCode::NOT_FOUND, Json("no purls.txt".into()))),
    }
}

/// The cluster's status, version and uptime.
#[utoipa::path(
    get,
    path = "/health",
    tag = "operational",
    responses((status = 200, description = "The cluster status", body = HealthResponse))
)]
async fn health(
    State(state): State<Arc<V2State<GoatHerd>>>,
) -> Json<HealthResponse> {
    let info = state.holder.health_info();
    Json(HealthResponse {
        node_count: info.node_count,
        cluster_count: info.cluster_count,
        last_reload_at: info.last_reload_at,
        uptime_seconds: info.uptime_seconds,
        version: info.version,
        git_sha: info.git_sha,
        status: info.status,
    })
}

/// Liveness: the process answers.
#[utoipa::path(
    get,
    path = "/healthz",
    tag = "operational",
    responses((status = 200, description = "The process is up", body = LivezResponse))
)]
async fn healthz() -> Json<LivezResponse> {
    Json(LivezResponse {
        status: "ok".to_string(),
    })
}

/// Readiness: the cluster holds items, so queries can be answered.
#[utoipa::path(
    get,
    path = "/readyz",
    tag = "operational",
    responses(
        (status = 200, description = "Ready", body = ReadyzResponse),
        (status = 503, description = "The cluster holds nothing")
    )
)]
async fn readyz(
    State(state): State<Arc<V2State<GoatHerd>>>,
) -> (StatusCode, Json<ReadyzResponse>) {
    let ready = state.holder.get_cluster().node_count() > 0;
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(ReadyzResponse { ready }))
}

/// Prometheus metrics.
#[utoipa::path(
    get,
    path = "/metrics",
    tag = "operational",
    responses((status = 200, description = "Metrics in the Prometheus text format"))
)]
async fn metrics(
    State(state): State<Arc<V2State<GoatHerd>>>,
) -> Result<Response, StatusCode> {
    match state.holder.metrics().encode() {
        Ok(body) => Ok(axum::response::Response::builder()
            .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            .body(body.into())
            .unwrap()),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// The GET form: one identifier, a named template.
#[utoipa::path(
    get,
    path = "/query",
    tag = "query",
    params(
        ("identifier" = String, Query, description = "The identifier to run the query from"),
        ("template" = Option<String>, Query, description = "The pre-canned query; omitted means the passthrough lookup")
    ),
    responses(
        (status = 200, description = "The rows, as JSON lines"),
        (status = 400, description = "No identifier supplied"),
        (status = 404, description = "Unknown template")
    )
)]
async fn query_get(
    State(state): State<Arc<V2State<GoatHerd>>>,
    AxumQuery(params): AxumQuery<HashMap<String, String>>,
) -> Result<Response, (StatusCode, Json<String>)> {
    let Some(identifier) = params.get("identifier") else {
        return Err((StatusCode::BAD_REQUEST, Json("`identifier` is required".into())));
    };
    let template_name = params.get("template").map(String::as_str);
    run_template(state, template_name, std::iter::once(identifier.clone()).collect()).await
}

/// The POST body: a set of roots, and either a template's name or an inline query.
/// The POST body of `/v2/query`.
#[derive(Deserialize, utoipa::ToSchema)]
pub struct QueryPost {
    /// The identifiers the query starts from.
    roots: Option<Vec<String>>,
    /// The name of a pre-canned query.
    template: Option<String>,
    /// A full query document, compiled per request and not cached.
    query: Option<Value>,
}

/// The POST form: a set of roots, and a template or an inline query.
///
/// With neither, the basic lookup runs — which is `/bulk`'s shape. The
/// template's name comes from the body's `template` field or the
/// `?template=` query parameter — the body wins when both are given — and
/// resolves against the server's registry, the same registry the GET's
/// `?template=` names.
#[utoipa::path(
    post,
    path = "/query",
    tag = "query",
    request_body = QueryPost,
    params(
        ("template" = Option<String>, Query, description = "The pre-canned query; the body's `template` field wins when both are given")
    ),
    responses(
        (status = 200, description = "The rows, as JSON lines"),
        (status = 404, description = "Unknown template"),
        (status = 422, description = "The inline query does not compile")
    )
)]
async fn query_post(
    State(state): State<Arc<V2State<GoatHerd>>>,
    AxumQuery(params): AxumQuery<HashMap<String, String>>,
    Json(body): Json<QueryPost>,
) -> Result<Response, (StatusCode, Json<String>)> {
    let roots = body.roots.unwrap_or_default();

    // the template's name comes from either the request's query parameter or
    // the body's field — the body wins when both are given — and resolves
    // against the REQUEST's registry, exactly as the GET does: a caller that
    // registered, replaced, or removed a template with `with_template`/
    // `without_template` sees the same template on both verbs. The static
    // built-in list the POST once consulted ignored the state entirely: a
    // custom template answered 404 and a removed builtin still ran.
    let template_name = body.template.as_deref().or_else(|| params.get("template").map(String::as_str));

    let compiled = if let Some(name) = template_name {
        let Some(text) = state.templates.get(name) else {
            return Err((
                StatusCode::NOT_FOUND,
                Json(format!("no template named `{name}`")),
            ));
        };
        state
            .queries
            .get_or_compile(text)
            .map_err(|error| (StatusCode::UNPROCESSABLE_ENTITY, located_error(&error)))?
    } else if let Some(document) = &body.query {
        // compiled per request and NOT cached: an inline document is unbounded in
        // size and caller-controlled, so a cache keyed on it is a memory leak the
        // caller controls
        compile_inline(document)
            .map_err(|error| (StatusCode::UNPROCESSABLE_ENTITY, located_error(&error)))?
    } else {
        // neither: the DEFAULT template — and absent, a 404 naming the omission
        // rather than a silent empty answer
        let default = state.templates.default_template().ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json("no default template is designated; name one with `template`".into()),
            )
        })?;
        state
            .queries
            .get_or_compile(default)
            .map_err(|error| (StatusCode::UNPROCESSABLE_ENTITY, located_error(&error)))?
    };

    stream_query(state, compiled, roots).await
}

/// The GET form's body: a template's name and one identifier.
async fn run_template(
    state: Arc<V2State<GoatHerd>>,
    template_name: Option<&str>,
    identifiers: Vec<String>,
) -> Result<Response, (StatusCode, Json<String>)> {
    let text = match template_name {
        None => state.templates.default_template().ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json("no default template is designated; name one with `template=`".into()),
            )
        })?,
        Some(name) => state.templates.get(name).ok_or_else(|| {
            (StatusCode::NOT_FOUND, Json(format!("no template named `{name}`")))
        })?,
    }
    .to_string();
    let compiled = state
        .queries
        .get_or_compile(&text)
        .map_err(|error| (StatusCode::UNPROCESSABLE_ENTITY, located_error(&error)))?;
    stream_query(state, compiled, identifiers).await
}

/// Compile an inline query document, uncached.
fn compile_inline(document: &Value) -> Result<Arc<CompiledQuery>, GrammarError> {
    let cache = QueryCache::new(4096);
    cache.get_or_compile(&document.to_string())
}

/// Run a compiled query over the given roots, and stream its rows as JSON lines.
///
/// The run happens on a blocking thread — the pipeline's stage threads are
/// synchronous, and the Starlark evaluator is not `Send` — and its rows are
/// forwarded through a bounded channel to the response, so the body streams as the
/// stages produce. A client that stops reading backpressures the whole pipeline,
/// and the run ends when the response stream is dropped.
///
/// The cluster the query walks is the server's, taken per request; the accountant
/// is the no-op one, until the budget work lands.
async fn stream_query(
    state: Arc<V2State<GoatHerd>>,
    compiled: Arc<CompiledQuery>,
    roots: Vec<String>,
) -> Result<Response, (StatusCode, Json<String>)> {
    // the query's input stream, fed by an async task from the request's roots
    let (input_sender, input) = flume::bounded::<Value>(256);
    tokio::spawn(async move {
        for root in roots {
            // a full input backpressures this task, which is fine: the roots are
            // bounded by the request
            let _ = input_sender.send_async(Value::String(root)).await;
        }
        // dropping the sender is what tells the pipeline the roots are complete
    });

    // the rows, forwarded from the blocking run to the streaming response
    let (rows_tx, rows_receiver) = flume::bounded::<Value>(256);
    let rows_sender = rows_tx.clone();
    let lookup = HerdOrCluster::Herd(state.holder.get_cluster());

    // THE CANCELLATION: when the response body is dropped — the client
    // disconnected — this guard flips the flag, and the run's accountant
    // checks stop the work promptly, even in a stage that is mid-computation
    // and not sending. Send-failure propagation handles the rest.
    let cancellation = crate::highwire::walk::Cancellation::new();
    struct CancelGuard(crate::highwire::walk::Cancellation);
    impl Drop for CancelGuard {
        fn drop(&mut self) {
            self.0.cancel();
        }
    }

    let cancellation_moved = cancellation.clone();
    std::thread::Builder::new()
        .name("v2-query-run".to_string())
        .spawn(move || {
            let accountant = Arc::new(crate::highwire::walk::Cancelled {
                cancellation,
            });
            let sink = move |row: Value,
                             _acc: &Arc<crate::highwire::walk::Cancelled>,
                             op: SinkOperation|
                  -> anyhow::Result<()> {
                // a stop's report is marked, so it cannot be mistaken for a row the
                // walk emitted
                let row = match op {
                    SinkOperation::NormalMessage => row,
                    SinkOperation::ErrorMessage => serde_json::json!({ "stop": row }),
                };
                rows_tx
                    .send(row)
                    .map_err(|_| anyhow::anyhow!("the response stream closed"))?;
                Ok(())
            };
            if let Err(error) = compiled.run(accountant, sink, lookup, input) {
                // a run failure is a row too, so the client sees it in the stream.
                // A FAILED send means the client is gone; there is nothing left
                // to report to, so the failure is swallowed deliberately.
                let _ = rows_sender.send(serde_json::json!({ "error": error.to_string() }));
            }
        })
        .expect("the query thread spawns");

    // the response: every row as one JSON line, in the order the pipeline emitted
    use axum::response::IntoResponse as _;
    use axum_streams::StreamBodyAs;
    // JSON LINES: one row per line, which is what a streaming client reads
    // incrementally; a JSON array would require the whole response before parsing
    // the GUARD rides the stream: when the body is dropped — the client
    // disconnected — the guard drops with it and cancels the run
    let guard = CancelGuard(cancellation_moved);
    let stream = futures::StreamExt::map(rows_receiver.into_stream(), move |row| {
        let _ = &guard; // the guard's lifetime is the stream's
        row
    });
    Ok(StreamBodyAs::json_nl(stream).into_response())
}

/// An error located the way every compilation error is.
fn located_error(error: &GrammarError) -> Json<String> {
    Json(error.to_string())
}

/// The V2 router: everything this module serves, under whatever prefix the server
/// nests it at. The state is the V2 state, holding the cluster and the query cache.
/// The V2 router, over the herd holder the server runs.
pub fn router_v2(state: Arc<V2State<GoatHerd>>) -> Router<()> {
    Router::new()
        .route("/openapi.json", get(serve_openapi_v2))
        .route("/node_count", get(node_count))
        .route("/purls", get(purls))
        .route("/health", get(health))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .route("/query", get(query_get))
        .route("/query", post(query_post))
        .with_state(state)
}

/// The V2 OpenAPI document, for tests and for merging into a wider document.
pub fn openapi_v2() -> utoipa::openapi::OpenApi {
    ApiV2Doc::openapi()
}
