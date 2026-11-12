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
        // NO PANIC: a built-in default that names nothing is a diagnosable 400 at
        // request time, not a server that dies at startup.
        let templates = match TemplateRegistry::new()
            .with_builtins()
            .with_default("item")
        {
            Ok(registry) => registry,
            Err(_) => TemplateRegistry::new().with_builtins(),
        };
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
/// THE BUILT-IN TEMPLATES — read from the documents the tests exercise.
///
/// They are `include_str!` rather than literals because they used to exist
/// twice: a text here and a twin under `test_data/queries/`. Every equivalence
/// test reads the twin, so a corrected twin could ship beside a stale served
/// text — which is exactly what happened to `flatten` and `flatten_source`,
/// where the fix was inert in production and green in the suite. With
/// `include_str!` there is one text, and divergence is impossible.
const TEMPLATES: &[(&str, &str)] = &[
    ("item", include_str!("../test_data/queries/item.json")),
    (
        "anti_alias",
        include_str!("../test_data/queries/anti_alias.json"),
    ),
    ("north", include_str!("../test_data/queries/north.json")),
    (
        "north_purls",
        include_str!("../test_data/queries/north_purls.json"),
    ),
    ("flatten", include_str!("../test_data/queries/flatten.json")),
    (
        "flatten_source",
        include_str!("../test_data/queries/flatten_source.json"),
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
            // only a missing file is a 404; a permission or read failure is the
            // server's problem and says so
            Err(error) => Err(crate::server::purl_failure(&anyhow::Error::new(error))),
        },
        Err(error) => Err(crate::server::purl_failure(&error)),
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
        Ok(body) => Ok((
            [
                (
                    axum::http::header::CONTENT_TYPE,
                    crate::server::PROMETHEUS_CONTENT_TYPE,
                ),
            ],
            body,
        )
            .into_response()),
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
/// Feed the request's roots into the pipeline's input stream, answering how many
/// were delivered.
///
/// A send failure means the pipeline dropped its input receiver: the run is gone,
/// so feeding stops rather than spinning through roots nobody will read. The loss
/// is REPORTED to the accountant, because a root that was never delivered produces
/// no row that could say so — without the event it leaves no trace at all.
async fn feed_roots(
    sender: &flume::Sender<Value>,
    roots: Vec<String>,
    accountant: &Arc<dyn crate::highwire::walk::AccountingAndBudget + Send + Sync>,
) -> usize {
    let mut delivered = 0;
    for root in roots {
        // a full input backpressures this task, which is fine: the roots are
        // bounded by the request
        if sender.send_async(Value::String(root)).await.is_err() {
            accountant.event(crate::highwire::walk::AccountingEventType::RootFeedLost);
            break;
        }
        delivered += 1;
    }
    delivered
}

async fn stream_query(
    state: Arc<V2State<GoatHerd>>,
    compiled: Arc<CompiledQuery>,
    roots: Vec<String>,
) -> Result<Response, (StatusCode, Json<String>)> {
    // the rows, forwarded from the blocking run to the streaming response
    let (rows_tx, rows_receiver) = flume::bounded::<Value>(256);
    let rows_sender = rows_tx.clone();
    let lookup = HerdOrCluster::Herd(state.holder.get_cluster());

    // THE CANCELLATION: when the response body is dropped — the client
    // disconnected — this guard flips the flag, and the run's accountant
    // checks stop the work promptly, even in a stage that is mid-computation
    // and not sending. Send-failure propagation handles the rest. It is built
    // BEFORE the feeder because the feeder reports a lost root to it.
    let cancellation = crate::highwire::walk::Cancellation::new();
    let accountant: Arc<dyn crate::highwire::walk::AccountingAndBudget + Send + Sync> =
        Arc::new(crate::highwire::walk::Cancelled {
            cancellation: cancellation.clone(),
        });

    // the query's input stream, fed by an async task from the request's roots
    let (input_sender, input) = flume::bounded::<Value>(256);
    let feeder_accountant = accountant.clone();
    tokio::spawn(async move {
        feed_roots(&input_sender, roots, &feeder_accountant).await;
        // dropping the sender is what tells the pipeline the roots are complete
    });

    struct CancelGuard(crate::highwire::walk::Cancellation);
    impl Drop for CancelGuard {
        fn drop(&mut self) {
            self.0.cancel();
        }
    }

    let cancellation_moved = cancellation.clone();
    // ONE OS THREAD, reported at creation: `ThreadStarted` means a real thread, and this
    // is the request's own — it reserves a stack and a scheduler slot whether or not the
    // client stays to read the rows.
    accountant.event(crate::highwire::walk::AccountingEventType::ThreadStarted);
    std::thread::Builder::new()
        .name("v2-query-run".to_string())
        .spawn(move || {
            let accountant = accountant.clone();
            let sink = move |row: Value,
                             _acc: &Arc<dyn crate::highwire::walk::AccountingAndBudget + Send + Sync>,
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
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, Json("the query's worker could not start".into())))?;

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

#[cfg(test)]
mod template_inventory {
    //! THE BUILT-IN INVENTORY, ENUMERATED INSTEAD OF LISTED BY HAND.
    //!
    //! REQUIREMENT (class-A audit): the anti-drift gate lived in an integration
    //! test with a HAND-WRITTEN list of built-in names, so a template added to
    //! `TEMPLATES` would be served without being covered — the gate would drift
    //! exactly as the templates drifted. It also could not say what a document
    //! under `test_data/queries/` is FOR when it is not a served template, so
    //! the four example documents were silently uncovered.
    //!
    //! THEORY OF THE TEST: enumerate rather than list. The module that owns
    //! `TEMPLATES` reads the twin directory and requires every document to be
    //! either a served template equal to it, or a declared example with a
    //! reason. A new template is covered the moment it is written; a new example
    //! document fails until someone says what it is. Living here rather than in
    //! `tests/` is deliberate: the list is private, and enumerating it needs no
    //! new public interface.

    use super::TemplateRegistry;

    /// The documents under `test_data/queries/` that are NOT served as built-in
    /// templates, and why. They are worked examples and V1-only shapes.
    const NOT_SERVED: &[(&str, &str)] = &[
        ("basic_walk", "a worked example of a bare walk, not an endpoint"),
        ("bulk", "V1's /bulk is a different endpoint shape; the passthrough it exercises is `item`"),
        ("filtered_emit", "a worked example of an emit gate, not an endpoint"),
        ("probe_flat", "a worked example of a flat projection, not an endpoint"),
    ];

    /// Every document twin is either served verbatim or declared an example.
    #[test]
    fn every_document_is_served_or_declared() {
        let registry = TemplateRegistry::new().with_builtins();
        let directory = concat!(env!("CARGO_MANIFEST_DIR"), "/test_data/queries");
        let mut seen = Vec::new();
        for entry in std::fs::read_dir(directory).expect("the example directory is present") {
            let path = entry.expect("a readable directory entry").path();
            let name = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("a document has a name")
                .to_string();
            seen.push(name.clone());
            let document = std::fs::read_to_string(&path).expect("a readable document");
            match registry.get(&name) {
                // SERVED: it must be the document, both as JSON
                Some(served) => {
                    let served: serde_json::Value =
                        serde_json::from_str(served).expect("a served template is JSON");
                    let document: serde_json::Value =
                        serde_json::from_str(&document).expect("a twin document is JSON");
                    assert_eq!(
                        served, document,
                        "the served `{name}` must be the document the tests exercise"
                    );
                }
                // NOT SERVED: it must say so, and say why
                None => assert!(
                    NOT_SERVED.iter().any(|(declared, _)| *declared == name),
                    "`{name}` is neither a served template nor a declared example: \
                     serve it, or add it to NOT_SERVED with its purpose"
                ),
            }
        }
        // the declaration must not rot: every entry names a document that exists
        for (declared, reason) in NOT_SERVED {
            assert!(!reason.is_empty(), "`{declared}` needs a reason");
            assert!(
                seen.iter().any(|name| name == declared),
                "NOT_SERVED names `{declared}`, which is not a document under {directory}"
            );
        }
    }

    /// The served names are unique and the registry serves every one of them:
    /// an append-only table cannot shadow an earlier entry.
    #[test]
    fn the_served_names_are_unique_and_all_present() {
        let registry = TemplateRegistry::new().with_builtins();
        for (position, (name, _)) in super::TEMPLATES.iter().enumerate() {
            assert!(
                super::TEMPLATES[..position]
                    .iter()
                    .all(|(earlier, _)| earlier != name),
                "`{name}` appears twice; the later entry would shadow the earlier one"
            );
        }
        // every name in the table is actually served: no entry is inert
        for (name, _) in super::TEMPLATES {
            assert!(
                registry.get(name).is_some(),
                "the table holds `{name}` but the registry does not serve it"
            );
        }
    }
}

#[cfg(test)]
mod feeder {
    //! A ROOT THAT CANNOT BE DELIVERED IS AN EVENT, NOT A SILENT SKIP.
    //!
    //! REQUIREMENT (S3a, same pattern as S1): the feeder sent roots with
    //! `let _ = send_async(..)`, so a pipeline that had already dropped its input
    //! receiver made every remaining root vanish with no trace — the run answered
    //! success and the roots that were never delivered produced no row that could
    //! say so.
    //!
    //! THEORY OF THE TEST: the feeder is driven directly with a REAL channel whose
    //! receiver is closed, which is the condition the pipeline itself produces only
    //! non-deterministically. Two properties: the feeder stops instead of spinning
    //! through the rest (the count answers how many it delivered) and the accountant
    //! records `RootFeedLost`, which is the only surviving evidence. The control
    //! feeds the same roots into a live channel: all are delivered and no loss is
    //! recorded, so a feeder that simply gave up would fail.
    use super::feed_roots;
    use crate::highwire::walk::{AccountingAndBudget, AccountingEventType};
    use serde_json::Value;
    use std::sync::Arc;

    /// An accountant that records the events it is told about.
    struct Ledger {
        events: std::sync::Mutex<Vec<AccountingEventType>>,
    }

    impl AccountingAndBudget for Ledger {
        fn should_stop(&self) -> Option<Vec<Value>> {
            None
        }
        fn can_sink_normal(&self) -> bool {
            true
        }
        fn event(&self, event_type: AccountingEventType) {
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(event_type);
        }
    }

    impl Ledger {
        fn new() -> Arc<Self> {
            Arc::new(Ledger {
                events: std::sync::Mutex::new(Vec::new()),
            })
        }
        fn lost_roots(&self) -> usize {
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .filter(|event| **event == AccountingEventType::RootFeedLost)
                .count()
        }
    }

    /// A closed receiver stops the feeder and records the loss.
    #[tokio::test]
    async fn a_root_that_cannot_be_delivered_is_recorded() {
        let (sender, receiver) = flume::bounded::<Value>(4);
        drop(receiver);
        let ledger = Ledger::new();
        let shared: Arc<dyn AccountingAndBudget + Send + Sync> = ledger.clone();

        let delivered = feed_roots(&sender, vec!["a".into(), "b".into()], &shared).await;

        assert_eq!(delivered, 0, "nothing could be delivered");
        assert_eq!(
            ledger.lost_roots(),
            1,
            "the loss must be recorded once, on the first failed send — it is the \
             only trace left"
        );
    }

    /// The control: a live receiver takes every root and records nothing.
    #[tokio::test]
    async fn every_root_reaches_a_live_pipeline() {
        let (sender, receiver) = flume::bounded::<Value>(4);
        let ledger = Ledger::new();
        let shared: Arc<dyn AccountingAndBudget + Send + Sync> = ledger.clone();

        let delivered = feed_roots(&sender, vec!["a".into(), "b".into()], &shared).await;
        drop(sender);

        assert_eq!(delivered, 2, "both roots delivered");
        assert_eq!(receiver.try_recv().expect("a root arrives"), Value::String("a".into()));
        assert_eq!(receiver.try_recv().expect("a root arrives"), Value::String("b".into()));
        assert_eq!(ledger.lost_roots(), 0, "a delivered root is not a loss");
    }
}
