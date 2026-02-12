//! # HTTP REST API Server
//!
//! This module implements the REST API for BigTent using the Axum web framework.
//!
//! ## Endpoints
//!
//! ### Item Retrieval
//! - `GET /item/{gitoid}` - Get item by GitOID
//! - `GET /item?identifier=...` - Get item by query parameter
//! - `POST /bulk` - Get multiple items by GitOID array
//!
//! ### Alias Resolution
//! - `GET /aa/{gitoid}` - Resolve alias to canonical item
//! - `GET /aa?identifier=...` - Resolve alias by query parameter
//! - `POST /aa` - Bulk alias resolution
//!
//! ### Graph Traversal
//! - `GET /north/{gitoid}` - Find containers/builders (upward traversal)
//! - `GET /north_purls/{gitoid}` - Same, but only items with PURLs
//! - `GET /flatten/{gitoid}` - Find contained items (downward traversal)
//! - `GET /flatten_source/{gitoid}` - Flatten with source information
//!
//! ### Metadata
//! - `GET /node_count` - Total items in cluster
//! - `GET /purls` - Download purls.txt file
//! - `GET /openapi.json` - OpenAPI specification
//!
//! ## Route Mirroring
//!
//! All routes are available at both `/` and `/omnibor/` prefixes for compatibility.
//!
//! ## OpenAPI Documentation
//!
//! The API is fully documented using utoipa. Access the spec at `/openapi.json`.
//! See [`ApiDoc`] for the specification structure.

use std::{collections::HashMap, net::SocketAddr, pin::Pin, sync::Arc, time::Instant};

use anyhow::Result;
use axum::{
    Json, Router,
    extract::{Path, Query, Request as ExtractRequest, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use axum_streams::*;

use futures::StreamExt;
#[cfg(not(test))]
use log::info;

use tokio::sync::mpsc::Receiver;
use tokio_stream::Stream;
use tokio_util::either::Either;
use tower_http::services::fs::ServeFileSystemResponseBody;
use utoipa::OpenApi;

use serde::Serialize;

use crate::{
    item::Item,
    rodeo::{goat_trait::GoatRodeoTrait, holder::ClusterHolder},
};
#[cfg(test)]
use std::println as info;

/// OpenAPI documentation for the BigTent API
#[derive(OpenApi)]
#[openapi(
    info(
        title = "BigTent API",
        version = "0.13.0",
        description = "An opinionated Graph Database API for serving millions of GitOIDs",
        license(name = "Apache-2.0")
    ),
    paths(
        serve_bulk,
        serve_gitoid,
        serve_gitoid_query,
        serve_anti_alias,
        serve_anti_alias_query,
        anti_alias_bulk,
        serve_north,
        serve_north_query,
        serve_north_bulk,
        serve_north_purls,
        serve_north_purls_query,
        serve_north_purls_bulk,
        serve_flatten,
        serve_flatten_query,
        serve_flatten_bulk,
        serve_flatten_source,
        serve_flatten_source_query,
        serve_flatten_source_bulk,
        gimme_purls,
        node_count,
    ),
    components(schemas(Item)),
    tags(
        (name = "items", description = "Item retrieval endpoints"),
        (name = "anti-alias", description = "Alias resolution endpoints"),
        (name = "traversal", description = "Graph traversal endpoints"),
        (name = "metadata", description = "Cluster metadata endpoints")
    )
)]
pub struct ApiDoc;

async fn stream_items<GRT: GoatRodeoTrait + 'static>(
    rodeo: Arc<ClusterHolder<GRT>>,
    items: Vec<String>,
) -> Receiver<Item> {
    let (mtx, mrx) = tokio::sync::mpsc::channel::<Item>(32);

    tokio::spawn(async move {
        for item_id in items {
            match rodeo.get_cluster().item_for_identifier(&item_id) {
                Some(i) => {
                    if !mtx.is_closed() {
                        let _ = mtx.send(i).await;
                    }
                }
                _ => {}
            }
        }
    });

    mrx
}

pub struct TokioReceiverToStream<T> {
    pub receiver: Receiver<T>,
}

impl<T> Stream for TokioReceiverToStream<T> {
    type Item = T;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let x = Pin::into_inner(self);

        x.receiver.poll_recv(cx)
    }
}

/// Retrieve multiple items by their GitOID identifiers in a single request.
///
/// Returns a JSON array stream of Item objects for the requested identifiers.
/// Items that don't exist are silently omitted from the response.
#[utoipa::path(
    post,
    path = "/bulk",
    tag = "items",
    request_body(content = Vec<String>, description = "Array of GitOID identifiers to retrieve"),
    responses(
        (status = 200, description = "Stream of items", body = Vec<Item>)
    )
)]
async fn serve_bulk<GRT: GoatRodeoTrait + 'static>(
    State(rodeo): State<Arc<ClusterHolder<GRT>>>,
    Json(payload): Json<Vec<String>>,
) -> impl IntoResponse {
    let start = Instant::now();
    let payload_len = payload.len();
    scopeguard::defer! {
      info!("Served bulk for {} items in {:?}",payload_len,
      start.elapsed());
    }

    StreamBodyAs::json_array(TokioReceiverToStream {
        receiver: stream_items(rodeo, payload).await,
    })
}

/// Returns the total item count as a bare decimal integer.
/// Response body is exactly a JSON number with no wrapper object.
/// Example response: `42`
///
/// Consumers (e.g. mace) parse this with `.trim().parse::<u64>()`.
/// Do not change this format without coordinating with mace.
#[utoipa::path(
    get,
    path = "/node_count",
    tag = "metadata",
    responses(
        (status = 200, description = "Total item count", body = u64)
    )
)]
async fn node_count<GRT: GoatRodeoTrait + 'static>(
    State(rodeo): State<Arc<ClusterHolder<GRT>>>,
) -> Result<Json<u64>, Json<serde_json::Value>> {
    Ok(Json(rodeo.get_cluster().node_count()))
}

/// Health check endpoint returning cluster status information.
#[derive(Serialize)]
struct HealthResponse {
    node_count: u64,
    cluster_count: u64,
    last_reload_at: Option<String>,
    uptime_seconds: u64,
}

async fn health<GRT: GoatRodeoTrait + 'static>(
    State(rodeo): State<Arc<ClusterHolder<GRT>>>,
) -> Json<HealthResponse> {
    let info = rodeo.health_info();
    Json(HealthResponse {
        node_count: info.node_count,
        cluster_count: info.cluster_count,
        last_reload_at: info.last_reload_at,
        uptime_seconds: info.uptime_seconds,
    })
}

async fn do_serve_gitoid<GRT: GoatRodeoTrait + 'static>(
    rodeo: Arc<ClusterHolder<GRT>>,
    maybe_gitoid: Option<&String>,
) -> Result<Json<Item>, (StatusCode, Json<String>)> {
    if let Some(gitoid) = maybe_gitoid {
        let ret = rodeo.get_cluster().item_for_identifier(&gitoid);

        match ret {
            Some(item) => Ok(Json(item)),
            _ => Err((
                StatusCode::NOT_FOUND,
                Json(format!("No item found for key {}", gitoid)),
            )),
        }
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(format!("No 'identifier' supplied")),
        ))
    }
}
/// Retrieve a single item by its GitOID identifier (path parameter).
#[utoipa::path(
    get,
    path = "/item/{gitoid}",
    tag = "items",
    params(
        ("gitoid" = String, Path, description = "The GitOID identifier of the item")
    ),
    responses(
        (status = 200, description = "Item found", body = Item),
        (status = 404, description = "Item not found", body = String)
    )
)]
async fn serve_gitoid<GRT: GoatRodeoTrait + 'static>(
    State(rodeo): State<Arc<ClusterHolder<GRT>>>,
    Path(gitoid): Path<String>,
) -> Result<Json<Item>, (StatusCode, Json<String>)> {
    do_serve_gitoid(rodeo, Some(&gitoid)).await
}

/// Retrieve a single item by its GitOID identifier (query parameter).
#[utoipa::path(
    get,
    path = "/item",
    tag = "items",
    params(
        ("identifier" = String, Query, description = "The GitOID identifier of the item")
    ),
    responses(
        (status = 200, description = "Item found", body = Item),
        (status = 404, description = "Item not found", body = String)
    )
)]
async fn serve_gitoid_query<GRT: GoatRodeoTrait + 'static>(
    State(rodeo): State<Arc<ClusterHolder<GRT>>>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Item>, (StatusCode, Json<String>)> {
    do_serve_gitoid(rodeo, query.get("identifier")).await
}

/// Resolve multiple identifiers to their canonical (non-alias) items.
///
/// Takes an array of identifiers and returns a map of identifier to resolved Item.
/// Identifiers that don't resolve are omitted from the response.
#[utoipa::path(
    post,
    path = "/aa",
    tag = "anti-alias",
    request_body(content = Vec<String>, description = "Array of identifiers to resolve"),
    responses(
        (status = 200, description = "Map of identifier to resolved item", body = HashMap<String, Item>)
    )
)]
async fn anti_alias_bulk<GRT: GoatRodeoTrait + 'static>(
    State(rodeo): State<Arc<ClusterHolder<GRT>>>,
    Json(payload): Json<Vec<String>>,
) -> Result<Json<HashMap<String, Item>>, (StatusCode, Json<String>)> {
    let mut ret = HashMap::new();
    let cluster = rodeo.get_cluster();
    for key in payload {
        match cluster.clone().antialias_for(&key) {
            Some(item) => {
                ret.insert(key, item);
            }
            None => {}
        }
    }

    Ok(Json(ret))
}
async fn do_serve_anti_alias<GRT: GoatRodeoTrait + 'static>(
    rodeo: Arc<ClusterHolder<GRT>>,
    gitoid: Option<&String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<String>)> {
    if let Some(to_find) = gitoid {
        let ret = rodeo.get_cluster().antialias_for(&to_find);

        match ret {
            Some(item) => Ok(Json(item.to_json())),
            _ => Err((
                StatusCode::NOT_FOUND,
                Json(format!("No item found for key {}", to_find)),
            )),
        }
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(format!("No 'identifier' supplied")),
        ))
    }
}
/// Resolve an identifier to its canonical (non-alias) item (path parameter).
///
/// Follows alias chains to find the canonical item that the given identifier points to.
#[utoipa::path(
    get,
    path = "/aa/{gitoid}",
    tag = "anti-alias",
    params(
        ("gitoid" = String, Path, description = "The identifier to resolve (may be an alias)")
    ),
    responses(
        (status = 200, description = "Resolved item", body = Item),
        (status = 404, description = "Item not found", body = String)
    )
)]
async fn serve_anti_alias<GRT: GoatRodeoTrait + 'static>(
    State(rodeo): State<Arc<ClusterHolder<GRT>>>,
    Path(gitoid): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<String>)> {
    do_serve_anti_alias(rodeo, Some(&gitoid)).await
}

/// Resolve an identifier to its canonical (non-alias) item (query parameter).
#[utoipa::path(
    get,
    path = "/aa",
    tag = "anti-alias",
    params(
        ("identifier" = String, Query, description = "The identifier to resolve (may be an alias)")
    ),
    responses(
        (status = 200, description = "Resolved item", body = Item),
        (status = 404, description = "Item not found", body = String)
    )
)]
async fn serve_anti_alias_query<GRT: GoatRodeoTrait + 'static>(
    State(rodeo): State<Arc<ClusterHolder<GRT>>>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<String>)> {
    do_serve_anti_alias(rodeo, query.get("identifier")).await
}

async fn serve_flatten_both<GRT: GoatRodeoTrait + 'static>(
    rodeo: Arc<ClusterHolder<GRT>>,
    payload: Vec<String>,
    source: bool,
) -> Result<impl IntoResponse, impl IntoResponse> {
    let start = Instant::now();
    let payload_len = payload.len();
    scopeguard::defer! {
      info!("Served bulk flatten for {} items in {:?}",payload_len,
      start.elapsed());
    }

    let stream: Receiver<Either<Item, String>> = match rodeo
        .get_cluster()
        .stream_flattened_items(payload, source)
        .await
    {
        Ok(s) => s,
        Err(e) => return Err((StatusCode::NOT_FOUND, Json(format!("{}", e)))),
    };

    let tok_stream = TokioReceiverToStream { receiver: stream };

    Ok(StreamBodyAs::json_array(tok_stream.map(|v| match v {
        Either::Left(item) => Into::<serde_json::Value>::into(item),
        Either::Right(string) => string.into(),
    })))
}

/// Get flattened dependencies with source information for an item (path parameter).
///
/// Returns all items contained within the given item, including source file information.
#[utoipa::path(
    get,
    path = "/flatten_source/{gitoid}",
    tag = "traversal",
    params(
        ("gitoid" = String, Path, description = "The GitOID of the item to flatten")
    ),
    responses(
        (status = 200, description = "Stream of items with source info", body = Vec<Item>),
        (status = 404, description = "Item not found", body = String)
    )
)]
async fn serve_flatten_source<GRT: GoatRodeoTrait + 'static>(
    State(rodeo): State<Arc<ClusterHolder<GRT>>>,
    Path(gitoid): Path<String>,
) -> impl IntoResponse {
    serve_flatten_both(rodeo, vec![gitoid], true).await
}

/// Get flattened dependencies with source information for an item (query parameter).
#[utoipa::path(
    get,
    path = "/flatten_source",
    tag = "traversal",
    params(
        ("identifier" = String, Query, description = "The GitOID of the item to flatten")
    ),
    responses(
        (status = 200, description = "Stream of items with source info", body = Vec<Item>),
        (status = 404, description = "Item not found", body = String)
    )
)]
async fn serve_flatten_source_query<GRT: GoatRodeoTrait + 'static>(
    State(rodeo): State<Arc<ClusterHolder<GRT>>>,
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    serve_flatten_both(
        rodeo,
        match query.get("identifier") {
            Some(id) => vec![id.to_string()],
            None => vec![],
        },
        true,
    )
    .await
}

/// Get flattened dependencies with source information for multiple items.
///
/// Returns all items contained within the given items, including source file information.
/// Follows `AliasTo` links during traversal.
#[utoipa::path(
    post,
    path = "/flatten_source",
    tag = "traversal",
    request_body(content = Vec<String>, description = "Array of GitOID identifiers to flatten"),
    responses(
        (status = 200, description = "Stream of items with source info", body = Vec<Item>),
        (status = 404, description = "Item not found", body = String)
    )
)]
async fn serve_flatten_source_bulk<GRT: GoatRodeoTrait + 'static>(
    State(rodeo): State<Arc<ClusterHolder<GRT>>>,
    Json(payload): Json<Vec<String>>,
) -> impl IntoResponse {
    serve_flatten_both(rodeo, payload, true).await
}

/// Get flattened dependencies for an item (path parameter).
///
/// Returns all items contained within the given item.
#[utoipa::path(
    get,
    path = "/flatten/{gitoid}",
    tag = "traversal",
    params(
        ("gitoid" = String, Path, description = "The GitOID of the item to flatten")
    ),
    responses(
        (status = 200, description = "Stream of contained items", body = Vec<Item>),
        (status = 404, description = "Item not found", body = String)
    )
)]
async fn serve_flatten<GRT: GoatRodeoTrait + 'static>(
    State(rodeo): State<Arc<ClusterHolder<GRT>>>,
    Path(gitoid): Path<String>,
) -> impl IntoResponse {
    serve_flatten_both(rodeo, vec![gitoid], false).await
}

/// Get flattened dependencies for an item (query parameter).
#[utoipa::path(
    get,
    path = "/flatten",
    tag = "traversal",
    params(
        ("identifier" = String, Query, description = "The GitOID of the item to flatten")
    ),
    responses(
        (status = 200, description = "Stream of contained items", body = Vec<Item>),
        (status = 404, description = "Item not found", body = String)
    )
)]
async fn serve_flatten_query<GRT: GoatRodeoTrait + 'static>(
    State(rodeo): State<Arc<ClusterHolder<GRT>>>,
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    serve_flatten_both(
        rodeo,
        match query.get("identifier") {
            Some(id) => vec![id.to_string()],
            None => vec![],
        },
        false,
    )
    .await
}

/// Get flattened dependencies for multiple items.
///
/// Returns all items contained within the given items.
/// Follows `AliasTo` links during traversal.
#[utoipa::path(
    post,
    path = "/flatten",
    tag = "traversal",
    request_body(content = Vec<String>, description = "Array of GitOID identifiers to flatten"),
    responses(
        (status = 200, description = "Stream of contained items", body = Vec<Item>),
        (status = 404, description = "Item not found", body = String)
    )
)]
async fn serve_flatten_bulk<GRT: GoatRodeoTrait + 'static>(
    State(rodeo): State<Arc<ClusterHolder<GRT>>>,
    Json(payload): Json<Vec<String>>,
) -> impl IntoResponse {
    serve_flatten_both(rodeo, payload, false).await
}

/// Traverse north (upward) to find containers and builders of an item (path parameter).
///
/// Returns all items that contain or build the given item.
#[utoipa::path(
    get,
    path = "/north/{gitoid}",
    tag = "traversal",
    params(
        ("gitoid" = String, Path, description = "The GitOID of the item to traverse from")
    ),
    responses(
        (status = 200, description = "Stream of containing/building items", body = Vec<Item>),
        (status = 500, description = "Internal error", body = String)
    )
)]
async fn serve_north<GRT: GoatRodeoTrait + 'static>(
    State(rodeo): State<Arc<ClusterHolder<GRT>>>,
    Path(gitoid): Path<String>,
) -> impl IntoResponse {
    do_north(rodeo, vec![gitoid], false).await
}

/// Traverse north (upward) to find containers and builders of an item (query parameter).
#[utoipa::path(
    get,
    path = "/north",
    tag = "traversal",
    params(
        ("identifier" = String, Query, description = "The GitOID of the item to traverse from")
    ),
    responses(
        (status = 200, description = "Stream of containing/building items", body = Vec<Item>),
        (status = 500, description = "Internal error", body = String)
    )
)]
async fn serve_north_query<GRT: GoatRodeoTrait + 'static>(
    State(rodeo): State<Arc<ClusterHolder<GRT>>>,
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    do_north(
        rodeo,
        match query.get("identifier") {
            Some(id) => vec![id.to_string()],
            None => vec![],
        },
        false,
    )
    .await
}

/// Traverse north filtering for items with Package URLs only (path parameter).
///
/// Like /north but only returns items that have associated PURLs.
#[utoipa::path(
    get,
    path = "/north_purls/{gitoid}",
    tag = "traversal",
    params(
        ("gitoid" = String, Path, description = "The GitOID of the item to traverse from")
    ),
    responses(
        (status = 200, description = "Stream of items with PURLs", body = Vec<Item>),
        (status = 500, description = "Internal error", body = String)
    )
)]
async fn serve_north_purls<GRT: GoatRodeoTrait + 'static>(
    State(rodeo): State<Arc<ClusterHolder<GRT>>>,
    Path(gitoid): Path<String>,
) -> impl IntoResponse {
    do_north(rodeo, vec![gitoid], true).await
}

/// Traverse north filtering for items with Package URLs only (query parameter).
#[utoipa::path(
    get,
    path = "/north_purls",
    tag = "traversal",
    params(
        ("identifier" = String, Query, description = "The GitOID of the item to traverse from")
    ),
    responses(
        (status = 200, description = "Stream of items with PURLs", body = Vec<Item>),
        (status = 500, description = "Internal error", body = String)
    )
)]
async fn serve_north_purls_query<GRT: GoatRodeoTrait + 'static>(
    State(rodeo): State<Arc<ClusterHolder<GRT>>>,
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    do_north(
        rodeo,
        match query.get("identifier") {
            Some(id) => vec![id.to_string()],
            None => vec![],
        },
        true,
    )
    .await
}

/// Traverse north (upward) for multiple items.
///
/// Returns all items that contain or build any of the given items.
#[utoipa::path(
    post,
    path = "/north",
    tag = "traversal",
    request_body(content = Vec<String>, description = "Array of GitOID identifiers to traverse from"),
    responses(
        (status = 200, description = "Stream of containing/building items", body = Vec<Item>),
        (status = 500, description = "Internal error", body = String)
    )
)]
async fn serve_north_bulk<GRT: GoatRodeoTrait + 'static>(
    State(rodeo): State<Arc<ClusterHolder<GRT>>>,
    Json(payload): Json<Vec<String>>,
) -> impl IntoResponse {
    do_north(rodeo, payload, false).await
}

/// Traverse north filtering for items with Package URLs only (bulk).
#[utoipa::path(
    post,
    path = "/north_purls",
    tag = "traversal",
    request_body(content = Vec<String>, description = "Array of GitOID identifiers to traverse from"),
    responses(
        (status = 200, description = "Stream of items with PURLs", body = Vec<Item>),
        (status = 500, description = "Internal error", body = String)
    )
)]
async fn serve_north_purls_bulk<GRT: GoatRodeoTrait + 'static>(
    State(rodeo): State<Arc<ClusterHolder<GRT>>>,
    Json(payload): Json<Vec<String>>,
) -> impl IntoResponse {
    do_north(rodeo, payload, true).await
}

/// Download the purls.txt file containing all Package URLs in the cluster.
///
/// Returns the raw purls.txt file as a downloadable text file.
#[utoipa::path(
    get,
    path = "/purls",
    tag = "metadata",
    responses(
        (status = 200, description = "The purls.txt file", content_type = "text/plain"),
        (status = 404, description = "No purls.txt available", body = String)
    )
)]
async fn gimme_purls<GRT: GoatRodeoTrait + 'static>(
    State(rodeo): State<Arc<ClusterHolder<GRT>>>,
    req: ExtractRequest,
) -> Result<Response<ServeFileSystemResponseBody>, (StatusCode, Json<String>)> {
    match rodeo.get_purl() {
        Err(_) => Err((StatusCode::NOT_FOUND, Json("no purls.txt".into()))),
        Ok(path) => {
            match tower_http::services::ServeFile::new(path)
                .try_call(req)
                .await
            {
                Ok(v) => Ok(v),
                Err(_) => Err((StatusCode::NOT_FOUND, Json("no purls.txt".into()))),
            }
        }
    }
}

async fn do_north<GRT>(
    rodeo: Arc<ClusterHolder<GRT>>,
    gitoids: Vec<String>,
    purls_only: bool,
) -> Result<impl IntoResponse, (StatusCode, Json<String>)>
where
    GRT: GoatRodeoTrait + 'static,
{
    let start = Instant::now();
    let gitoid_clone = gitoids.clone();
    scopeguard::defer! {
      info!("Served North for {:?} in {:?}", gitoid_clone,
      start.elapsed());
    }

    let mrx = match rodeo
        .get_cluster()
        .north_send(gitoids, purls_only, start)
        .await
    {
        Ok(mrx) => mrx,
        Err(e) => return Err((StatusCode::INTERNAL_SERVER_ERROR, Json(format!("{:?}", e)))),
    };
    Ok(StreamBodyAs::json_array(
        TokioReceiverToStream { receiver: mrx }.map(|v| match v {
            Either::Left(item) => Into::<serde_json::Value>::into(item),
            Either::Right(string) => string.into(),
        }),
    ))
}

/// Serve the OpenAPI specification as JSON.
async fn serve_openapi() -> Json<utoipa::openapi::OpenApi> {
    Json(ApiDoc::openapi())
}

/// Build up the routes for the default Big Tent features
pub fn build_route<GRT: GoatRodeoTrait + 'static>(state: Arc<ClusterHolder<GRT>>) -> Router {
    let app: Router<()> = Router::new()
        .route("/openapi.json", get(serve_openapi))
        .route("/bulk", post(serve_bulk))
        .route("/item/{*gitoid}", get(serve_gitoid))
        .route("/item", get(serve_gitoid_query))
        .route("/aa/{*gitoid}", get(serve_anti_alias))
        .route("/aa", get(serve_anti_alias_query))
        .route("/aa", post(anti_alias_bulk))
        .route("/north", get(serve_north_query))
        .route("/north/{*gitoid}", get(serve_north))
        .route("/north", post(serve_north_bulk))
        .route("/north_purls/{*gitoid}", get(serve_north_purls))
        .route("/north_purls", get(serve_north_purls_query))
        .route("/north_purls", post(serve_north_purls_bulk))
        .route("/flatten_source/{*gitoid}", get(serve_flatten_source))
        .route("/flatten_source", get(serve_flatten_source_query))
        .route("/flatten_source", post(serve_flatten_source_bulk))
        .route("/flatten/{*gitoid}", get(serve_flatten))
        .route("/flatten", get(serve_flatten_query))
        .route("/flatten", post(serve_flatten_bulk))
        .route("/purls", get(gimme_purls))
        .route("/node_count", get(node_count))
        .route("/health", get(health))
        .route("/", get(serve_gitoid_query))
        .route("/{*gitoid}", get(serve_gitoid))
        .with_state(state.clone());

    app
}

/// middleware for request logging
pub async fn request_log_middleware(request: ExtractRequest, next: Next) -> Response {
    let start = Instant::now();
    let request_uri_string = format!("{}", request.uri());
    let response = next.run(request).await;

    info!(
        "Served {} response {} time {:?}",
        request_uri_string,
        response.status(),
        Instant::now().duration_since(start)
    );

    response
}

pub async fn run_web_server<GRT: GoatRodeoTrait + 'static>(
    index: Arc<ClusterHolder<GRT>>,
) -> Result<()> {
    let state = index.clone();
    let addrs = index.the_args().to_socket_addrs();
    info!("Listen on {:?}", addrs);

    let app = build_route(state).layer(middleware::from_fn(request_log_middleware));

    let nested = Router::new().nest("/omnibor", app.clone());

    let aggregated = nested.merge(app);

    let s: &[SocketAddr] = &addrs;
    let listener = tokio::net::TcpListener::bind(s).await?;
    axum::serve(listener, aggregated).await?;
    Ok(())
}

#[cfg(test)]
mod openapi_tests {
    use super::*;
    use utoipa::OpenApi;

    #[test]
    fn test_openapi_spec_generates() {
        // Verify that the OpenAPI spec can be generated without panicking
        let spec = ApiDoc::openapi();
        assert_eq!(spec.info.title, "BigTent API");
        assert_eq!(spec.info.version, "0.13.0");
    }

    #[test]
    fn test_openapi_spec_serializes_to_json() {
        // Verify that the spec can be serialized to valid JSON
        let spec = ApiDoc::openapi();
        let json = serde_json::to_string_pretty(&spec);
        assert!(json.is_ok(), "OpenAPI spec should serialize to JSON");

        let json_str = json.unwrap();
        assert!(
            json_str.contains("\"openapi\""),
            "JSON should contain openapi version"
        );
        assert!(
            json_str.contains("BigTent API"),
            "JSON should contain API title"
        );

        // Print a sample when running with --nocapture for verification
        #[cfg(test)]
        {
            let lines: Vec<&str> = json_str.lines().take(50).collect();
            eprintln!(
                "\n=== OpenAPI Spec Sample (first 50 lines) ===\n{}",
                lines.join("\n")
            );
        }
    }

    #[test]
    fn test_openapi_contains_item_schema() {
        let spec = ApiDoc::openapi();
        let schemas = &spec
            .components
            .as_ref()
            .expect("Should have components")
            .schemas;

        assert!(schemas.contains_key("Item"), "Should contain Item schema");
    }

    #[test]
    fn test_openapi_contains_all_endpoints() {
        let spec = ApiDoc::openapi();
        let paths = &spec.paths;

        // List of expected endpoint paths
        let expected_paths = vec![
            "/bulk",
            "/item/{gitoid}",
            "/item",
            "/aa/{gitoid}",
            "/aa",
            "/north/{gitoid}",
            "/north",
            "/north_purls/{gitoid}",
            "/north_purls",
            "/flatten/{gitoid}",
            "/flatten",
            "/flatten_source/{gitoid}",
            "/flatten_source",
            "/purls",
            "/node_count",
        ];

        for path in &expected_paths {
            assert!(
                paths.paths.contains_key(*path),
                "OpenAPI spec should contain path: {}",
                path
            );
        }
    }

    #[test]
    fn test_openapi_bulk_endpoint_is_post() {
        let spec = ApiDoc::openapi();
        let bulk_path = spec
            .paths
            .paths
            .get("/bulk")
            .expect("Should have /bulk path");

        assert!(bulk_path.post.is_some(), "/bulk should have POST method");
        assert!(bulk_path.get.is_none(), "/bulk should not have GET method");
    }

    #[test]
    fn test_openapi_item_endpoint_is_get() {
        let spec = ApiDoc::openapi();
        let item_path = spec
            .paths
            .paths
            .get("/item/{gitoid}")
            .expect("Should have /item/{{gitoid}} path");

        assert!(
            item_path.get.is_some(),
            "/item/{{gitoid}} should have GET method"
        );
        assert!(
            item_path.post.is_none(),
            "/item/{{gitoid}} should not have POST method"
        );
    }

    #[test]
    fn test_openapi_has_tags() {
        let spec = ApiDoc::openapi();
        let tags = spec.tags.as_ref().expect("Should have tags");

        let tag_names: Vec<&str> = tags.iter().map(|t| t.name.as_str()).collect();

        assert!(tag_names.contains(&"items"), "Should have 'items' tag");
        assert!(
            tag_names.contains(&"anti-alias"),
            "Should have 'anti-alias' tag"
        );
        assert!(
            tag_names.contains(&"traversal"),
            "Should have 'traversal' tag"
        );
        assert!(
            tag_names.contains(&"metadata"),
            "Should have 'metadata' tag"
        );
    }

    #[test]
    fn test_openapi_item_schema_has_required_fields() {
        let spec = ApiDoc::openapi();
        let schemas = &spec
            .components
            .as_ref()
            .expect("Should have components")
            .schemas;
        let item_schema = schemas.get("Item").expect("Should have Item schema");

        // Serialize to check structure
        let json = serde_json::to_value(item_schema).expect("Should serialize");

        // Check that it's an object with properties
        assert!(
            json.get("properties").is_some(),
            "Item schema should have properties"
        );

        let props = json.get("properties").unwrap();
        assert!(
            props.get("identifier").is_some(),
            "Item should have identifier property"
        );
        assert!(
            props.get("connections").is_some(),
            "Item should have connections property"
        );
    }
}
