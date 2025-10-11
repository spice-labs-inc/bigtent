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

use crate::{
  item::Item,
  rodeo::{goat_trait::GoatRodeoTrait, holder::ClusterHolder},
};
#[cfg(test)]
use std::println as info;

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

//#[axum::debug_handler]
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

async fn node_count<GRT: GoatRodeoTrait + 'static>(
  State(rodeo): State<Arc<ClusterHolder<GRT>>>,
) -> Result<Json<u64>, Json<serde_json::Value>> {
  Ok(Json(rodeo.get_cluster().node_count()))
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
// #[axum::debug_handler]
async fn serve_gitoid<GRT: GoatRodeoTrait + 'static>(
  State(rodeo): State<Arc<ClusterHolder<GRT>>>,
  Path(gitoid): Path<String>,
) -> Result<Json<Item>, (StatusCode, Json<String>)> {
  do_serve_gitoid(rodeo, Some(&gitoid)).await
}
async fn serve_gitoid_query<GRT: GoatRodeoTrait + 'static>(
  State(rodeo): State<Arc<ClusterHolder<GRT>>>,
  Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Item>, (StatusCode, Json<String>)> {
  do_serve_gitoid(rodeo, query.get("identifier")).await
}

/// Do a bulk set of anti-alias transforms
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
async fn serve_anti_alias<GRT: GoatRodeoTrait + 'static>(
  State(rodeo): State<Arc<ClusterHolder<GRT>>>,
  Path(gitoid): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<String>)> {
  do_serve_anti_alias(rodeo, Some(&gitoid)).await
}

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

async fn serve_flatten_source<GRT: GoatRodeoTrait + 'static>(
  State(rodeo): State<Arc<ClusterHolder<GRT>>>,
  Path(gitoid): Path<String>,
) -> impl IntoResponse {
  serve_flatten_both(rodeo, vec![gitoid], true).await
}
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

/// Serve all the "contains" gitoids for a given Item
/// Follows `AliasTo` links
async fn serve_flatten_source_bulk<GRT: GoatRodeoTrait + 'static>(
  State(rodeo): State<Arc<ClusterHolder<GRT>>>,
  Json(payload): Json<Vec<String>>,
) -> impl IntoResponse {
  serve_flatten_both(rodeo, payload, true).await
}

async fn serve_flatten<GRT: GoatRodeoTrait + 'static>(
  State(rodeo): State<Arc<ClusterHolder<GRT>>>,
  Path(gitoid): Path<String>,
) -> impl IntoResponse {
  serve_flatten_both(rodeo, vec![gitoid], false).await
}
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

/// Serve all the "contains" gitoids for a given Item
/// Follows `AliasTo` links
async fn serve_flatten_bulk<GRT: GoatRodeoTrait + 'static>(
  State(rodeo): State<Arc<ClusterHolder<GRT>>>,
  Json(payload): Json<Vec<String>>,
) -> impl IntoResponse {
  serve_flatten_both(rodeo, payload, false).await
}

async fn serve_north<GRT: GoatRodeoTrait + 'static>(
  State(rodeo): State<Arc<ClusterHolder<GRT>>>,
  Path(gitoid): Path<String>,
) -> impl IntoResponse {
  do_north(rodeo, vec![gitoid], false).await
}
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

async fn serve_north_purls<GRT: GoatRodeoTrait + 'static>(
  State(rodeo): State<Arc<ClusterHolder<GRT>>>,
  Path(gitoid): Path<String>,
) -> impl IntoResponse {
  do_north(rodeo, vec![gitoid], true).await
}
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

async fn serve_north_bulk<GRT: GoatRodeoTrait + 'static>(
  State(rodeo): State<Arc<ClusterHolder<GRT>>>,
  Json(payload): Json<Vec<String>>,
) -> impl IntoResponse {
  do_north(rodeo, payload, false).await
}

async fn serve_north_purls_bulk<GRT: GoatRodeoTrait + 'static>(
  State(rodeo): State<Arc<ClusterHolder<GRT>>>,
  Json(payload): Json<Vec<String>>,
) -> impl IntoResponse {
  do_north(rodeo, payload, true).await
}

/// serve the `purls.txt` file
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

/// Build up the routes for the default Big Tent features
pub fn build_route<GRT: GoatRodeoTrait + 'static>(state: Arc<ClusterHolder<GRT>>) -> Router {
  let app: Router<()> = Router::new()
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
