use std::{collections::HashMap, net::SocketAddr, pin::Pin, sync::Arc, time::Instant};

use anyhow::Result;
use axum::{
  Json, Router,
  extract::{Path, Query, Request as ExtractRequest, State},
  http::{StatusCode, Uri},
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

/// compute the package from either uri which will include query parameters (which are part of)
/// a pURL or the path that was passed in
fn compute_package(query: &HashMap<String, String>, maybe_gitoid: &str, uri: &Uri) -> String {
  if maybe_gitoid.len() == uri.path().len() || maybe_gitoid.len() == uri.path().len() - 1 {
    if let Some(value) = query.get("identifier") {
      return value.clone();
    }
  }
  if let Some(pq) = uri.path_and_query() {
    let path = pq.as_str();
    let offset = if maybe_gitoid.len() > 3 {
      path.find(&maybe_gitoid[0..3])
    } else {
      None
    };

    if let Some(actual_offset) = offset {
      path[actual_offset..].to_string()
    } else {
      path.to_string()
    }
  } else {
    maybe_gitoid.to_string()
  }
}

async fn node_count<GRT: GoatRodeoTrait + 'static>(
  State(rodeo): State<Arc<ClusterHolder<GRT>>>,
) -> Result<Json<serde_json::Value>, Json<serde_json::Value>> {
  Ok(Json(rodeo.get_cluster().node_count().into()))
}
// #[axum::debug_handler]
async fn serve_gitoid<GRT: GoatRodeoTrait + 'static>(
  State(rodeo): State<Arc<ClusterHolder<GRT>>>,
  Path(gitoid): Path<String>,
  Query(query): Query<HashMap<String, String>>,
  uri: Uri,
) -> Result<Json<serde_json::Value>, impl IntoResponse> {
  let to_find = compute_package(&query, &gitoid, &uri);
  let ret = rodeo.get_cluster().item_for_identifier(&to_find);

  match ret {
    Some(item) => Ok(Json(item.to_json())),
    _ => Err((
      StatusCode::NOT_FOUND,
      format!("No item found for key {}", to_find),
    )),
  }
}

async fn serve_anti_alias<GRT: GoatRodeoTrait + 'static>(
  State(rodeo): State<Arc<ClusterHolder<GRT>>>,
  Path(gitoid): Path<String>,
  Query(query): Query<HashMap<String, String>>,
  uri: Uri,
) -> Result<Json<serde_json::Value>, impl IntoResponse> {
  let to_find = compute_package(&query, &gitoid, &uri);
  let ret = rodeo.get_cluster().antialias_for(&to_find);

  match ret {
    Some(item) => Ok(Json(item.to_json())),
    _ => Err((
      StatusCode::NOT_FOUND,
      format!("No item found for key {}", to_find),
    )),
  }
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
    Err(e) => return Err((StatusCode::NOT_FOUND, format!("{}", e))),
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
  Query(query): Query<HashMap<String, String>>,
  uri: Uri,
) -> impl IntoResponse {
  let to_find = compute_package(&query, &gitoid, &uri);
  serve_flatten_both(rodeo, vec![to_find], true).await
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
  Query(query): Query<HashMap<String, String>>,
  uri: Uri,
) -> impl IntoResponse {
  let to_find = compute_package(&query, &gitoid, &uri);
  serve_flatten_both(rodeo, vec![to_find], false).await
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
  Query(query): Query<HashMap<String, String>>,
  uri: Uri,
) -> impl IntoResponse {
  let to_find = compute_package(&query, &gitoid, &uri);
  do_north(rodeo, vec![to_find], false).await
}

async fn serve_north_purls<GRT: GoatRodeoTrait + 'static>(
  State(rodeo): State<Arc<ClusterHolder<GRT>>>,
  Path(gitoid): Path<String>,
  Query(query): Query<HashMap<String, String>>,
  uri: Uri,
) -> impl IntoResponse {
  let to_find = compute_package(&query, &gitoid, &uri);
  do_north(rodeo, vec![to_find], true).await
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
) -> Result<Response<ServeFileSystemResponseBody>, (StatusCode, String)> {
  match rodeo.get_purl() {
    Err(_) => Err((StatusCode::NOT_FOUND, "no purls.txt".to_string())),
    Ok(path) => {
      match tower_http::services::ServeFile::new(path)
        .try_call(req)
        .await
      {
        Ok(v) => Ok(v),
        Err(_) => Err((StatusCode::NOT_FOUND, "no purls.txt".to_string())),
      }
    }
  }
}

async fn do_north<GRT>(
  rodeo: Arc<ClusterHolder<GRT>>,
  gitoids: Vec<String>,
  purls_only: bool,
) -> Result<impl IntoResponse, (StatusCode, String)>
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
    Err(e) => return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("{:?}", e))),
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
    .route("/{*gitoid}", get(serve_gitoid))
    .route("/aa/{*gitoid}", get(serve_anti_alias))
    .route("/north/{*gitoid}", get(serve_north))
    .route("/north_purls/{*gitoid}", get(serve_north_purls))
    .route("/north", post(serve_north_bulk))
    .route("/flatten_source/{*gitoid}", get(serve_flatten_source))
    .route("/flatten_source", post(serve_flatten_source_bulk))
    .route("/flatten/{*gitoid}", get(serve_flatten))
    .route("/flatten", post(serve_flatten_bulk))
    .route("/north_purls", post(serve_north_purls_bulk))
    .route("/purls", get(gimme_purls))
    .route("/node_count", get(node_count))
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
