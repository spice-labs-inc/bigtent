use std::{net::SocketAddr, pin::Pin, sync::Arc, time::Instant};

use anyhow::Result;
use axum::{
  extract::{Path, Request, State},
  http::{StatusCode, Uri},
  middleware::{self, Next},
  response::{IntoResponse, Response},
  routing::{get, post},
  Json, Router,
};
use axum_streams::*;

#[cfg(not(test))]
use log::info;

use tokio::sync::mpsc::Receiver;
use tokio_stream::Stream;

use crate::{cluster_holder::ClusterHolder, structs::Item};
#[cfg(test)]
use std::println as info;

async fn stream_items(rodeo: Arc<ClusterHolder>, items: Vec<String>) -> Receiver<Item> {
  let (mtx, mrx) = tokio::sync::mpsc::channel::<Item>(32);

  tokio::spawn(async move {
    for item_id in items {
      match rodeo.get_cluster().item_for_identifier(&item_id).await {
        Ok(Some(i)) => {
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

#[axum::debug_handler]
async fn serve_bulk(
  State(rodeo): State<Arc<ClusterHolder>>,
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
fn compute_package(maybe_gitoid: &str, uri: &Uri) -> String {
  if let Some(pq) = uri.path_and_query() {
    let path = pq.as_str();
    let offset = path.find(maybe_gitoid);
    if let Some(actual_offset) = offset {
      path[actual_offset..].to_string()
    } else {
      path.to_string()
    }
  } else {
    maybe_gitoid.to_string()
  }
}

#[axum::debug_handler]
async fn serve_gitoid(
  State(rodeo): State<Arc<ClusterHolder>>,
  Path(gitoid): Path<String>,
  uri: Uri,
) -> Result<Json<serde_json::Value>, impl IntoResponse> {
  let to_find = compute_package(&gitoid, &uri);

  let ret = rodeo.get_cluster().item_for_identifier(&to_find).await;

  match ret {
    Ok(Some(item)) => Ok(Json(item.to_json())),
    _ => Err((
      StatusCode::NOT_FOUND,
      format!("No item found for key {}", to_find),
    )),
  }
}

async fn serve_anti_alias(
  State(rodeo): State<Arc<ClusterHolder>>,
  Path(gitoid): Path<String>,
  uri: Uri,
) -> Result<Json<serde_json::Value>, impl IntoResponse> {
  let to_find = compute_package(&gitoid, &uri);
  let ret = rodeo.get_cluster().antialias_for(&to_find).await;

  match ret {
    Ok(Some(item)) => Ok(Json(item.to_json())),
    _ => Err((
      StatusCode::NOT_FOUND,
      format!("No item found for key {}", to_find),
    )),
  }
}

async fn serve_flatten_both(
  rodeo: Arc<ClusterHolder>,
  payload: Vec<String>,
  source: bool
) -> Result<impl IntoResponse, impl IntoResponse> {
  let start = Instant::now();
  let payload_len = payload.len();
  scopeguard::defer! {
    info!("Served bulk flatten for {} items in {:?}",payload_len,
    start.elapsed());
  }

  let stream: Receiver<serde_json::Value> =
    match rodeo.get_cluster().stream_flattened_items(payload, source).await {
      Ok(s) => s,
      Err(e) => return Err((StatusCode::NOT_FOUND, format!("{}", e))),
    };
  let tok_stream = TokioReceiverToStream { receiver: stream };

  Ok(StreamBodyAs::json_array(tok_stream))
}

async fn serve_flatten_source(
  State(rodeo): State<Arc<ClusterHolder>>,
  Path(gitoid): Path<String>,
  uri: Uri,
) -> impl IntoResponse {
  let to_find = compute_package(&gitoid, &uri);
  serve_flatten_both(rodeo, vec![to_find], true).await
}

/// Serve all the "contains" gitoids for a given Item
/// Follows `AliasTo` links
async fn serve_flatten_source_bulk(
  State(rodeo): State<Arc<ClusterHolder>>,
  Json(payload): Json<Vec<String>>,
) -> impl IntoResponse {
  serve_flatten_both(rodeo, payload, true).await
}

async fn serve_flatten(
  State(rodeo): State<Arc<ClusterHolder>>,
  Path(gitoid): Path<String>,
  uri: Uri,
) -> impl IntoResponse {
  let to_find = compute_package(&gitoid, &uri);
  serve_flatten_both(rodeo, vec![to_find], false).await
}

/// Serve all the "contains" gitoids for a given Item
/// Follows `AliasTo` links
async fn serve_flatten_bulk(
  State(rodeo): State<Arc<ClusterHolder>>,
  Json(payload): Json<Vec<String>>,
) -> impl IntoResponse {
  serve_flatten_both(rodeo, payload, false).await
}

async fn serve_north(
  State(rodeo): State<Arc<ClusterHolder>>,
  Path(gitoid): Path<String>,
  uri: Uri,
) -> impl IntoResponse {
  let to_find = compute_package(&gitoid, &uri);
  do_north(rodeo, vec![to_find], false).await
}

async fn serve_north_purls(
  State(rodeo): State<Arc<ClusterHolder>>,
  Path(gitoid): Path<String>,
  uri: Uri,
) -> impl IntoResponse {
  let to_find = compute_package(&gitoid, &uri);
  do_north(rodeo, vec![to_find], true).await
}

async fn serve_north_bulk(
  State(rodeo): State<Arc<ClusterHolder>>,
  Json(payload): Json<Vec<String>>,
) -> impl IntoResponse {
  do_north(rodeo, payload, false).await
}

async fn serve_north_purls_bulk(
  State(rodeo): State<Arc<ClusterHolder>>,
  Json(payload): Json<Vec<String>>,
) -> impl IntoResponse {
  do_north(rodeo, payload, true).await
}

async fn do_north(
  rodeo: Arc<ClusterHolder>,
  gitoids: Vec<String>,
  purls_only: bool,
) -> impl IntoResponse {
  let start = Instant::now();
  let gitoid_clone = gitoids.clone();
  scopeguard::defer! {
    info!("Served North for {:?} in {:?}", gitoid_clone,
    start.elapsed());
  }
  let (mtx, mrx) = tokio::sync::mpsc::channel::<serde_json::Value>(32);

  tokio::spawn(async move {
    match rodeo
      .get_cluster()
      .north_send(gitoids, purls_only, mtx, start)
      .await
    {
      Ok(_) => {}
      Err(e) => log::error!("Failed to do `north_send` {:?}", e),
    }
  });

  StreamBodyAs::json_array(TokioReceiverToStream { receiver: mrx })
}

/// Build up the routes for the default Big Tent features
pub fn build_route(state: Arc<ClusterHolder>) -> Router {
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
    .with_state(state);

  app
}

/// middleware for request logging
pub async fn request_log_middleware(request: Request, next: Next) -> Response {
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

pub async fn run_web_server(index: Arc<ClusterHolder>) -> Result<()> {
  let state = index.clone();
  let addrs = index.the_args().to_socket_addrs();
  info!("Listen on {:?}", addrs);

  let app = build_route(state)
    .layer(middleware::from_fn(request_log_middleware));

  let nested = Router::new().nest("/omnibor", app.clone());

  let aggregated = nested.merge(app);

  let s: &[SocketAddr] = &addrs;
  let listener = tokio::net::TcpListener::bind(s).await?;
  axum::serve(listener, aggregated).await?;
  Ok(())
}
