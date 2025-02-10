use std::{net::SocketAddr, pin::Pin, sync::Arc, time::Instant};

use anyhow::Result;
use axum::{
  extract::{Path, State},
  http::StatusCode,
  response::IntoResponse,
  routing::{get, post},
  Json, Router,
};
use axum_streams::*;

#[cfg(not(test))]
use log::info;

use tokio::sync::mpsc::Receiver;
use tokio_stream::Stream; // Use log crate when building application

use crate::{rodeo_server::RodeoServer, structs::Item};
#[cfg(test)]
use std::println as info;

async fn stream_items(rodeo: Arc<RodeoServer>, items: Vec<String>) -> Receiver<Item> {
  let (mtx, mrx) = tokio::sync::mpsc::channel::<Item>(32);

  tokio::spawn(async move {
    for item_id in items {
      match rodeo.data_for_key(&item_id).await {
        Ok(i) => {
          if !mtx.is_closed() {
            let _ = mtx.send(i).await;
          }
        }
        Err(_) => {}
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
  State(rodeo): State<Arc<RodeoServer>>,
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

async fn serve_gitoid(
  State(rodeo): State<Arc<RodeoServer>>,
  Path(gitoid): Path<String>,
) -> Result<Json<Item>, impl IntoResponse> {
  let ret = rodeo.data_for_key(&gitoid).await;

  match ret {
    Ok(item) => Ok(Json(item)),
    Err(_) => Err((
      StatusCode::NOT_FOUND,
      format!("No item found for key {}", gitoid),
    )),
  }
}

async fn serve_anti_alias(
  State(rodeo): State<Arc<RodeoServer>>,
  Path(gitoid): Path<String>,
) -> Result<Json<Item>, impl IntoResponse> {
  let ret = rodeo.antialias_for(&gitoid).await;

  match ret {
    Ok(item) => Ok(Json(item)),
    Err(_) => Err((
      StatusCode::NOT_FOUND,
      format!("No item found for key {}", gitoid),
    )),
  }
}

async fn serve_north(
  State(rodeo): State<Arc<RodeoServer>>,
  Path(gitoid): Path<String>,
) -> impl IntoResponse {
  do_north(rodeo, vec![gitoid], false).await
}

async fn serve_north_purls(
  State(rodeo): State<Arc<RodeoServer>>,
  Path(gitoid): Path<String>,
) -> impl IntoResponse {
  do_north(rodeo, vec![gitoid], true).await
}

async fn serve_bulk_north(
  State(rodeo): State<Arc<RodeoServer>>,
  Json(payload): Json<Vec<String>>,
) -> impl IntoResponse {
  do_north(rodeo, payload, false).await
}

async fn serve_bulk_north_purls(
  State(rodeo): State<Arc<RodeoServer>>,
  Json(payload): Json<Vec<String>>,
) -> impl IntoResponse {
  do_north(rodeo, payload, true).await
}

async fn do_north(
  rodeo: Arc<RodeoServer>,
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

fn build_route(state: Arc<RodeoServer>) -> Router {
  let app: Router<()> = Router::new()
    .route("/bulk", post(serve_bulk))
    .route("/{*gitoid}", get(serve_gitoid))
    .route("/aa/{*gitoid}", get(serve_anti_alias))
    .route("/north/{*gitoid}", get(serve_north))
    .route("/north_purls/{*gitoid}", get(serve_north_purls))
    .route("/north", post(serve_bulk_north))
    .route("/north_purls", post(serve_bulk_north_purls))
    .with_state(state);

  app
}

pub async fn run_web_server(index: Arc<RodeoServer>) -> Result<()> {
  let state = index.clone();
  let addrs = index.the_args().to_socket_addrs();
  info!("Listen on {:?}", addrs);

  let app = build_route(state);

  let nested = Router::new().nest("/omnibor", app.clone());

  let aggregated = nested.merge(app);

  let s: &[SocketAddr] = &addrs;
  let listener = tokio::net::TcpListener::bind(s).await?;
  axum::serve(listener, aggregated).await?;
  Ok(())
}

// #[cfg(test)]
// mod tests {
//   use super::*;

//   #[test]
//   fn test_parse_body_to_json() {
//     let request_body_json = serde_json::json!({
//       "foo": "bar",
//       "baz": 42
//     });

//     let request_body_vec = request_body_json.to_string().as_bytes().to_vec();

//     let request = Request::fake_http(
//       "POST",
//       "/",
//       vec![
//         ("Content-Type".to_owned(), "application/json".to_owned()),
//         (
//           "Content-Length".to_owned(),
//           request_body_vec.len().to_string().to_owned(),
//         ),
//       ],
//       request_body_vec,
//     );

//     let result = parse_body_to_json(&request);

//     assert_eq!(result.expect("error"), request_body_json);
//   }
// }
