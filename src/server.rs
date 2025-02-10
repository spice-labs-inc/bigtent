use std::{net::SocketAddr, sync::Arc};

use anyhow::{bail, Result};
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

use serde_json::Value as SJValue;
use tokio::time::Instant;
use tokio_stream::Stream; // Use log crate when building application

use crate::{rodeo_server::RodeoServer, structs::Item};
#[cfg(test)]
use std::println as info;

pub fn value_to_string_array(pv: Result<SJValue>) -> Result<Vec<String>> {
  match pv {
    Ok(SJValue::Array(arr)) => {
      let mut ret = vec![];
      for v in arr {
        match v {
          SJValue::String(s) => ret.push(s),
          _ => {}
        }
      }
      Ok(ret)
    }
    Ok(_) => {
      bail!("Must supply an array of String");
    }
    Err(e) => Err(e),
  }
}

async fn stream_items(rodeo: Arc<RodeoServer>, items: Vec<String>) -> impl Stream<Item = Item> {
  let (tx, rx) = futures::channel::mpsc::unbounded();
  tokio::spawn(async move {
    for item_id in items {
      match rodeo.data_for_key(&item_id).await {
        Ok(i) => {
          _ = tx.unbounded_send(i);
        }
        Err(_) => {}
      }
    }
  });

  rx
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
  StreamBodyAs::json_array(stream_items(rodeo, payload).await)
}

#[axum::debug_handler]
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

// pub async fn north_serve(
//   index: &RodeoServer,
//   request: &Request,
//   path: Option<&str>,
//   purls_only: bool,
//   start: Instant,
// ) -> Response {
//   let to_serve: Vec<String> = if request.method() == "POST" {
//     let body = value_to_string_array(parse_body_to_json(request).await);

//     match body {
//       Ok(v) if v.len() <= 6000 => v,
//       Ok(v) => {
//         return rouille::Response::json(&format!("Bulk request for too many elements {}", v.len()))
//           .with_status_code(400)
//       }
//       Err(e) => return rouille::Response::json(&format!("Error {}", e)).with_status_code(400),
//     }
//   } else {
//     match path {
//       Some(p) => vec![p.into()],
//       _ => return rouille::Response::empty_404(),
//     }
//   };

//   let serve_clone = to_serve.clone();

//   defer! {
//     info!("Served North for {:?} in {:?}", serve_clone,
//     start.elapsed());
//   }

//   let (rx, tx) = pipe::pipe();
//   match index.do_north_serve(to_serve, purls_only, tx, Instant::now()) {
//     Ok(_) => Response {
//       status_code: 200,
//       headers: vec![("Content-Type".into(), "application/json".into())],
//       data: ResponseBody::from_reader(rx),
//       upgrade: None,
//     },
//     _ => Response {
//       status_code: 500,
//       headers: vec![],
//       data: ResponseBody::from_string("Failed to serve north"),
//       upgrade: None,
//     },
//   }
// }

// pub async fn serve_antialias(
//   index: &RodeoServer,
//   _request: &Request,
//   path: &str,
//   start: Instant,
// ) -> Response {
//   defer! {
//     info!("Served Antialias for {} in {:?}", path,
//     start.elapsed());
//   }
//   match index.antialias_for(path).await {
//     Ok(line) => match cbor_to_json_str(&line) {
//       Ok(line) => Response {
//         status_code: 200,
//         headers: vec![("Content-Type".into(), "application/json".into())],
//         data: ResponseBody::from_string(line),
//         upgrade: None,
//       },
//       Err(_e) => Response {
//         status_code: 500,
//         headers: vec![],
//         data: ResponseBody::from_string("Error"),
//         upgrade: None,
//       },
//     },
//     _ => rouille::Response::json(&format!("Couldn't Anti-alias {}", path)).with_status_code(404),
//   }
// }

pub fn fix_path(p: String) -> String {
  if p.starts_with("/omnibor") {
    return fix_path(p[8..].to_string());
  } else if p.starts_with("/omnibor_test") {
    return fix_path(p[13..].to_string());
  } else if p.starts_with("/purl") {
    return fix_path(p[5..].to_string());
  } else if p.starts_with("/") {
    return fix_path(p[1..].to_string());
  }

  p
}

// pub async fn line_serve(index: &RodeoServer, _request: &Request, path: String) -> Response {
//   // FIXME -- deal with getting a raw MD5 hex string
//   let hash = md5hash_str(&path);
//   match index.data_for_hash(hash).await {
//     Ok(line) => match cbor_to_json_str(&line) {
//       Ok(line) => Response {
//         status_code: 200,
//         headers: vec![("Content-Type".into(), "application/json".into())],
//         data: ResponseBody::from_string(line),
//         upgrade: None,
//       },
//       Err(_e) => Response {
//         status_code: 500,
//         headers: vec![],
//         data: ResponseBody::from_string("Error"),
//         upgrade: None,
//       },
//     },
//     _ => rouille::Response::json(&format!("Not found {}", path)).with_status_code(404),
//   }
// }

// struct MyState {
//   pub name: String,
// }

// impl MyState {
//   pub fn get_name(&self) -> String {
//     self.name.clone()
//   }
// }

// async fn foo(State(s): State<Arc<MyState>>) -> String {
//   format!("Foo sez {}", s.get_name())
// }

fn build_route(state: Arc<RodeoServer>) -> Router {
  let app: Router<()> = Router::new()
    .route("/bulk", post(serve_bulk))
    .route("/:gitoid", get(serve_gitoid))
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
  // FIXME
  // rouille::start_server(
  //   addrs.as_slice(),
  //   move |request| {
  //     let start = Instant::now();
  //     let url = request.url();
  //     defer! {
  //       info!("Serving {} took {:?}", url,  start.elapsed());
  //     }

  //     let path = request.url();

  //     let path = fix_path(path);
  //     let path_str: &str = &path;
  //     match (request.method(), path_str) {
  //
  //       ("POST", "north") => north_serve(&index, request, None, false, start),
  //       ("POST", "north_purls") => north_serve(&index, request, None, true, start),
  //       ("GET", url) if url.starts_with("north/") => {
  //         north_serve(&index, request, Some(&url[6..]), false, start)
  //       }
  //       ("GET", url) if url.starts_with("north_purls/") => {
  //         north_serve(&index, request, Some(&url[12..]), true, start)
  //       }
  //       ("GET", url) if url.starts_with("aa/") => {
  //         serve_antialias(&index, request, &url[3..], start)
  //       }
  //       ("GET", _url) => line_serve(&index, request, path),
  //       _ => rouille::Response::empty_404(),
  //     }
  //   },
  // );
}

fn _split_path(path: String) -> Vec<String> {
  let mut segments = Vec::new();

  for s in path.split('/') {
    match s {
      "" | "." => {}
      ".." => {
        segments.pop();
      }
      s => segments.push(s.to_string()),
    }
  }

  segments
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
