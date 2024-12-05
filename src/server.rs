use std::{sync::Arc, time::Instant};

use anyhow::{bail, Result};
#[cfg(not(test))]
use log::info;
use rouille::{Request, Response, ResponseBody};
use scopeguard::defer;
use serde_json::Value as SJValue; // Use log crate when building application

use crate::{
  rodeo_server::RodeoServer,
  util::{cbor_to_json_str, md5hash_str, read_all},
};
#[cfg(test)]
use std::println as info;

pub fn parse_body_to_json(request: &Request) -> Result<SJValue> {
  let count = match request.header("Content-Length") {
    Some(v) => match usize::from_str_radix(v, 10) {
      Ok(n) => n,
      _ => {
        bail!("Couldn't get bytes count for content length");
      }
    },
    _ => {
      bail!("Content length header required");
    }
  };

  let mut body = match request.data() {
    Some(v) => v,
    None => bail!("Request has no body"),
  };

  let bytes = read_all(&mut body, count)?;
  let ret = serde_json::from_slice::<SJValue>(&bytes).map_err(|e| e.into());
  ret
}

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

fn serve_bulk(index: &RodeoServer, bulk_data: Vec<String>) -> Result<Response> {
  let (rx, tx) = pipe::pipe();
  index.bulk_serve(bulk_data, tx, Instant::now())?;
  Ok(Response {
    status_code: 200,
    headers: vec![("Content-Type".into(), "application/json".into())],
    data: ResponseBody::from_reader(rx),
    upgrade: None,
  })
}

pub fn basic_bulk_serve(index: &RodeoServer, request: &Request, start: Instant) -> Response {
  let body = value_to_string_array(parse_body_to_json(request));

  let cnt_string = body
    .as_ref()
    .map(|b| b.len().to_string())
    .unwrap_or("Failed to parse body".into());

  defer! {
    info!("Served bulk for {} items in {:?}",cnt_string,
    start.elapsed());
  }

  match body {
    Ok(v) if v.len() <= 420 => match serve_bulk(index, v) {
      Ok(r) => r,
      Err(e) => rouille::Response::json(&format!("Error {}", e)).with_status_code(400),
    },
    Ok(v) => rouille::Response::json(&format!("Bulk request for too many elements {}", v.len())).with_status_code(400),
    Err(e) => rouille::Response::json(&format!("Error {}", e)).with_status_code(400),
  }
}

pub fn north_serve(
  index: &RodeoServer,
  request: &Request,
  path: Option<&str>,
  purls_only: bool,
  start: Instant,
) -> Response {
  let to_serve: Vec<String> = if request.method() == "POST" {
    let body = value_to_string_array(parse_body_to_json(request));

    match body {
      Ok(v) if v.len() <= 6000 => v,
      Ok(v) => return rouille::Response::json(&format!("Bulk request for too many elements {}", v.len())).with_status_code(400),
      Err(e) => return rouille::Response::json(&format!("Error {}", e)).with_status_code(400),
    }
  } else {
    match path {
      Some(p) => vec![p.into()],
      _ => return rouille::Response::empty_404(),
    }
  };

  let serve_clone = to_serve.clone();

  defer! {
    info!("Served North for {:?} in {:?}", serve_clone,
    start.elapsed());
  }

  let (rx, tx) = pipe::pipe();
  match index.do_north_serve(to_serve, purls_only, tx, Instant::now()) {
    Ok(_) => Response {
      status_code: 200,
      headers: vec![("Content-Type".into(), "application/json".into())],
      data: ResponseBody::from_reader(rx),
      upgrade: None,
    },
    _ => Response {
      status_code: 500,
      headers: vec![],
      data: ResponseBody::from_string("Failed to serve north"),
      upgrade: None,
    },
  }
}

pub fn serve_antialias(
  index: &RodeoServer,
  _request: &Request,
  path: &str,
  start: Instant,
) -> Response {
  defer! {
    info!("Served Antialias for {} in {:?}", path,
    start.elapsed());
  }
  match index.antialias_for(path) {
    Ok(line) => match cbor_to_json_str(&line) {
      Ok(line) => Response {
        status_code: 200,
        headers: vec![("Content-Type".into(), "application/json".into())],
        data: ResponseBody::from_string(line),
        upgrade: None,
      },
      Err(_e) => Response {
        status_code: 500,
        headers: vec![],
        data: ResponseBody::from_string("Error"),
        upgrade: None,
      },
    },
    _ => rouille::Response::json(&format!("Couldn't Anti-alias {}", path)).with_status_code(404),
  }
}

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

pub fn line_serve(index: &RodeoServer, _request: &Request, path: String) -> Response {
  // FIXME -- deal with getting a raw MD5 hex string
  let hash = md5hash_str(&path);
  match index.data_for_hash(hash) {
    Ok(line) => match cbor_to_json_str(&line) {
      Ok(line) => Response {
        status_code: 200,
        headers: vec![("Content-Type".into(), "application/json".into())],
        data: ResponseBody::from_string(line),
        upgrade: None,
      },
      Err(_e) => Response {
        status_code: 500,
        headers: vec![],
        data: ResponseBody::from_string("Error"),
        upgrade: None,
      },
    },
    _ => rouille::Response::json(&format!("Not found {}", path)).with_status_code(404),
  }
}

pub fn run_web_server(index: Arc<RodeoServer>) -> () {
  rouille::start_server(
    index.the_args().to_socket_addrs().as_slice(),
    move |request| {
      let start = Instant::now();
      let url = request.url();
      defer! {
        info!("Serving {} took {:?}", url,  start.elapsed());
      }

      let path = request.url();

      let path = fix_path(path);
      let path_str: &str = &path;
      match (request.method(), path_str) {
        ("POST", "bulk") => basic_bulk_serve(&index, request, start),
        ("POST", "north") => north_serve(&index, request, None, false, start),
        ("POST", "north_purls") => north_serve(&index, request, None, true, start),
        ("GET", url) if url.starts_with("north/") => {
          north_serve(&index, request, Some(&url[6..]), false, start)
        }
        ("GET", url) if url.starts_with("north_purls/") => {
          north_serve(&index, request, Some(&url[12..]), true, start)
        }
        ("GET", url) if url.starts_with("aa/") => {
          serve_antialias(&index, request, &url[3..], start)
        }
        ("GET", _url) => line_serve(&index, request, path),
        _ => rouille::Response::empty_404(),
      }
    },
  );
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

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_parse_body_to_json() {
    let request_body_json = serde_json::json!({
      "foo": "bar",
      "baz": 42
    });

    let request_body_vec = request_body_json.to_string().as_bytes().to_vec();

    let request = Request::fake_http(
      "POST",
      "/",
      vec![
        ("Content-Type".to_owned(), "application/json".to_owned()),
        (
          "Content-Length".to_owned(),
          request_body_vec.len().to_string().to_owned(),
        ),
      ],
      request_body_vec,
    );

    let result = parse_body_to_json(&request);

    assert_eq!(result.expect("error"), request_body_json);
  }
}
