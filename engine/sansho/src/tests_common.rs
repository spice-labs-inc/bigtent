//! The shared test-support fixture builder: the Appendix-A-shaped item
//! with parameterized connection and file-name counts (the plan's
//! Phase 0 — the machinery the benchmarks and the escape-case matrix
//! rest on; the spike's `SpikeItem::appendix_a` promoted into the
//! engine). Included via `#[path]` by the benches and the integration
//! tests; not part of the library's public surface.

use serde_json::Value as J;
use std::collections::BTreeMap;

/// The Appendix-A-shaped item:
/// - `connections`: a map of connection type → targets — `alias:from`
///   with `n_connections` entries (every other one `pkg:`-prefixed),
///   `contained:up` with a single parent entry;
/// - `body`: the goatrodeo metadata (`file_names` with `n_file_names`
///   entries, every third one gitoid-prefixed; `file_size`; `mime_type`;
///   `extra`).
pub fn appendix_a(n_connections: usize, n_file_names: usize) -> J {
    let mut alias_from: Vec<String> = Vec::with_capacity(n_connections);
    for i in 0..n_connections {
        if i % 2 == 0 {
            alias_from.push(format!("pkg:maven/org.example/art{i}@1.0.{i}"));
        } else {
            alias_from.push(format!("gitoid:blob:sha1:{i:040x}"));
        }
    }
    let file_names: Vec<String> = (0..n_file_names)
        .map(|i| {
            if i % 3 == 0 {
                format!("gitoid:blob:sha256:{i:064x}!$org/example/Thing{i}.java")
            } else {
                format!("org/example/Thing{i}.java")
            }
        })
        .collect();
    J::Object(
        [
            (
                "identifier".to_string(),
                J::String(format!("gitoid:blob:sha1:{}", "30e6".repeat(10))),
            ),
            (
                "connections".to_string(),
                J::Object(
                    BTreeMap::from([
                        (
                            "alias:from".to_string(),
                            J::Array(alias_from.into_iter().map(J::String).collect()),
                        ),
                        (
                            "contained:up".to_string(),
                            J::Array(vec![J::String("gitoid:blob:sha1:parent".to_string())]),
                        ),
                    ])
                    .into_iter()
                    .collect(),
                ),
            ),
            (
                "body_mime_type".to_string(),
                J::String("application/vnd.cc.goatrodeo".to_string()),
            ),
            (
                "body".to_string(),
                J::Object(
                    [
                        ("file_names".to_string(), J::Array(file_names.into_iter().map(J::String).collect())),
                        ("file_size".to_string(), J::Number(3050.into())),
                        ("mime_type".to_string(), J::Array(vec![J::String("text/x-java-source".to_string())])),
                        ("extra".to_string(), J::Object(Default::default())),
                    ]
                    .into_iter()
                    .collect(),
                ),
            ),
        ]
        .into_iter()
        .collect(),
    )
}

/// The connection count of an Appendix-A item (for assertions).
pub fn connection_count(item: &J) -> usize {
    item["connections"]["alias:from"].as_array().map(|a| a.len()).unwrap_or(0)
}

/// The file-name count of an Appendix-A item (for assertions).
pub fn file_name_count(item: &J) -> usize {
    item["body"]["file_names"].as_array().map(|a| a.len()).unwrap_or(0)
}
