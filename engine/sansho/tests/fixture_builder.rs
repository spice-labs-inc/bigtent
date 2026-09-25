//! Phase 0, T0.2: the fixture builder generates the parameterized
//! shapes exactly (the benches and the escape-case matrix rest on it).

#[path = "../src/tests_common.rs"]
mod tests_common;

use tests_common::*;

#[test]
fn fixture_builder_generates_shapes() {
    let item = appendix_a(25_000, 12_000);
    assert_eq!(connection_count(&item), 25_000, "connection count exact");
    assert_eq!(file_name_count(&item), 12_000, "file-name count exact");

    let alias: Vec<&str> = item["connections"]["alias:from"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    let pkg_count = alias.iter().filter(|s| s.starts_with("pkg:")).count();
    let gitoid_count = alias.iter().filter(|s| s.starts_with("gitoid:")).count();
    assert_eq!(pkg_count, 12_500, "every other connection is pkg:");
    assert_eq!(gitoid_count, 12_500, "every other connection is gitoid:");

    // the scan shape's contract: the north_purls emit over this item
    // yields exactly the pkg: entries, in order
    let parsed = sansho::parse("connections.\"alias:from\"[?starts_with(@, 'pkg:')]").unwrap();
    let program = sansho::compile(&parsed).unwrap();
    let result = sansho::evaluate_json(&program, &item).unwrap();
    let rows = result.as_array().unwrap();
    assert_eq!(rows.len(), 12_500, "one row per pkg: entry");
    assert!(rows[0].as_str().unwrap().starts_with("pkg:"), "rows are pkg: entries");
}
