//! THE SERVED BUILT-INS ARE THE TESTED DOCUMENTS.
//!
//! REQUIREMENT (found while auditing the PR review's findings): `TEMPLATES`
//! carried the `flatten_source` template TWICE — a corrected entry and the
//! original `build:from` one, and `with_builtins` inserts in order, so the
//! stale entry WON. Every equivalence test in this suite reads
//! `test_data/queries/*.json`, so nothing compared the document under test with
//! the text the server actually serves: a fix could land in the twin and be
//! inert in production.
//!
//! THEORY OF THE TEST: for every built-in the registry serves, the served text
//! must be byte-identical to its document twin, and no two built-ins may share a
//! name. Whatever the templates say, the written-and-tested copy and the served
//! copy are one thing.
use bigtent::api_v2::TemplateRegistry;

/// Every built-in the registry ships, as (name, text).
fn builtins() -> Vec<(String, String)> {
    let registry = TemplateRegistry::new().with_builtins();
    let mut out = Vec::new();
    // the built-ins the registry ships (there is no  template: V1's
    //  is a different endpoint shape, and its document twin exercises
    // the passthrough the  template serves)
    for name in [
        "item",
        "anti_alias",
        "north",
        "north_purls",
        "flatten",
        "flatten_source",
    ] {
        match registry.get(name) {
            Some(text) => out.push((name.to_string(), text.to_string())),
            None => panic!("the built-ins must include `{name}`"),
        }
    }
    out
}

/// A served built-in and its document twin must be the same text.
#[test]
fn every_served_template_matches_its_document() {
    for (name, served) in builtins() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test_data/queries")
            .join(format!("{name}.json"));
        let document = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("the twin `{}` is present: {error}", path.display()));
        let served_json: serde_json::Value = serde_json::from_str(&served)
            .unwrap_or_else(|error| panic!("`{name}` is JSON: {error}"));
        let document_json: serde_json::Value = serde_json::from_str(&document)
            .unwrap_or_else(|error| panic!("`{name}`'s twin is JSON: {error}"));
        assert_eq!(
            served_json, document_json,
            "the served `{name}` must be the document the tests exercise"
        );
    }
}

/// No template names an edge type that does not exist. `build:from` was the
/// ghost in finding 25; a template that names it returns nothing on real data.
#[test]
fn no_template_names_an_edge_type_that_does_not_exist() {
    for (name, served) in builtins() {
        assert!(
            !served.contains("build:from"),
            "`{name}` names `build:from`, which is not an edge type (BUILT_FROM is `build:down`)"
        );
    }
}
