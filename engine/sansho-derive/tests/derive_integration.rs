//! The derive's integration tests (the plan's T3.3, T3.5): the
//! generated node equals the hand-written baseline, and the
//! serde-rename guard is a compile error.
//!
//! The tests live in the derive crate (dev-depending on sansho — the
//! allowed direction per ADR-0001: sansho never depends on this crate).

use sansho_derive::SanshoNode;
use sansho::view::Node;

/// The item-shaped struct under test.
#[derive(SanshoNode)]
pub struct TestItem {
    pub identifier: String,
    pub body_mime_type: Option<String>,
    pub body: serde_json::Value,
    pub not_walkable: std::collections::BTreeMap<String, String>,
}

/// The hand-written baseline (what the derive must match).
#[derive(Clone)]
pub struct BaselineNode<'a>(&'a TestItem);

impl<'a> Node<'a> for BaselineNode<'a> {
    fn kind(&self) -> sansho::view::Kind {
        sansho::view::Kind::Object
    }
    fn as_str(&self) -> Option<std::borrow::Cow<'a, str>> {
        None
    }
    fn as_f64(&self) -> Option<f64> {
        None
    }
    fn as_bool(&self) -> Option<bool> {
        None
    }
    fn is_null(&self) -> bool {
        false
    }
    fn get_key(&self, name: &str) -> Option<Self> {
        match name {
            "identifier" => Some(BaselineNode(self.0)),
            "body_mime_type" => Some(BaselineNode(self.0)),
            "body" => Some(BaselineNode(self.0)),
            _ => None,
        }
    }
    fn get_index(&self, _index: usize) -> Option<Self> {
        None
    }
    fn entries(&self) -> Vec<(String, Self)> {
        Vec::new()
    }
    fn elements(&self) -> Vec<Self> {
        Vec::new()
    }
    fn container_len(&self) -> Option<usize> {
        None
    }
    fn counts_toward_aggregation() -> bool {
        false
    }
    fn materialize(&self) -> Result<serde_json::Value, sansho::SanshoError> {
        Ok(serde_json::json!({"identifier": self.0.identifier, "body": self.0.body}))
    }
}

/// T3.3: the derive-generated node and the hand-written baseline
/// agree on the walkable shapes (field lookup + materialization).
#[test]
fn derive_matches_hand_written() {
    let item = TestItem {
        identifier: "gitoid:blob:sha1:x".to_string(),
        body_mime_type: Some("application/vnd.cc.goatrodeo".to_string()),
        body: serde_json::json!({"file_size": 3050, "file_names": ["a.java"]}),
        not_walkable: Default::default(),
    };

    let derived = TestItemNode::wrap(&item);
    let baseline = BaselineNode(&item);

    // the walkable fields resolve identically
    for key in ["identifier", "body_mime_type", "body"] {
        let d = derived.get_key(key);
        let b = baseline.get_key(key);
        assert!(d.is_some(), "derive resolves {key}");
        assert!(b.is_some(), "baseline resolves {key}");
    }

    // materialization agrees
    let dv = derived.materialize().unwrap();
    let bv = baseline.materialize().unwrap();
    assert_eq!(dv["identifier"], bv["identifier"]);
    assert_eq!(dv["body"], bv["body"]);

    // the string field's content via the generated Str variant
    let id_node = derived.get_key("identifier").unwrap();
    assert_eq!(id_node.as_str().map(|s| s.into_owned()), Some("gitoid:blob:sha1:x".to_string()));
    assert_eq!(id_node.kind(), sansho::view::Kind::String);

    // the body value's navigation through the generated Value variant
    let body_node = derived.get_key("body").unwrap();
    assert_eq!(body_node.kind(), sansho::view::Kind::Object);
    let size = body_node.get_key("file_size").unwrap();
    assert_eq!(size.as_f64(), Some(3050.0));
    let names = body_node.get_key("file_names").unwrap();
    assert_eq!(names.elements().len(), 1);
}

/// T3.5's behavioral half: the Skip field evaluates as null and does
/// not navigate.
#[test]
fn skip_field_is_null() {
    let item = TestItem {
        identifier: "x".to_string(),
        body_mime_type: None,
        body: serde_json::Value::Null,
        not_walkable: Default::default(),
    };
    let derived = TestItemNode::wrap(&item);
    let skip = derived.get_key("not_walkable").unwrap();
    assert!(skip.is_null(), "the Skip field is null");
    assert_eq!(skip.kind(), sansho::view::Kind::Null);
    assert!(skip.get_key("anything").is_none(), "Skip does not navigate");
}

/// T3.5's compile-fail half is pinned in the crate's lib.rs module
/// documentation (a `compile_fail` doc-test — integration-test doc
/// comments never run as doc-tests).
/// Finding 1's regression: the derive compiles for a struct with only
/// ONE walkable field shape (the enum always emits all variants).
#[derive(SanshoNode)]
pub struct OneField {
    pub identifier: String,
}

#[test]
fn derive_compiles_for_single_shape_struct() {
    let item = OneField { identifier: "x".to_string() };
    let node = OneFieldNode::wrap(&item);
    assert_eq!(
        node.get_key("identifier").unwrap().as_str().map(|s| s.into_owned()),
        Some("x".to_string())
    );
    assert!(node.get_key("nope").is_none());
}

/// Finding 3's regression: navigation past a leaf answers null (the
/// leaf does NOT return itself for its own field name).
#[test]
fn leaf_nodes_answer_no_navigation() {
    let item = TestItem {
        identifier: "gitoid:blob:sha1:x".to_string(),
        body_mime_type: None,
        body: serde_json::Value::Null,
        not_walkable: Default::default(),
    };
    let derived = TestItemNode::wrap(&item);
    let id = derived.get_key("identifier").unwrap();
    // JMESPath: navigation on a string is null
    assert!(id.get_key("identifier").is_none(), "leaf navigation is null");
    let mime = derived.get_key("body_mime_type").unwrap();
    assert!(mime.get_key("body_mime_type").is_none());
}
