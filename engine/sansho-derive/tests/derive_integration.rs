//! The derive's integration test: a derived struct used as a Sansho
//! source, through the public entry points.
//!
//! The test is about navigation results, not generated code: a struct's
//! fields ARE the document's members, so what it asserts is what a
//! JMESPath expression answers over that struct.

use sansho::view::SanshoTrait;
use sansho_derive::SanshoObject;
use std::collections::BTreeSet;

/// A nested object. Its own derived implementation makes `&Inner` a
/// source, so an `Inner` field of another struct is a member like any
/// other.
#[derive(SanshoObject)]
struct Inner {
    label: String,
    count: u64,
}

/// One field of every shape the derive maps: a borrowed string, a
/// renamed field, an optional string, the Copy scalars, a nested derived
/// struct, an in-memory JSON value, a container, and a skipped field.
#[derive(SanshoObject)]
struct Outer {
    identifier: String,
    #[serde(rename = "bodyMimeType")]
    body_mime_type: String,
    optional: Option<String>,
    count: u64,
    flag: bool,
    inner: Inner,
    payload: serde_json::Value,
    tags: BTreeSet<String>,
    #[serde(skip)]
    scratch: Vec<u8>,
}

/// The document under test.
fn fixture() -> Outer {
    let mut tags = BTreeSet::new();
    tags.insert("alpha".to_string());
    tags.insert("beta".to_string());
    Outer {
        identifier: "gitoid:blob:sha1:abc".to_string(),
        body_mime_type: "application/vnd.cc.goatrodeo".to_string(),
        optional: Some("present".to_string()),
        count: 7,
        flag: true,
        inner: Inner {
            label: "nested".to_string(),
            count: 2,
        },
        payload: serde_json::json!({"x": [1, 2, 3]}),
        tags,
        scratch: vec![0xff, 0xfe],
    }
}

/// Evaluate one expression over the fixture through the derived source
/// and materialize the result — the same shape a consumer uses.
fn query(expression: &str) -> serde_json::Value {
    let program = sansho::compile(&sansho::parse(expression).unwrap(), None).unwrap();
    let document = fixture();
    sansho::evaluate(&program, &document)
        .and_then(|value| sansho::materialize(&value))
        .expect("the derived source evaluates")
}

// Requirement: the derive is the generic path from a Rust struct to a
// JMESPath source — Sansho is not tied to any one consumer's item type.
// What: a derived struct queried through the public entry points, one
// expression per field shape. Why: the derive's contract is that the
// struct's fields are the document's members, so each shape has to answer
// as its JMESPath type: a `String` as a string, a `u64` as a number, a
// nested derived struct as an object to descend into, and so on.
//
// LLM section: the shapes covered, in order — borrowed `String`,
// `#[serde(rename)]`, `Option<String>`, a Copy `u64`, a Copy `bool`, a
// nested derived struct (two levels of navigation), a
// `serde_json::Value` (its own tree), a `BTreeSet<String>` (a
// container), a missing name (a miss, not an error), and a
// `#[serde(skip)]` field (not a member).
#[test]
fn derived_struct_is_a_jmespath_source() {
    assert_eq!(
        query("identifier"),
        serde_json::json!("gitoid:blob:sha1:abc")
    );
    // the serde rename is the member name
    assert_eq!(
        query("bodyMimeType"),
        serde_json::json!("application/vnd.cc.goatrodeo")
    );
    assert_eq!(query("optional"), serde_json::json!("present"));
    assert_eq!(query("count"), serde_json::json!(7));
    assert_eq!(query("flag"), serde_json::json!(true));
    // a nested derived struct: navigation descends into it
    assert_eq!(query("inner.label"), serde_json::json!("nested"));
    assert_eq!(query("inner.count"), serde_json::json!(2));
    // an in-memory JSON value walks its own tree
    assert_eq!(query("payload.x[1]"), serde_json::json!(2));
    // a container field answers as a container
    assert_eq!(query("length(tags)"), serde_json::json!(2));
    assert_eq!(query("contains(tags, 'alpha')"), serde_json::json!(true));
    // a name the struct does not have is a miss, not an error
    assert_eq!(query("no_such_field"), serde_json::Value::Null);
    // a skipped field is not a member — and it HAS content, so what
    // keeps it out of the document is the skip rather than emptiness
    assert!(!fixture().scratch.is_empty());
    assert_eq!(query("scratch"), serde_json::Value::Null);
}

// Requirement: the member set is the field set — a rename renames the
// member, a skip removes it, and the object's length is the member count.
// What: the object's own length and its key set, queried over the
// derived struct. Why: `as_object` and `container_len` are what the
// evaluator's object paths read, and a field that is present in the
// struct but absent from the document (or the reverse) is a silent
// wrong answer rather than an error.
//
// LLM section: eight fields are members (the nine declared fields less
// the skipped one). The key set is asserted through `contains` rather
// than by position, because the engine's object iteration order is not
// part of a value.
#[test]
fn the_member_set_is_the_field_set() {
    assert_eq!(query("length(@)"), serde_json::json!(8));
    assert_eq!(query("contains(keys(@), 'bodyMimeType')"), serde_json::json!(true));
    assert_eq!(query("contains(keys(@), 'body_mime_type')"), serde_json::json!(false));
    assert_eq!(query("contains(keys(@), 'scratch')"), serde_json::json!(false));
    assert_eq!(query("contains(keys(@), 'inner')"), serde_json::json!(true));
    assert_eq!(query("contains(keys(@), 'tags')"), serde_json::json!(true));
}

// Requirement: a derived struct renders as its members. What: the
// `to_string` rendering of the derived document. Why: `to_string_sansho`
// is the trait method the `to_string` function reads, and the derive has
// to supply it — the trait declares it without a default, so a wrong
// rendering is a wrong `to_string` result rather than a missing method.
//
// LLM section: the rendering names each member; the skipped field is
// absent from it. The assertions read the rendered text, which is a
// runtime value produced by the source — not the source code.
#[test]
fn the_rendering_names_the_members() {
    let rendered = query("to_string(@)");
    let text = rendered.as_str().expect("the rendering is a string");
    for member in ["identifier", "bodyMimeType", "optional", "count", "inner", "tags"] {
        assert!(
            text.contains(member),
            "the rendering must name the member {member}: {text}"
        );
    }
    assert!(
        !text.contains("scratch"),
        "a skipped field must not render: {text}"
    );
}

// Requirement: navigation and iteration answer the same document. What:
// a member reached by name (`get_key`) and the same member reached
// through the member map (`as_object`) are equal, for every member.
// Why: the two paths are generated separately — `get_key`'s match arms
// and `as_object`'s inserts — so a field added to one and not the other
// would make the document answer differently depending on how it was
// read.
//
// LLM section: the comparison is by value over each member name, so the
// test is independent of the map's iteration order. The member names are
// the ones the derive produced (the rename, not the Rust field name).
#[test]
fn navigation_and_iteration_agree() {
    let document = fixture();
    let object: &Outer = &document;

    let members = SanshoTrait::as_object(&object).expect("the derived object iterates");
    assert_eq!(members.len(), 8, "the member map holds every non-skipped field");

    for (name, member) in &members {
        let found = SanshoTrait::get_key(&object, name)
            .expect("the lookup answers")
            .expect("every iterated member is also reachable by name");
        let iterated = sansho::materialize(member).expect("the member materializes");
        let looked_up = sansho::materialize(&found).expect("the member materializes");
        assert_eq!(
            looked_up, iterated,
            "the member {name} must be the same value on both paths"
        );
    }

    // and a name that is not a member answers a miss
    assert!(
        SanshoTrait::get_key(&object, "scratch")
            .expect("the lookup answers")
            .is_none(),
        "a skipped field is not reachable by name"
    );
}
