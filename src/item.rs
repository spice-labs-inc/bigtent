//! # Item - Core Data Model
//!
//! This module defines the core data structures for BigTent's graph database:
//! [`Item`], [`Edge`], and [`ItemMetaData`].
//!
//! ## Items
//!
//! An [`Item`] represents a node in the graph, identified by a GitOID (Git Object ID).
//! Items can represent any software artifact: source files, packages, builds, etc.
//!
//! ## Edges
//!
//! Items are connected by typed edges. Each edge is a tuple of `(edge_type, target_id)`.
//!
//! ### Edge Types
//!
//! | Type | Direction | Meaning |
//! |------|-----------|---------|
//! | `alias:to` | → | This item is an alias for target |
//! | `alias:from` | ← | Target is an alias for this item |
//! | `contained:up` | ↑ | This item is contained by target |
//! | `contained:down` | ↓ | This item contains target |
//! | `build:up` | ↑ | This item was built into target |
//! | `build:down` | ↓ | This item was built from target |
//! | `tag:to` | → | This item tags target |
//! | `tag:from` | ← | This item is tagged by target |
//!
//! ## Merging
//!
//! When the same GitOID appears in multiple clusters, Items are merged:
//! - Connections are unioned
//! - Metadata is recursively merged
//! - Filename conflicts are resolved by prefixing with GitOID
//!
//! ## Example
//!
//! ```rust,ignore
//! use bigtent::item::{Item, CONTAINED_BY, ALIAS_TO};
//!
//! // An item representing a source file contained in a package
//! let item = Item {
//!     identifier: "gitoid:blob:sha256:abc123...".to_string(),
//!     connections: [
//!         (CONTAINED_BY.to_string(), "gitoid:blob:sha256:pkg456...".to_string())
//!     ].into_iter().collect(),
//!     body_mime_type: Some("application/vnd.cc.goatrodeo".to_string()),
//!     body: Some(metadata_value),
//! };
//! ```

use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde::{
    Deserialize, Serialize,
    de::{self, Visitor},
    ser::SerializeSeq,
};
use serde_cbor::{
    Value,
    value::{from_value, to_value},
};
use utoipa::ToSchema;

/// Recursively merge two CBOR Values.
///
/// This function implements the merge semantics for Item metadata:
///
/// ## Merge Rules
///
/// - **Arrays**: Union of elements (deduplicated via BTreeSet)
/// - **Maps**: Recursive merge of values with matching keys
/// - **Scalars**: First value wins (v1 takes precedence)
/// - **Null + X**: X wins
///
/// ## Special Case: file_names
///
/// The `file_names` field at the top level (depth=0) has special handling:
///
/// When the same GitOID has different filenames in different containers,
/// we need to disambiguate. The solution is to prefix filenames with the
/// container GitOID: `container_gitoid!filename`
///
/// This allows tracking which filename came from which container when
/// an artifact appears in multiple places with different names.
///
/// ## Parameters
///
/// - `v1`, `v2`: The values to merge
/// - `depth`: Current recursion depth (0 = top level)
/// - `v1_contains`, `v2_contains`: Closures returning container GitOIDs for filename disambiguation
fn merge_values<F: Fn() -> Vec<String>, F2: Fn() -> Vec<String>>(
    v1: &Value,
    v2: &Value,
    depth: usize,
    v1_contains: &F,
    v2_contains: &F2,
) -> Value {
    // Expand a filename Value into prefixed versions with container GitOIDs
    // e.g., "foo.rs" -> ["foo.rs", "gitoid123!foo.rs", "gitoid456!foo.rs"]
    fn fix(filename: &Value, gitoids: Vec<String>) -> Vec<String> {
        let expanded_filename = fix_uno(filename);
        let mut ret = vec![];
        for the_filename in expanded_filename {
            ret.push(the_filename.clone());
            for gitoid in &gitoids {
                ret.push(format!("{}!{}", gitoid, the_filename));
            }
        }

        ret
    }

    // Extract string(s) from a Value (handles both single strings and arrays)
    fn fix_uno(s: &Value) -> Vec<String> {
        match s {
            Value::Text(s) => vec![s.clone()],
            Value::Array(value_vec) => value_vec.iter().flat_map(|v| fix_uno(v)).collect(),
            _ => vec![],
        }
    }

    match (v1, v2) {
        (Value::Array(vec1), Value::Array(vec2)) => {
            let mut ret = BTreeSet::new();
            for i in vec1 {
                ret.insert(i.clone());
            }
            for i in vec2 {
                ret.insert(i.clone());
            }

            Value::Array(ret.into_iter().collect())
        }
        (Value::Map(map1), Value::Map(map2)) => {
            // let mut remaining = map2.clone();
            let mut ret = BTreeMap::new();
            for (k, v) in map1 {
                match (v, map2.get(k)) {
                    // special case for filenames
                    (Value::Array(names), Some(Value::Array(v2_names)))
                        if depth == 0 && k == &Value::Text("file_names".to_string()) =>
                    {
                        let mut map_names = BTreeSet::new();
                        for n in names {
                            map_names.insert(n);
                        }
                        let mut v2_map_names = BTreeSet::new();
                        for n in v2_names {
                            v2_map_names.insert(n);
                        }
                        let mut merged_file_names = map_names.clone();
                        merged_file_names.append(&mut v2_map_names.clone());

                        let clean_merge: BTreeSet<Value> =
                            match (merged_file_names.len(), names.len(), v2_names.len()) {
                                // if there's only one filename, then it's a clean merge
                                (1, _, _) => {
                                    merged_file_names.into_iter().map(|v| v.clone()).collect()
                                }

                                // if both have already done the filename to gitoid mapping, then it's safe to merge
                                (_, tsize, osize) if tsize > 1 && osize > 1 => {
                                    merged_file_names.into_iter().map(|v| v.clone()).collect()
                                }

                                // create the mapping
                                (_, tsize, osize) => {
                                    let mut v1_with_mapping = if tsize > 1 {
                                        fix_uno(&names[0])
                                    } else {
                                        fix(&names[0], v1_contains())
                                    };
                                    let mut v2_with_mapping = if osize > 1 {
                                        fix_uno(&v2_names[0])
                                    } else {
                                        fix(&v2_names[0], v2_contains())
                                    };
                                    v1_with_mapping.append(&mut v2_with_mapping);
                                    let mut the_final = BTreeSet::new();
                                    for s in v1_with_mapping {
                                        the_final.insert(Value::Text(s));
                                    }
                                    the_final
                                }
                            };
                        ret.insert(k.clone(), Value::Array(clean_merge.into_iter().collect()));
                    }

                    (v, Some(ov)) => {
                        let to_insert = merge_values(v, ov, depth + 1, v1_contains, v2_contains);
                        // remaining.remove(k);
                        ret.insert(k.clone(), to_insert);
                    }
                    (v, None) => {
                        ret.insert(k.clone(), v.clone());
                    }
                }
            }
            Value::Map(ret)
        }
        (Value::Null, _) => v2.clone(),
        // Anything else just becomes a clone of the left side of the merge
        _ => v1.clone(),
    }
}

#[test]
fn test_merge() {
    fn gimme_vec() -> Vec<String> {
        vec!["foo".into(), "bar".into()]
    }
    fn gimme_vec2() -> Vec<String> {
        vec!["baz".into(), "cat".into()]
    }

    let foo = Value::Text("foo".to_string());
    let v1 = Value::Array(vec![foo.clone()]);
    let bar = Value::Text("bar".to_string());
    let v2 = Value::Array(vec![bar.clone()]);
    let v1_2 = Value::Array(vec![bar.clone(), foo.clone()]);
    let m1 = Value::Map(BTreeMap::from([(foo.clone(), v1.clone())]));
    let m2 = Value::Map(BTreeMap::from([(foo.clone(), v2.clone())]));
    assert_eq!(foo, merge_values(&foo, &foo, 0, &gimme_vec, &gimme_vec2));
    assert_eq!(v1, merge_values(&v1, &foo, 0, &gimme_vec, &gimme_vec2));
    assert_eq!(v1, merge_values(&v1, &v1, 0, &gimme_vec, &gimme_vec2));
    assert_eq!(
        v1_2.clone(),
        merge_values(&v1, &v2, 0, &gimme_vec, &gimme_vec2)
    );
    let merge1 = merge_values(&m1, &m2, 0, &gimme_vec, &gimme_vec2);
    let merged_key = match merge1 {
        Value::Map(map) => {
            let m2 = map.clone();
            m2.get(&foo).map(|v| v.clone())
        }
        _ => None,
    };

    assert_eq!(Some(v1_2.clone()), merged_key);

    let a = Value::Map(BTreeMap::from([(
        Value::Text("file_names".to_string()),
        Value::Array(vec![Value::Text("foo".to_string())]),
    )]));
    let b = Value::Map(BTreeMap::from([(
        Value::Text("file_names".to_string()),
        Value::Array(vec![Value::Text("bar".to_string())]),
    )]));

    let merged_aa = merge_values(&a, &a, 0, &gimme_vec, &gimme_vec2);

    assert_eq!(a, merged_aa, "merging with self should be same");

    let merged_ab = merge_values(&a, &b, 0, &gimme_vec, &gimme_vec2);
    assert_ne!(a, merged_ab, "They should differ");
    let size = match merged_ab {
        Value::Map(m) => {
            match m
                .get(&Value::Text("file_names".to_string()))
                .expect("Should find file_names")
            {
                Value::Array(ar) => ar.len(),
                _ => panic!("'files_names' is not an array"),
            }
        }
        _ => {
            panic!("merged_ab is not a map")
        }
    };
    assert!(
        size == 6,
        "there should be 6 different filenames, but got {}",
        size
    )
}

pub trait EdgeType {
    fn is_alias_from(&self) -> bool;

    fn is_alias_to(&self) -> bool;

    fn is_from(&self) -> bool;

    fn is_to(&self) -> bool;

    fn is_up(&self) -> bool;

    fn is_down(&self) -> bool;

    fn is_contains_down(&self) -> bool;

    fn is_contained_by_up(&self) -> bool;

    fn is_built_from(&self) -> bool;

    fn is_builds_to(&self) -> bool;

    fn is_tag_to(&self) -> bool;

    fn is_tag_from(&self) -> bool;
}

pub const TO: &str = ":to";
pub const FROM: &str = ":from";
pub const DOWN: &str = ":down";
pub const UP: &str = ":up";

pub const CONTAINED_BY: &str = "contained:up";
pub const CONTAINS: &str = "contained:down";
pub const ALIAS_TO: &str = "alias:to";
pub const ALIAS_FROM: &str = "alias:from";
pub const BUILDS_TO: &str = "build:up";
pub const BUILT_FROM: &str = "build:down";
pub const TAG_FROM: &str = "tag:from";
pub const TAG_TO: &str = "tag:to";

impl EdgeType for String {
    fn is_alias_from(&self) -> bool {
        self == ALIAS_FROM
    }

    fn is_alias_to(&self) -> bool {
        self == ALIAS_TO
    }

    fn is_from(&self) -> bool {
        self.ends_with(FROM)
    }

    fn is_to(&self) -> bool {
        self.ends_with(TO)
    }

    fn is_up(&self) -> bool {
        self.ends_with(UP)
    }

    fn is_contains_down(&self) -> bool {
        self.ends_with(CONTAINS)
    }

    fn is_contained_by_up(&self) -> bool {
        self.ends_with(CONTAINED_BY)
    }

    fn is_built_from(&self) -> bool {
        self.ends_with(BUILT_FROM)
    }

    fn is_builds_to(&self) -> bool {
        self.ends_with(BUILDS_TO)
    }

    fn is_down(&self) -> bool {
        self.ends_with(DOWN)
    }

    fn is_tag_to(&self) -> bool {
        self == TAG_TO
    }

    fn is_tag_from(&self) -> bool {
        self == TAG_FROM
    }
}

pub type Edge = (String, String);

/// A node in the BigTent graph database representing a software artifact.
///
/// Items are identified by GitOIDs and connected to other items via typed edges.
/// The `body` field contains optional metadata in CBOR format.
#[derive(Debug, Deserialize, Serialize, Clone, ToSchema)]
#[schema(example = json!({
    "identifier": "gitoid:blob:sha256:a1b2c3d4e5f6...",
    "connections": [["contained:up", "gitoid:blob:sha256:parent..."]],
    "body_mime_type": "application/vnd.cc.goatrodeo",
    "body": {"file_names": ["example.rs"], "file_size": 1234}
}))]
pub struct Item {
    /// The unique GitOID identifier for this item
    pub identifier: String,
    /// Set of typed edges connecting this item to other items.
    /// Each edge is a tuple of (edge_type, target_identifier).
    /// Edge types include: "alias:to", "alias:from", "contained:up", "contained:down",
    /// "build:up", "build:down", "tag:to", "tag:from"
    #[schema(value_type = Vec<(String, String)>)]
    pub connections: BTreeSet<Edge>,
    /// MIME type of the body content, typically "application/vnd.cc.goatrodeo" for metadata
    pub body_mime_type: Option<String>,
    /// Optional metadata body in CBOR format, serialized as JSON in API responses
    #[schema(value_type = Option<serde_json::Value>)]
    pub body: Option<Value>,
}

impl From<Item> for serde_json::Value {
    fn from(value: Item) -> Self {
        value.to_json()
    }
}

impl From<&Item> for serde_json::Value {
    fn from(value: &Item) -> Self {
        value.to_json()
    }
}

impl Item {
    /// Make a list of all the gitoids that this Item contains
    pub fn list_contains(&self) -> Vec<String> {
        self.connections
            .iter()
            .filter(|v| v.0.is_contained_by_up())
            .map(|v| v.1.clone())
            .collect()
    }
    /// is the item a "root" item... no "up" or "tag:from"
    pub fn is_root_item(&self) -> bool {
        if self.body_mime_type != Some("application/vnd.cc.goatrodeo".to_string()) {
            return false;
        }
        if self.identifier == "tags" {
            return false;
        }

        for v in &self.connections {
            if v.0.is_alias_to() {
                return false;
            }
            if v.0.is_contained_by_up() {
                return false;
            }
            if v.0.is_tag_from() {
                return true;
            }
        }
        true
    }
    /// Find all the aliases that are package URLs
    pub fn find_purls(&self) -> Vec<String> {
        self
            // from connections
            .connections
            .iter()
            // find all aliases to this thing that start with `pkg:`
            .filter(|c| c.0.is_alias_from() && c.1.starts_with("pkg:"))
            // make into a string
            .map(|c| c.1.clone())
            // turn into a Vec<String>
            .collect()
    }

    pub fn is_alias(&self) -> bool {
        self.connections.iter().any(|v| v.0.is_alias_to())
    }
    // pub fn remove_references(&mut self) {
    //   self.reference = Item::NOOP;
    // }

    /// get all the connections that are either `contained_by_up` or `is_alias_to` or `is_tag_from`
    pub fn contained_by(&self) -> HashSet<String> {
        let mut ret = HashSet::new();
        for edge in self.connections.iter() {
            if edge.0.is_alias_to() || edge.0.is_tag_from() || edge.0.is_up() {
                ret.insert(edge.1.clone());
            }
        }

        ret
    }

    pub fn to_json(&self) -> serde_json::Value {
        let mut ret = serde_json::to_value(self).expect("Should be able to serialize Item");
        match ret {
            serde_json::Value::Object(mut m) => {
                m.remove_entry("reference");
                ret = serde_json::Value::Object(m)
            }
            _ => {}
        }
        ret
    }

    // tests two items... they are the same if they have the same
    // `identifier`, `connections`, `metadata`, `version`,
    // and `_type`
    pub fn is_same(&self, other: &Item) -> bool {
        self.identifier == other.identifier
            && self.connections == other.connections
            && self.body_mime_type == other.body_mime_type
            && self.body == other.body
    }

    pub fn merge_items(mut items: Vec<Item>) -> Option<Item> {
        let mut ret: Option<Item> = None;
        while let Some(to_merge) = items.pop() {
            ret = Some(match ret {
                None => to_merge,
                Some(a) => a.merge(to_merge),
            });
        }
        ret
    }

    // merge to `Item`s
    pub fn merge(&self, other: Item) -> Item {
        let (body, mime_type) = match (
            &self.body,
            other.body.clone(),
            self.body_mime_type == other.body_mime_type,
        ) {
            (None, None, _) => (None, None),
            (None, Some(a), _) => (Some(a), other.body_mime_type.clone()),
            (Some(a), None, _) => (Some(a.clone()), self.body_mime_type.clone()),
            (Some(a), Some(b), true)
                if self.body_mime_type.iter().map(|mt| mt as &str).last()
                    == Some(ITEM_METADATA_MIME_TYPE) =>
            {
                match (from_value(a.clone()), from_value(b.clone())) {
                    (Ok::<ItemMetaData, _>(ai), Ok::<ItemMetaData, _>(bi)) => {
                        let merged = ai.merge(bi, &self, &other);
                        (to_value(merged).ok(), self.body_mime_type.clone())
                    }

                    // if we can't deserialize, then try merging the CBOR
                    _ => (
                        Some(merge_values(a, &b, 0, &|| self.list_contains(), &|| {
                            other.list_contains()
                        })),
                        self.body_mime_type.clone(),
                    ),
                }
            }
            (Some(a), Some(b), true) => (
                Some(merge_values(
                    a,
                    &b,
                    0,
                    &|| {
                        self.connections
                            .iter()
                            .filter(|v| v.0.is_contained_by_up())
                            .map(|v| v.1.to_string())
                            .collect()
                    },
                    &|| {
                        other
                            .connections
                            .iter()
                            .filter(|v| v.0.is_contained_by_up())
                            .map(|v| v.1.to_string())
                            .collect()
                    },
                )),
                self.body_mime_type.clone(),
            ),
            _ => (self.body.clone(), self.body_mime_type.clone()),
        };

        Item {
            identifier: self.identifier.clone(),
            // reference: self.reference,
            connections: {
                let mut it = self.connections.clone();
                it.extend(other.connections);
                it
            },
            body_mime_type: mime_type,
            body: body,
        }
    }
}

pub const ITEM_METADATA_MIME_TYPE: &str = "application/vnd.cc.goatrodeo";

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum StringOrPair {
    Str(String),
    Pair(String, String),
}

struct StringOrPairVisitor;
impl<'de> Visitor<'de> for StringOrPairVisitor {
    type Value = StringOrPair;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("Expecting a string or an array of string")
    }

    fn visit_str<E>(self, cmd_str: &str) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(StringOrPair::Str(cmd_str.into()))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(StringOrPair::Str(v))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        match (seq.next_element(), seq.next_element()) {
            (Ok(Some::<String>(s1)), Ok(Some::<String>(s2))) => Ok(StringOrPair::Pair(s1, s2)),
            _ => Err(de::Error::custom("Expecting a tuple of strings")),
        }
    }
}
impl<'de> Deserialize<'de> for StringOrPair {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let res = deserializer.deserialize_any(StringOrPairVisitor)?;
        Ok(res)
    }
}

impl Serialize for StringOrPair {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            StringOrPair::Str(v) => serializer.serialize_str(v),
            StringOrPair::Pair(a, b) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element(a)?;
                seq.serialize_element(b)?;

                seq.end()
            }
        }
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct ItemMetaData {
    pub file_names: BTreeSet<String>,
    pub file_size: u64,
    pub mime_type: BTreeSet<String>,
    pub extra: BTreeMap<String, BTreeSet<StringOrPair>>,
}

impl ItemMetaData {
    pub fn merge(&self, other: ItemMetaData, self_item: &Item, other_item: &Item) -> ItemMetaData {
        fn fix(filename: String, gitoids: Vec<String>) -> BTreeSet<String> {
            let mut ret: BTreeSet<String> = gitoids
                .iter()
                .map(|go| format!("{go}!${filename}"))
                .collect();

            ret.insert(filename);

            ret
        }

        let mut merged_filenames = self.file_names.clone();
        merged_filenames.append(&mut other.file_names.clone());

        let resolved_filenames = match (
            merged_filenames.len(),
            self.file_names.len(),
            other.file_names.len(),
        ) {
            // if there's only one filename, then it's a clean merge
            (1, _, _) => merged_filenames,

            // if both have already done the filename to gitoid mapping, then it's safe to merge
            (_, tsize, osize) if tsize > 1 && osize > 1 => merged_filenames,

            // create the mapping
            (_, tsize, osize) => {
                let mut this_with_mapping: BTreeSet<String> = if tsize > 1 {
                    self.file_names.clone()
                } else if tsize == 0 {
                    BTreeSet::new()
                } else {
                    fix(
                        self.file_names.first().expect("Just tested").clone(),
                        self_item.list_contains(),
                    )
                };
                let mut other_with_mapping = if osize > 1 {
                    other.file_names.clone()
                } else if osize == 0 {
                    BTreeSet::new()
                } else {
                    fix(
                        other.file_names.first().expect("Just tested").clone(),
                        other_item.list_contains(),
                    )
                };
                this_with_mapping.append(&mut other_with_mapping);
                this_with_mapping
            }
        };

        ItemMetaData {
            file_names: resolved_filenames,
            mime_type: {
                let mut it = self.mime_type.clone();
                it.extend(other.mime_type.clone());
                it
            },
            file_size: self.file_size,
            extra: {
                let mut it = self.extra.clone();
                for (k, v) in other.extra.iter() {
                    let v = v.clone();
                    let the_val = match it.get(k) {
                        Some(mine) => {
                            let mut tmp = mine.clone();
                            tmp.extend(v);
                            tmp
                        }
                        None => v,
                    };

                    it.insert(k.clone(), the_val);
                }
                it
            },
        }
    }
}
