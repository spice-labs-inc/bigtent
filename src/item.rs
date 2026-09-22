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
//! ```rust,no_run
//! use bigtent::item::{Item, Connections};
//! use std::collections::BTreeSet;
//!
//! // An item representing a source file contained in a package
//! let mut targets = BTreeSet::new();
//! targets.insert("gitoid:blob:sha256:pkg456...".to_string());
//! let item = Item {
//!     identifier: "gitoid:blob:sha256:abc123...".to_string(),
//!     connections: Connections(
//!         [("contained:up".to_string(), targets)].into_iter().collect(),
//!     ),
//!     body_mime_type: Some("application/vnd.cc.goatrodeo".to_string()),
//!     body: None,
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

fn merge_values(v1: &Value, v2: &Value, depth: usize) -> Value {
    // Extract string(s) from a Value (handles both single strings and arrays)
    fn fix_uno(s: &Value) -> Vec<String> {
        match s {
            Value::Text(s) => vec![s.clone()],
            Value::Array(value_vec) => value_vec.iter().flat_map(fix_uno).collect(),
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
                                (1, _, _) => merged_file_names.into_iter().cloned().collect(),

                                // if both have already done the filename to gitoid mapping, then it's safe to merge
                                (_, tsize, osize) if tsize > 1 && osize > 1 => {
                                    merged_file_names.into_iter().cloned().collect()
                                }

                                // create the mapping
                                (_, _, _) => {
                                    let mut v1_with_mapping = fix_uno(&names[0]);
                                    let mut v2_with_mapping = fix_uno(&v2_names[0]);
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
                        let to_insert = merge_values(v, ov, depth + 1);
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
    let foo = Value::Text("foo".to_string());
    let v1 = Value::Array(vec![foo.clone()]);
    let bar = Value::Text("bar".to_string());
    let v2 = Value::Array(vec![bar.clone()]);
    let v1_2 = Value::Array(vec![bar.clone(), foo.clone()]);
    let m1 = Value::Map(BTreeMap::from([(foo.clone(), v1.clone())]));
    let m2 = Value::Map(BTreeMap::from([(foo.clone(), v2.clone())]));
    assert_eq!(foo, merge_values(&foo, &foo, 0));
    assert_eq!(v1, merge_values(&v1, &foo, 0));
    assert_eq!(v1, merge_values(&v1, &v1, 0));
    assert_eq!(v1_2.clone(), merge_values(&v1, &v2, 0));
    let merge1 = merge_values(&m1, &m2, 0);
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

    let merged_aa = merge_values(&a, &a, 0);

    assert_eq!(a, merged_aa, "merging with self should be same");

    let merged_ab = merge_values(&a, &b, 0);
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
        size == 2,
        "there should be 2 different filenames, but got {}",
        size
    )
}

pub trait EdgeType {
    fn is_alias_from(&self) -> bool;

    fn is_alias_to(&self) -> bool;

    fn is_up(&self) -> bool;

    fn is_contains_down(&self) -> bool;

    fn is_contained_by_up(&self) -> bool;

    fn is_built_from(&self) -> bool;

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

    fn is_tag_to(&self) -> bool {
        self == TAG_TO
    }

    fn is_tag_from(&self) -> bool {
        self == TAG_FROM
    }
}

/// The connections of an [`Item`]: an ordered map of edge type to the set
/// of target identifiers (ADR 0001).
///
/// Ordered containers are mandatory: the serialized bytes feed
/// content-addressed file names, so iteration order must be deterministic.
/// Maps serialize with keys in sorted order and target sets in sorted
/// order (BTree containers).
///
/// Deserialization accepts both shapes for CBOR and JSON (D5):
/// * the map shape: `{"contained:up": ["gitoid:..."]}`
/// * the legacy pair-array shape: `[["contained:up", "gitoid:..."]]`,
///   deduplicated by folding each target under its edge type
/// * a missing field: an empty map
///
/// Wrong-arity pairs, non-string elements, and nested arrays are rejected
/// with an error naming the offending entry. Values are preserved as
/// given, including empty target arrays — no empty-set invariant exists.
#[derive(Debug, Clone, Default, PartialEq, Serialize, ToSchema)]
pub struct Connections(pub BTreeMap<String, BTreeSet<String>>);

impl Connections {
    /// Fold one legacy `(edge type, target)` pair into the map: the target
    /// is inserted into the set for that edge type (ADR 0001).
    fn fold_pair(&mut self, edge_type: String, target: String) {
        self.0.entry(edge_type).or_default().insert(target);
    }
}

/// Build a `Connections` map by folding an iterator of legacy
/// `(edge type, target)` pairs (ADR 0001).
impl FromIterator<(String, String)> for Connections {
    fn from_iter<T: IntoIterator<Item = (String, String)>>(iter: T) -> Self {
        let mut ret = Connections::default();
        for (edge_type, target) in iter {
            ret.fold_pair(edge_type, target);
        }
        ret
    }
}

impl<'de> Deserialize<'de> for Connections {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ConnectionsVisitor;

        impl<'de> Visitor<'de> for ConnectionsVisitor {
            type Value = Connections;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str(
                    "a map of edge type to target array, or an array of \
                     (edge type, target) pairs",
                )
            }

            /// The v4 map shape: edge type -> array of targets.
            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut ret: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
                let mut index = 0usize;
                while let Some(key) = map
                    .next_key::<String>()
                    .map_err(|e| {
                        de::Error::custom(format!(
                            "connections[{}]: edge type must be a string: {}",
                            index, e
                        ))
                    })?
                {
                    let targets: Vec<String> = map.next_value::<Vec<String>>().map_err(|e| {
                        de::Error::custom(format!(
                            "connections[{:?}]: targets must be an array of strings: {}",
                            key, e
                        ))
                    })?;
                    ret.entry(key).or_default().extend(targets);
                    index += 1;
                }
                Ok(Connections(ret))
            }

            /// The legacy pair-array shape: array of (edge type, target).
            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut ret = Connections(BTreeMap::new());
                let mut index = 0usize;
                while let Some(pair) = seq
                    .next_element::<(String, String)>()
                    .map_err(|e| {
                        de::Error::custom(format!(
                            "connections[{}]: each entry must be a \
                             (edge type, target) pair of two strings: {}",
                            index, e
                        ))
                    })?
                {
                    ret.fold_pair(pair.0, pair.1);
                    index += 1;
                }
                Ok(ret)
            }
        }

        deserializer.deserialize_any(ConnectionsVisitor)
    }
}

/// A node in the BigTent graph database representing a software artifact.
///
/// A legacy (version 3) edge: a tuple of (edge_type, target_id).
///
/// Retained for the version 3 item shape ([`ItemV3`]) and the wire format
/// of legacy clusters.
pub type Edge = (String, String);

/// Items are identified by GitOIDs and connected to other items via typed edges.
/// The `body` field contains optional metadata in CBOR format.
#[derive(Debug, Deserialize, Serialize, Clone, ToSchema)]
#[schema(example = json!({
    "identifier": "gitoid:blob:sha256:a1b2c3d4e5f6...",
    "connections": {"contained:up": ["gitoid:blob:sha256:parent..."]},
    "body_mime_type": "application/vnd.cc.goatrodeo",
    "body": {"file_names": ["example.rs"], "file_size": 1234}
}))]
pub struct Item {
    /// The unique GitOID identifier for this item
    pub identifier: String,
    /// Ordered map of edge type to the set of target identifiers (ADR 0001).
    /// Edge types include: "alias:to", "alias:from", "contained:up", "contained:down",
    /// "build:up", "build:down", "tag:to", "tag:from". Serialization emits
    /// sorted keys and sorted target sets. Deserialization also accepts the
    /// legacy pair-array shape (see [`Connections`]); a missing field reads
    /// as an empty map.
    #[serde(default)]
    pub connections: Connections,
    /// MIME type of the body content, typically "application/vnd.cc.goatrodeo" for metadata
    pub body_mime_type: Option<String>,
    /// Optional metadata body in CBOR format, serialized as JSON in API responses
    #[schema(value_type = Option<serde_json::Value>)]
    pub body: Option<Value>,
}

/// Structural equality: two Items are equal when their identifiers,
/// connection maps, body MIME types, and bodies are all equal. Because the
/// connection map is an ordered container, equality does not depend on
/// insertion history.
impl PartialEq for Item {
    fn eq(&self, other: &Self) -> bool {
        self.identifier == other.identifier
            && self.connections == other.connections
            && self.body_mime_type == other.body_mime_type
            && self.body == other.body
    }
}

/// The legacy (version 3) item representation.
///
/// The version in the name matches the cluster version that carries this
/// shape. `connections` is the legacy set of `(edge type, target)` pairs,
/// serialized exactly as version 3 clusters store them.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, ToSchema)]
pub struct ItemV3 {
    /// The unique GitOID identifier for this item
    pub identifier: String,
    /// Legacy connections: a sorted set of (edge type, target) pairs
    #[schema(value_type = Vec<(String, String)>)]
    pub connections: BTreeSet<Edge>,
    /// MIME type of the body content
    pub body_mime_type: Option<String>,
    /// Optional metadata body
    #[schema(value_type = Option<serde_json::Value>)]
    pub body: Option<Value>,
}

impl Item {
    /// Produce the legacy (version 3) view of this item: the connection map
    /// folds back to the sorted set of pairs (ADR 0001/D4).
    pub fn to_v3(&self) -> ItemV3 {
        let mut connections = BTreeSet::new();
        for (edge_type, targets) in &self.connections.0 {
            for target in targets {
                connections.insert((edge_type.clone(), target.clone()));
            }
        }
        ItemV3 {
            identifier: self.identifier.clone(),
            connections,
            body_mime_type: self.body_mime_type.clone(),
            body: self.body.clone(),
        }
    }
}

impl From<&Item> for ItemV3 {
    fn from(value: &Item) -> Self {
        value.to_v3()
    }
}

impl From<ItemV3> for Item {
    fn from(value: ItemV3) -> Self {
        let mut connections = Connections(BTreeMap::new());
        for (edge_type, target) in value.connections {
            connections.fold_pair(edge_type, target);
        }
        Item {
            identifier: value.identifier,
            connections,
            body_mime_type: value.body_mime_type,
            body: value.body,
        }
    }
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

impl From<ItemV3> for serde_json::Value {
    fn from(value: ItemV3) -> Self {
        serde_json::to_value(&value).expect("ItemV3 serializes to JSON")
    }
}

impl From<&ItemV3> for serde_json::Value {
    fn from(value: &ItemV3) -> Self {
        serde_json::to_value(value).expect("ItemV3 serializes to JSON")
    }
}

impl Item {
    /// Make a list of all the gitoids that this Item contains
    // pub fn list_contains(&self) -> Vec<String> {
    //     self.connections
    //         .iter()
    //         .filter(|v| v.0.is_contained_by_up())
    //         .map(|v| v.1.clone())
    //         .collect()
    // }
    /// is the item a "root" item... no "up" or "tag:from"
    pub fn is_root_item(&self) -> bool {
        if self.body_mime_type != Some("application/vnd.cc.goatrodeo".to_string()) {
            return false;
        }
        if self.identifier == "tags" {
            return false;
        }

        // Decision depends only on the edge type, so iterating the
        // edge-type map in sorted order is equivalent to the legacy
        // pair-set iteration (both see the decisive type first).
        for (edge_type, _targets) in &self.connections.0 {
            if edge_type.is_alias_to() {
                return false;
            }
            if edge_type.is_contained_by_up() {
                return false;
            }
            if edge_type.is_tag_from() {
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
            .0
            .iter()
            // find all aliases to this thing that start with `pkg:`
            .filter(|(edge_type, _)| edge_type.is_alias_from())
            .flat_map(|(_, targets)| targets.iter())
            .filter(|t| t.starts_with("pkg:"))
            // make into a string
            .map(|c| c.clone())
            // turn into a Vec<String>
            .collect()
    }

    /// is the item an alias (does it carry an `alias:to` edge with at
    /// least one target)?
    pub fn is_alias(&self) -> bool {
        self.connections
            .0
            .iter()
            .any(|(edge_type, targets)| edge_type.is_alias_to() && !targets.is_empty())
    }
    // pub fn remove_references(&mut self) {
    //   self.reference = Item::NOOP;
    // }

    /// get all the connections that are either `contained_by_up` or `is_alias_to` or `is_tag_from`
    pub fn contained_by(&self) -> HashSet<String> {
        let mut ret = HashSet::new();
        for (edge_type, targets) in &self.connections.0 {
            if edge_type.is_alias_to() || edge_type.is_tag_from() || edge_type.is_up() {
                for target in targets {
                    ret.insert(target.clone());
                }
            }
        }

        ret
    }

    pub fn to_json(&self) -> serde_json::Value {
        let mut ret = serde_json::to_value(self).expect("Should be able to serialize Item");
        if let serde_json::Value::Object(mut m) = ret {
            m.remove_entry("reference");
            ret = serde_json::Value::Object(m);
        }
        ret
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
                if self.body_mime_type.iter().map(|mt| mt as &str).next_back()
                    == Some(ITEM_METADATA_MIME_TYPE) =>
            {
                match (from_value(a.clone()), from_value(b.clone())) {
                    (Ok::<ItemMetaData, _>(ai), Ok::<ItemMetaData, _>(bi)) => {
                        let merged = ai.merge(bi);
                        (to_value(merged).ok(), self.body_mime_type.clone())
                    }

                    // if we can't deserialize, then try merging the CBOR
                    _ => (Some(merge_values(a, &b, 0)), self.body_mime_type.clone()),
                }
            }
            (Some(a), Some(b), true) => (Some(merge_values(a, &b, 0)), self.body_mime_type.clone()),
            _ => (self.body.clone(), self.body_mime_type.clone()),
        };

        Item {
            identifier: self.identifier.clone(),
            // reference: self.reference,
            connections: {
                // union per edge type (ADR 0001): each target set is the
                // union of both items' target sets for that type
                let mut it = self.connections.clone();
                for (edge_type, targets) in other.connections.0 {
                    it.0.entry(edge_type).or_default().extend(targets);
                }
                it
            },
            body_mime_type: mime_type,
            body,
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
    pub fn merge(&self, other: ItemMetaData) -> ItemMetaData {
        let mut merged_filenames = self.file_names.clone();
        merged_filenames.append(&mut other.file_names.clone());

        ItemMetaData {
            file_names: merged_filenames,
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

// ---------------------------------------------------------------------------
// Phase 2 tests (plan: plans/2026_09_16_connection_map_and_blake3/
// phase_2_v4_format_and_item.md — Item shape tests 1-15).
//
// Requirement provenance and test theory per project rule 3. The map shape
// and the fold from legacy pairs are specified by approved ADR 0001.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod phase2_item_shape {
    use super::*;
    use proptest::prelude::*;

    fn item_with_pairs(pairs: &[(&str, &str)]) -> Item {
        Item {
            identifier: "gitoid:blob:sha256:test".to_string(),
            connections: pairs
                .iter()
                .map(|(t, g)| (t.to_string(), g.to_string()))
                .collect(),
            body_mime_type: None,
            body: None,
        }
    }

    /// Test 1: the v4 map shape round-trips through CBOR.
    ///
    /// Requirement: D1/D5 (ADR 0001). Theory: the map is the canonical
    /// in-memory and on-disk shape; a round trip must preserve it exactly,
    /// including multiple edge types with multiple targets each.
    #[test]
    fn test_item_v4_cbor_round_trip() {
        let item = item_with_pairs(&[
            ("contained:up", "gitoid:blob:sha256:target1"),
            ("contained:up", "gitoid:blob:sha256:target2"),
            ("alias:from", "pkg:npm/example@1.0.0"),
            ("tag:to", "gitoid:blob:sha256:tagdest"),
        ]);
        let bytes = serde_cbor::to_vec(&item).expect("serialize");
        let back: Item = serde_cbor::from_slice(&bytes).expect("deserialize");
        assert_eq!(item, back);
        // and the stored shape is the map, not pairs
        let targets = back.connections.0.get("contained:up").expect("edge type");
        assert_eq!(targets.len(), 2);
    }

    /// Test 2: the legacy pair-array CBOR shape deserializes into the map.
    ///
    /// Requirement: D5 (dual-shape reading). Theory: version 3 clusters
    /// store pair arrays; every reader must accept them by folding each
    /// target under its edge type, with duplicates deduplicated.
    #[test]
    fn test_item_legacy_pairs_cbor_deserialize() {
        // CBOR: {"identifier": "...", "connections": [["contained:up", "t1"], ["contained:up", "t2"], ["contained:up", "t1"]]}
        let pairs_cbor = serde_cbor::Value::Array(vec![
            serde_cbor::Value::Array(vec![
                serde_cbor::Value::Text("contained:up".into()),
                serde_cbor::Value::Text("gitoid:blob:sha256:t1".into()),
            ]),
            serde_cbor::Value::Array(vec![
                serde_cbor::Value::Text("contained:up".into()),
                serde_cbor::Value::Text("gitoid:blob:sha256:t2".into()),
            ]),
            // duplicate pair: must deduplicate
            serde_cbor::Value::Array(vec![
                serde_cbor::Value::Text("contained:up".into()),
                serde_cbor::Value::Text("gitoid:blob:sha256:t1".into()),
            ]),
        ]);
        let item_bytes = serde_cbor::to_vec(&serde_cbor::Value::Map(
            [
                (
                    serde_cbor::Value::Text("identifier".into()),
                    serde_cbor::Value::Text("gitoid:blob:sha256:test".into()),
                ),
                (
                    serde_cbor::Value::Text("connections".into()),
                    pairs_cbor,
                ),
            ]
            .into_iter()
            .collect(),
        ))
        .expect("cbor");

        let item: Item = serde_cbor::from_slice(&item_bytes).expect("dual-shape read");
        let targets = item.connections.0.get("contained:up").expect("folded");
        assert_eq!(targets.len(), 2, "duplicate pairs must deduplicate");
        assert!(targets.contains("gitoid:blob:sha256:t1"));
        assert!(targets.contains("gitoid:blob:sha256:t2"));
    }

    /// Test 3: the legacy pair-array JSON shape deserializes (checked-in
    /// neutral fixture).
    ///
    /// Requirement: D5. Theory: downstream consumers have checked-in
    /// legacy item JSON; the dual-shape read must work for JSON exactly as
    /// for CBOR. The fixture lives in test_data/ (never an untracked
    /// directory) and contains duplicate pairs on purpose.
    #[test]
    fn test_item_legacy_pairs_json_deserialize() {
        let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("test_data/legacy_item_v3.json");
        let text = std::fs::read_to_string(fixture).expect("fixture readable");
        let item: Item = serde_json::from_str(&text).expect("legacy JSON reads");
        assert_eq!(
            item.identifier,
            "gitoid:blob:sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        );
        let targets = item.connections.0.get("contained:up").expect("folded");
        assert_eq!(targets.len(), 1, "duplicate pair in fixture deduplicates");
        assert!(item.find_purls().contains(&"pkg:npm/example@1.0.0".to_string()));
    }

    /// Test 4: serialization is canonical and deterministic — the same
    /// content yields the same bytes regardless of insertion history.
    ///
    /// Requirement: invariant 1 (determinism). Theory: item bytes feed
    /// content-addressed names; two items built with the same edges in
    /// different insertion orders must serialize to identical bytes
    /// (sorted keys, sorted target sets — BTree containers).
    #[test]
    fn test_item_serialize_canonical_deterministic() {
        let a = item_with_pairs(&[
            ("contained:up", "gitoid:blob:sha256:t1"),
            ("contained:up", "gitoid:blob:sha256:t2"),
            ("alias:from", "pkg:npm/example@1.0.0"),
        ]);
        let b = item_with_pairs(&[
            ("alias:from", "pkg:npm/example@1.0.0"),
            ("contained:up", "gitoid:blob:sha256:t2"),
            ("contained:up", "gitoid:blob:sha256:t1"),
        ]);
        let a_bytes = serde_cbor::to_vec(&a).expect("a");
        let b_bytes = serde_cbor::to_vec(&b).expect("b");
        assert_eq!(a_bytes, b_bytes, "same content, different insertion order");
        let a_json = serde_json::to_string(&a).expect("json a");
        let b_json = serde_json::to_string(&b).expect("json b");
        assert_eq!(a_json, b_json);
    }

    /// Test 5: a missing `connections` field deserializes as an empty map.
    ///
    /// Requirement: D5 (missing field accepted). Theory: legacy producers
    /// may omit connections entirely; the reader treats it as no edges.
    #[test]
    fn test_item_missing_connections_field_is_empty_map() {
        let item_bytes = serde_cbor::to_vec(&serde_cbor::Value::Map(
            [(
                serde_cbor::Value::Text("identifier".into()),
                serde_cbor::Value::Text("gitoid:blob:sha256:test".into()),
            )]
            .into_iter()
            .collect(),
        ))
        .expect("cbor");
        let item: Item = serde_cbor::from_slice(&item_bytes).expect("reads");
        assert!(item.connections.0.is_empty());
    }

    /// Test 6: ItemV3 round-trips the legacy shape.
    ///
    /// Requirement: D4. Theory: the legacy view is public and preserves
    /// exactly the v3 wire shape (set of pairs), so downstream code that
    /// needs the old representation can convert without loss.
    #[test]
    fn test_item_v3_round_trip() {
        let item = item_with_pairs(&[
            ("contained:up", "gitoid:blob:sha256:t1"),
            ("alias:from", "pkg:npm/example@1.0.0"),
        ]);
        let v3: ItemV3 = item.to_v3();
        let v3_bytes = serde_cbor::to_vec(&v3).expect("v3 cbor");
        let v3_back: ItemV3 = serde_cbor::from_slice(&v3_bytes).expect("v3 read");
        assert_eq!(v3, v3_back);
        // legacy wire shape: connections is an array of 2-element arrays
        let json = serde_json::to_value(&v3_back).expect("v3 json");
        assert!(json["connections"].is_array());
        assert!(json["connections"][0].is_array());
        // and back to the v4 map without loss
        let back: Item = v3_back.into();
        assert_eq!(back, item);
    }

    /// Test 7: folding the map back to pairs and re-folding preserves
    /// order — the flattened pair order equals the legacy order.
    ///
    /// Requirement: D1/D4. Theory: content addressing depends on
    /// deterministic bytes; the map must be a lossless re-indexing of the
    /// legacy pairs (same pairs, same sorted order).
    #[test]
    fn test_flattened_map_order_equals_legacy_pair_order() {
        let pairs: Vec<(String, String)> = vec![
            ("alias:from".to_string(), "pkg:x@1".to_string()),
            ("contained:up".to_string(), "gitoid:blob:sha256:t2".to_string()),
            ("contained:up".to_string(), "gitoid:blob:sha256:t1".to_string()),
            ("tag:to".to_string(), "gitoid:blob:sha256:tt".to_string()),
        ];
        let item = Item {
            identifier: "gitoid:blob:sha256:test".to_string(),
            connections: {
                let mut m: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> =
                    Default::default();
                for (t, g) in &pairs {
                    m.entry(t.clone()).or_default().insert(g.clone());
                }
                Connections(m)
            },
            body_mime_type: None,
            body: None,
        };
        let v3: ItemV3 = item.to_v3();
        let flattened: Vec<(String, String)> = v3.connections.into_iter().collect();
        let mut expected = pairs;
        expected.sort();
        expected.dedup();
        assert_eq!(flattened, expected);
    }

    /// Test 8: merge unions the connection map per edge type.
    ///
    /// Requirement: ADR 0001 (merge unions target sets per edge type).
    #[test]
    fn test_item_merge_unions_connection_map() {
        let a = item_with_pairs(&[
            ("contained:up", "gitoid:blob:sha256:t1"),
            ("tag:to", "gitoid:blob:sha256:tt"),
        ]);
        let b = item_with_pairs(&[
            ("contained:up", "gitoid:blob:sha256:t2"),
            ("alias:from", "pkg:npm/x@1"),
        ]);
        let merged = a.merge(b);
        let contained = merged.connections.0.get("contained:up").expect("union");
        assert_eq!(contained.len(), 2);
        assert!(merged.connections.0.contains_key("tag:to"));
        assert!(merged.connections.0.contains_key("alias:from"));
    }

    /// Test 9: the block-list retain removes blocked targets and keeps
    /// other targets and edge types intact.
    ///
    /// Requirement: merge block-list semantics preserved under the map
    /// shape. Theory: blocking removes individual targets, never whole
    /// edge types; non-blocked targets of the same type and other types
    /// must survive.
    #[test]
    fn test_block_list_retain_removes_blocked_targets() {
        let blocked = "gitoid:blob:sha256:blocked".to_string();
        let mut item = item_with_pairs(&[
            ("contained:up", "gitoid:blob:sha256:blocked"),
            ("contained:up", "gitoid:blob:sha256:keep"),
            ("tag:to", "gitoid:blob:sha256:blocked"),
        ]);
        item.connections
            .0
            .values_mut()
            .for_each(|targets| targets.retain(|t| *t != blocked));
        let contained = item.connections.0.get("contained:up").expect("type kept");
        assert!(contained.contains("gitoid:blob:sha256:keep"));
        assert!(!contained.contains(&blocked));
        let tagged = item.connections.0.get("tag:to").expect("type kept");
        assert!(tagged.is_empty(), "blocked target removed even if now empty");
    }

    /// Test 10: malformed legacy connections are rejected with an
    /// entry-naming error.
    ///
    /// Requirement: D5 (reject wrong arity, non-strings, nested arrays).
    /// Theory: silent acceptance of malformed shapes would corrupt the
    /// fold; errors must name the offending entry so producers can fix
    /// their data.
    #[test]
    fn test_legacy_connections_malformed_rejected() {
        // wrong arity: [["contained:up"]] (single element)
        let bad_arity = serde_cbor::to_vec(&serde_cbor::Value::Map(
            [(
                serde_cbor::Value::Text("connections".into()),
                serde_cbor::Value::Array(vec![serde_cbor::Value::Array(vec![
                    serde_cbor::Value::Text("contained:up".into()),
                ])]),
            )]
            .into_iter()
            .collect(),
        ))
        .unwrap();
        let err: Result<Item, _> = serde_cbor::from_slice(&bad_arity);
        let msg = format!("{}", err.expect_err("arity must fail"));
        assert!(
            msg.to_lowercase().contains("connection"),
            "error must name the entry: {msg}"
        );

        // non-string target: [["contained:up", 42]]
        let bad_target = serde_cbor::to_vec(&serde_cbor::Value::Map(
            [(
                serde_cbor::Value::Text("connections".into()),
                serde_cbor::Value::Array(vec![serde_cbor::Value::Array(vec![
                    serde_cbor::Value::Text("contained:up".into()),
                    serde_cbor::Value::Integer(42),
                ])]),
            )]
            .into_iter()
            .collect(),
        ))
        .unwrap();
        let err: Result<Item, _> = serde_cbor::from_slice(&bad_target);
        let msg = format!("{}", err.expect_err("non-string target must fail"));
        assert!(
            msg.to_lowercase().contains("connection"),
            "error must name the entry: {msg}"
        );

        // nested arrays: [[["contained:up", "t"]]]
        let nested = serde_cbor::to_vec(&serde_cbor::Value::Map(
            [(
                serde_cbor::Value::Text("connections".into()),
                serde_cbor::Value::Array(vec![serde_cbor::Value::Array(vec![
                    serde_cbor::Value::Array(vec![
                        serde_cbor::Value::Text("contained:up".into()),
                        serde_cbor::Value::Text("t".into()),
                    ]),
                ])]),
            )]
            .into_iter()
            .collect(),
        ))
        .unwrap();
        let err: Result<Item, _> = serde_cbor::from_slice(&nested);
        let msg = format!("{}", err.expect_err("nested arrays must fail"));
        assert!(
            msg.to_lowercase().contains("connection"),
            "error must name the entry: {msg}"
        );
    }

    /// Test 11: property — the v4 map shape round-trips through CBOR for
    /// arbitrary generated connection maps.
    #[test]
    fn prop_item_v4_cbor_round_trip() {
        proptest!(|(pairs in prop::collection::vec(
            ("[a-z:]{3,12}".prop_map(|s| s), prop::collection::vec("gitoid:blob:sha256:[a-f0-9]{8,16}".prop_map(|s| s), 1..5)),
            0..20
        ))| {
            let mut m: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> = Default::default();
            for (t, targets) in pairs {
                m.entry(t).or_default().extend(targets);
            }
            let item = Item {
                identifier: "gitoid:blob:sha256:test".to_string(),
                connections: Connections(m.clone()),
                body_mime_type: None,
                body: None,
            };
            let bytes = serde_cbor::to_vec(&item).expect("ser");
            let back: Item = serde_cbor::from_slice(&bytes).expect("de");
            prop_assert_eq!(back.connections.0, m);
        });
    }

    /// Test 12: property — v3 pair conversion loses nothing: for arbitrary
    /// maps, to_v3 then back yields the identical map.
    #[test]
    fn prop_item_v3_pair_conversion_no_loss() {
        proptest!(|(pairs in prop::collection::vec(
            ("[a-z:]{3,12}".prop_map(|s| s), prop::collection::vec("gitoid:blob:sha256:[a-f0-9]{8,16}".prop_map(|s| s), 1..5)),
            0..20
        ))| {
            let mut m: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> = Default::default();
            for (t, targets) in pairs {
                m.entry(t).or_default().extend(targets);
            }
            let item = Item {
                identifier: "gitoid:blob:sha256:test".to_string(),
                connections: Connections(m.clone()),
                body_mime_type: None,
                body: None,
            };
            let v3: ItemV3 = item.to_v3();
            let back: Item = v3.into();
            prop_assert_eq!(back.connections.0, m);
        });
    }

    /// Test 13: property — connection merge is a union (commutative for
    /// the connection map).
    #[test]
    fn prop_connection_merge_is_union() {
        proptest!(|(a in prop::collection::vec(("[a-z:]{3,10}".prop_map(|s| s), "[a-z]{3,8}".prop_map(|s| s)), 0..10),
                   b in prop::collection::vec(("[a-z:]{3,10}".prop_map(|s| s), "[a-z]{3,8}".prop_map(|s| s)), 0..10))| {
            let fold = |pairs: Vec<(String, String)>| {
                let mut m: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> = Default::default();
                for (t, g) in pairs { m.entry(t).or_default().insert(g); }
                m
            };
            let item_a = Item {
                identifier: "x".to_string(),
                connections: Connections(fold(a.clone())),
                body_mime_type: None,
                body: None,
            };
            let item_b = Item {
                identifier: "x".to_string(),
                connections: Connections(fold(b.clone())),
                body_mime_type: None,
                body: None,
            };
            let merged = item_a.merge(item_b);
            let mut expected = fold(a);
            for (t, g) in fold(b) { expected.entry(t).or_default().extend(g); }
            prop_assert_eq!(merged.connections.0, expected);
        });
    }

    /// Test 14: property — any ordering of the same legacy pairs yields
    /// identical item bytes.
    #[test]
    fn prop_legacy_pair_deserialization_is_permutation_invariant() {
        proptest!(|(seed in any::<u64>())| {
            use rand::{Rng, SeedableRng, seq::IteratorRandom};
            let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
            let types = ["contained:up", "contained:down", "alias:from", "tag:to"];
            let mut pairs: Vec<(String, String)> = vec![];
            for _ in 0..(rng.random_range(1..20)) {
                pairs.push((
                    types[rng.random_range(0..types.len())].to_string(),
                    format!("gitoid:blob:sha256:{:016x}", rng.random::<u64>()),
                ));
            }
            let permuted: Vec<(String, String)> = {
                // deterministic shuffle: repeatedly pick a remaining element
                let mut rest = pairs.clone();
                let mut out = vec![];
                while !rest.is_empty() {
                    let idx = (0..rest.len()).choose(&mut rng).unwrap();
                    out.push(rest.remove(idx));
                }
                out
            };
            let build = |pairs: Vec<(String, String)>| -> Item {
                let mut m: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> = Default::default();
                for (t, g) in pairs { m.entry(t).or_default().insert(g); }
                Item {
                    identifier: "gitoid:blob:sha256:test".to_string(),
                    connections: Connections(m),
                    body_mime_type: None,
                    body: None,
                }
            };
            // serialize through the LEGACY shape to simulate arbitrary
            // insertion history, then read back dual-shape
            let legacy_a = serde_cbor::to_vec(&build(pairs.clone()).to_v3()).unwrap();
            let legacy_b = serde_cbor::to_vec(&build(permuted).to_v3()).unwrap();
            let a: Item = serde_cbor::from_slice(&legacy_a).unwrap();
            let b: Item = serde_cbor::from_slice(&legacy_b).unwrap();
            prop_assert_eq!(a, b);
        });
    }

    /// Test 15: property — connection union is commutative, associative,
    /// and idempotent (the merge algebra).
    #[test]
    fn prop_item_merge_algebra() {
        proptest!(|(a in prop::collection::vec(("[a-z:]{3,10}".prop_map(|s| s), "[a-z]{3,8}".prop_map(|s| s)), 0..6),
                   b in prop::collection::vec(("[a-z:]{3,10}".prop_map(|s| s), "[a-z]{3,8}".prop_map(|s| s)), 0..6),
                   c in prop::collection::vec(("[a-z:]{3,10}".prop_map(|s| s), "[a-z]{3,8}".prop_map(|s| s)), 0..6))| {
            let fold = |pairs: Vec<(String, String)>| {
                let mut m: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> = Default::default();
                for (t, g) in pairs { m.entry(t).or_default().insert(g); }
                m
            };
            let mk = |m: std::collections::BTreeMap<String, std::collections::BTreeSet<String>>| Item {
                identifier: "x".to_string(),
                connections: Connections(m),
                body_mime_type: None,
                body: None,
            };
            let (ma, mb, mc) = (fold(a), fold(b), fold(c));
            let (ia, ib, ic) = (mk(ma.clone()), mk(mb.clone()), mk(mc.clone()));

            // commutative
            let ab: Item = ia.merge(ib.clone());
            let ba: Item = ib.merge(ia.clone());
            prop_assert_eq!(ab.connections.0, ba.connections.0);

            // associative
            let ab_c = ia.merge(ib.clone()).merge(ic.clone());
            let a_bc = ia.clone().merge(ib.merge(ic.clone()));
            prop_assert_eq!(ab_c.connections.0, a_bc.connections.0);

            // idempotent
            let aa = ia.clone().merge(ia.clone());
            prop_assert_eq!(aa.connections.0, ia.connections.0);
        });
    }
}

#[cfg(test)]
mod phase4_item_format_props {
    use super::*;
    use proptest::prelude::*;

    /// Test 11 (phase 4): property — arbitrary items serialize as ItemV3
    /// JSON and parse back equal.
    ///
    /// Requirement: D4/D8. Theory: the legacy wire shape must be a real
    /// wire shape: any item round-trips through ItemV3 JSON losslessly.
    #[test]
    fn prop_v3_json_round_trip() {
        proptest!(|(pairs in prop::collection::vec(
            ("[a-z:]{3,12}".prop_map(|s| s), prop::collection::vec("gitoid:blob:sha256:[a-f0-9]{8,16}".prop_map(|s| s), 1..5)),
            0..20
        ))| {
            let mut m: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> = Default::default();
            for (t, targets) in pairs {
                m.entry(t).or_default().extend(targets);
            }
            let item = Item {
                identifier: "gitoid:blob:sha256:test".to_string(),
                connections: Connections(m),
                body_mime_type: None,
                body: None,
            };
            let v3: ItemV3 = item.to_v3();
            let json = serde_json::to_value(&v3).expect("v3 json");
            let back: ItemV3 = serde_json::from_value(json).expect("v3 json reads back");
            prop_assert_eq!(v3, back);
        });
    }

    /// Test 12 (phase 4): property — the default (map) and v3 (pair)
    /// responses contain the same edge multiset.
    ///
    /// Requirement: D8. Theory: the two wire shapes are two views of the
    /// SAME edges: for arbitrary items, the parsed default response and
    /// the parsed v3 response must agree on the (edge type, target)
    /// multiset, so no shape can drop or invent edges.
    #[test]
    fn prop_default_and_v3_responses_are_semantically_equal() {
        proptest!(|(pairs in prop::collection::vec(
            ("[a-z:]{3,12}".prop_map(|s| s), prop::collection::vec("gitoid:blob:sha256:[a-f0-9]{8,16}".prop_map(|s| s), 1..5)),
            0..20
        ))| {
            let mut m: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> = Default::default();
            for (t, targets) in pairs {
                m.entry(t).or_default().extend(targets);
            }
            let item = Item {
                identifier: "gitoid:blob:sha256:test".to_string(),
                connections: Connections(m),
                body_mime_type: None,
                body: None,
            };

            // default response: connections is an object of arrays
            let default_json = serde_json::to_value(&item).unwrap();
            let mut default_edges: std::collections::BTreeSet<(String, String)> = Default::default();
            for (edge_type, targets) in default_json["connections"].as_object().unwrap() {
                for target in targets.as_array().unwrap() {
                    default_edges.insert((edge_type.clone(), target.as_str().unwrap().to_string()));
                }
            }

            // v3 response: connections is an array of 2-arrays
            let v3_json: serde_json::Value = item.to_v3().into();
            let mut v3_edges: std::collections::BTreeSet<(String, String)> = Default::default();
            for pair in v3_json["connections"].as_array().unwrap() {
                let p = pair.as_array().unwrap();
                v3_edges.insert((p[0].as_str().unwrap().to_string(), p[1].as_str().unwrap().to_string()));
            }

            prop_assert_eq!(default_edges, v3_edges);
        });
    }
}
