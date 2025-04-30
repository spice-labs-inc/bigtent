use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde::{Deserialize, Serialize};
use serde_cbor::Value;
pub trait Mergeable {
  fn merge(&self, other: Self) -> Self;
}

/// Merge two values
fn merge_values(v1: &Value, v2: Option<&Value>) -> Value {
  match v2 {
    None => v1.clone(),
    Some(v2) => match (v1, v2) {
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
        let mut remaining = map2.clone();
        let mut ret = BTreeMap::new();
        for (k, v) in map1 {
          match remaining.get(k) {
            Some(ov) => {
              let to_insert = merge_values(v, Some(ov));
              remaining.remove(k);
              ret.insert(k.clone(), to_insert);
            }
            None => {
              ret.insert(k.clone(), v.clone());
            }
          }
        }
        Value::Map(ret)
      }
      (Value::Null, _) => v2.clone(),
      // Anything else just becomes a clone of the left side of the merge
      _ => v1.clone(),
    },
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
  assert_eq!(foo, merge_values(&foo, None));
  assert_eq!(foo, merge_values(&foo, Some(&foo)));
  assert_eq!(v1, merge_values(&v1, Some(&foo)));
  assert_eq!(v1, merge_values(&v1, Some(&v1)));
  assert_eq!(v1_2.clone(), merge_values(&v1, Some(&v2)));
  let merge1 = merge_values(&m1, Some(&m2));
  let merged_key = match merge1 {
    Value::Map(map) => {
      let m2 = map.clone();
      m2.get(&foo).map(|v| v.clone())
    }
    _ => None,
  };

  assert_eq!(Some(v1_2.clone()), merged_key);
}

impl Mergeable for Value {
  fn merge(&self, other: Self) -> Self {
    merge_values(self, Some(&other))
  }
}

pub trait EdgeType {
  fn is_alias_from(&self) -> bool;

  fn is_alias_to(&self) -> bool;

  fn is_from(&self) -> bool;

  fn is_to(&self) -> bool;

  fn is_contains_down(&self) -> bool;

  fn is_contained_by_up(&self) -> bool;

  fn is_built_from(&self) -> bool;

  fn is_builds_to(&self) -> bool;
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
}

pub type Edge = (String, String);

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Item {
  pub identifier: String,
  pub connections: BTreeSet<Edge>,
  pub body_mime_type: Option<String>,
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

  /// get all the connections that are either `contained_by_up` or `is_to` or `is_builds_to`
  pub fn contained_by(&self) -> HashSet<String> {
    let mut ret = HashSet::new();
    for edge in self.connections.iter() {
      if edge.0.is_contained_by_up() || edge.0.is_to() || edge.0.is_builds_to() {
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

  // merge to `Item`s
  pub fn merge(&self, other: Item) -> Item {
    let (body, mime_type) = match (
      &self.body,
      other.body,
      self.body_mime_type == other.body_mime_type,
    ) {
      (None, None, _) => (None, None),
      (None, Some(a), _) => (Some(a), other.body_mime_type.clone()),
      (Some(a), None, _) => (Some(a.clone()), self.body_mime_type.clone()),
      (Some(a), Some(b), true) => (Some(a.merge(b)), self.body_mime_type.clone()),
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
