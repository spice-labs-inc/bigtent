use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_cbor::Value;


pub type LocationReference = (u64, u64);

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

  fn is_from_left(&self) -> bool;

  fn is_to_right(&self) -> bool;

  fn is_contains_down(&self) -> bool;

 fn is_contained_by_up(&self) -> bool;
}

pub const TO_RIGHT: &str = ":->";
pub const FROM_LEFT: &str = ":<-";
pub const CONTAINS_DOWN: &str = ":\\/";
pub const CONTAINED_BY_UP: &str = ":^";

pub const CONTAINED_BY: &str = "ContainedBy:^";
pub const CONTAINS: &str = "Contains:\\/";
pub const ALIAS_TO: &str = "AliasTo:->";
pub const ALIAS_FROM: &str = "AliasFrom:<-";


impl EdgeType for String {
  fn is_alias_from(&self) -> bool {
    self == ALIAS_FROM
  }

   fn is_alias_to(&self) -> bool {
    self == ALIAS_TO
  }

  fn is_from_left(&self) -> bool {
    self.ends_with(FROM_LEFT)
  }

  fn is_to_right(&self) -> bool {
    self.ends_with(TO_RIGHT)
  }

  fn is_contains_down(&self) -> bool {
    self.ends_with(CONTAINS_DOWN)
  }

  fn is_contained_by_up(&self) -> bool {
    self.ends_with(CONTAINED_BY_UP)
  }

}

pub type Edge = (String, String);


#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Item
{
  pub identifier: String,
  pub reference: LocationReference,
  pub connections: BTreeSet<Edge>,
  pub body_mime_type: Option<String>,
  pub body: Option<Value>,
  pub merged_from: BTreeSet<LocationReference>,
}

impl Item
{
  pub const NOOP: LocationReference = (0u64, 0u64);

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
  pub fn remove_references(&mut self) {
    self.reference = Item::NOOP;
    self.merged_from = BTreeSet::new();
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
    let (body, mime_type) = match (&self.body, other.body, self.body_mime_type == other.body_mime_type) {
      (None, None, _) => (None, None),
      (None, Some(a), _) => (Some(a), other.body_mime_type.clone()),
      (Some(a), None, _) => (Some(a.clone()), self.body_mime_type.clone()),
      (Some(a), Some(b), true) => (Some(a.merge(b)), self.body_mime_type.clone()),
      _ => (self.body.clone(), self.body_mime_type.clone())
    };

    Item {
      identifier: self.identifier.clone(),
      reference: self.reference,
      connections: {
        let mut it = self.connections.clone();
        it.extend(other.connections);
        it
      },
      body_mime_type: mime_type,
      body: body,
      merged_from: self.merged_from.clone(),
    }
  }

  // merges this item and returns a new item with `merged_from` set
  // correctly if there were changes
  pub fn merge_vecs(items: Vec<Item>) -> ItemMergeResult {
    let len = items.len();
    if len <= 1 {
      return ItemMergeResult::Same;
    }

    let mut base = items[0].clone();
    let mut mf = BTreeSet::new();

    mf.insert(base.reference);
    for i in 1..len {
      mf.insert(items[i].reference);
      base = items[i].merge(base);
    }

    for i in 0..len {
      if base.is_same(&items[i]) {
        return ItemMergeResult::ContainsAll(items[i].reference);
      }
    }

    base.merged_from = mf;
    return ItemMergeResult::New(base);
  }
}

pub enum ItemMergeResult
{
  Same,
  ContainsAll(LocationReference),
  New(Item),
}
