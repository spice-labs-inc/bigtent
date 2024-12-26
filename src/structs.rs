use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_cbor::Value;

#[derive(Debug, Eq, PartialEq, PartialOrd, Deserialize, Serialize, Clone, Copy, Hash, Ord)]
pub enum EdgeType {
  AliasTo,
  AliasFrom,
  Contains,
  ContainedBy,
  BuildsTo,
  BuiltFrom,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct ItemMetaData {
  pub file_names: BTreeSet<String>,
  pub file_type: BTreeSet<String>,
  pub file_sub_type: BTreeSet<String>,
  pub file_size: Option<i64>,
  pub extra: BTreeMap<String, BTreeSet<String>>,
}

impl<'de2> MetaData<'de2> for ItemMetaData {}

impl<'de2> MetaData<'de2> for Value {}

impl Mergeable for ItemMetaData {
  fn merge(&self, other: ItemMetaData) -> ItemMetaData {
    ItemMetaData {
      file_names: {
        let mut it = self.file_names.clone();
        it.extend(other.file_names);
        it
      },
      file_type: {
        let mut it = self.file_type.clone();
        it.extend(other.file_type.clone());
        it
      },
      file_sub_type: {
        let mut it = self.file_sub_type.clone();
        it.extend(other.file_sub_type.clone());
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

pub type Edge = (EdgeType, String);

/// A trait that defines the metadata field in an Item
pub trait MetaData<'a>:
  Deserialize<'a> + Serialize + PartialEq + Clone + Mergeable + Sized + Send + Sync
{
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Item<MDT>
where
  for<'de2> MDT: MetaData<'de2>,
{
  pub identifier: String,
  pub reference: LocationReference,
  pub connections: BTreeSet<Edge>,
  #[serde(bound(deserialize = "Option<MDT>: Deserialize<'de>"))]
  pub metadata: Option<MDT>,
  pub merged_from: BTreeSet<LocationReference>,
}

impl<MDT> Item<MDT>
where
  for<'de2> MDT: MetaData<'de2>,
{
  pub const NOOP: LocationReference = (0u64, 0u64);

  /// Find all the aliases that are package URLs
  pub fn find_purls(&self) -> Vec<String> {
    self
      // from connections
      .connections
      .iter()
      // find all aliases to this thing that start with `pkg:`
      .filter(|c| c.0 == EdgeType::AliasFrom && c.1.starts_with("pkg:"))
      // make into a string
      .map(|c| c.1.clone())
      // turn into a Vec<String>
      .collect()
  }

  pub fn is_alias(&self) -> bool {
    self.connections.iter().any(|v| v.0 == EdgeType::AliasTo)
  }
  pub fn remove_references(&mut self) {
    self.reference = Item::<MDT>::NOOP;
    self.merged_from = BTreeSet::new();
  }

  // tests two items... they are the same if they have the same
  // `identifier`, `connections`, `metadata`, `version`,
  // and `_type`
  pub fn is_same(&self, other: &Item<MDT>) -> bool {
    self.identifier == other.identifier
      && self.connections == other.connections
      && self.metadata == other.metadata
  }

  // merge to `Item`s
  pub fn merge(&self, other: Item<MDT>) -> Item<MDT> {
    Item {
      identifier: self.identifier.clone(),
      reference: self.reference,
      connections: {
        let mut it = self.connections.clone();
        it.extend(other.connections);
        it
      },
      metadata: match (&self.metadata, other.metadata) {
        (None, None) => None,
        (None, Some(a)) => Some(a),
        (Some(a), None) => Some(a.clone()),
        (Some(a), Some(b)) => Some(a.merge(b)),
      },
      merged_from: self.merged_from.clone(),
    }
  }

  // merges this item and returns a new item with `merged_from` set
  // correctly if there were changes
  pub fn merge_vecs(items: Vec<Item<MDT>>) -> ItemMergeResult<MDT> {
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

pub enum ItemMergeResult<MDT>
where
  for<'de> MDT: MetaData<'de>,
{
  Same,
  ContainsAll(LocationReference),
  New(Item<MDT>),
}

/// The Item<ItemMetaData> type that replaces the old baked in Item
pub type MDItem = Item<ItemMetaData>;
