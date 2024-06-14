use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::util::millis_now;

#[derive(Debug, Eq, PartialEq, PartialOrd, Deserialize, Serialize, Clone, Copy, Hash)]
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
    pub file_names: HashSet<String>,
    pub file_type: HashSet<String>,
    pub file_sub_type: HashSet<String>,
    pub extra: HashMap<String, HashSet<String>>,
}

impl ItemMetaData {
    pub fn merge(&self, other: ItemMetaData) -> ItemMetaData {
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
pub type Edge = (String, EdgeType, Option<String>);

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Item {
    pub identifier: String,
    pub reference: LocationReference,
    pub connections: HashSet<Edge>,
    pub metadata: Option<ItemMetaData>,
    pub merged_from: Vec<LocationReference>,
    pub file_size: i64,
    pub _timestamp: i64,
    pub _version: u32,
    pub _type: String,
}

impl Item {
    pub const NOOP: LocationReference = (0u64, 0u64);

    pub fn is_alias(&self) -> bool {
      self.connections.iter().any(|v| v.1 == EdgeType::AliasTo)
    }
    pub fn remove_references(&mut self) {
        self.reference = Item::NOOP;
        self.merged_from = vec![];
    }

    // tests two items... they are the same if they have the same
    // `identifier`, `connections`, `metadata`, `file_size`, `version`,
    // and `_type`
    pub fn is_same(&self, other: &Item) -> bool {
        self.file_size == other.file_size
            && self._version == other._version
            && self.identifier == other.identifier
            && self.connections == other.connections
            && self.metadata == other.metadata
            && self._type == other._type
    }

    // merge to `Item`s
    pub fn merge(&self, other: Item) -> Item {
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
            file_size: self.file_size,
            _timestamp: millis_now(),
            _version: self._version,
            _type: self._type.clone(),
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
      let mut mf = vec![base.reference];
      for i in 1..len {
        mf.push(items[i].reference);
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

pub enum ItemMergeResult {
  Same,
  ContainsAll(LocationReference),
  New(Item)
}