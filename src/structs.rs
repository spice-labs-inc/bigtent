use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

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
    pub extra: BTreeMap<String, BTreeSet<String>>,
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


pub type Edge = (EdgeType,String);

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Item {
    pub identifier: String,
    pub reference: LocationReference,
    pub connections: BTreeSet<Edge>,
    pub metadata: Option<ItemMetaData>,
    pub merged_from: BTreeSet<LocationReference>,
    pub file_size: i64,
}

impl Item {
    pub const NOOP: LocationReference = (0u64, 0u64);

    pub fn is_alias(&self) -> bool {
      self.connections.iter().any(|v| v.0 == EdgeType::AliasTo)
    }
    pub fn remove_references(&mut self) {
        self.reference = Item::NOOP;
        self.merged_from = BTreeSet::new();
    }

    /// Returns all the pURL (package URLs) in this item
    pub fn find_purls(&self) -> Vec<String> {
        match &self.metadata {
            Some(md) => {
                let mut ret = vec![];
                for name in &md.file_names{
                    if name.starts_with("pkg:") {
                        ret.push(name.clone());
                    }
                }
                ret
            }
            None => vec![]
        }
    }

    // tests two items... they are the same if they have the same
    // `identifier`, `connections`, `metadata`, `file_size`, `version`,
    // and `_type`
    pub fn is_same(&self, other: &Item) -> bool {
        self.file_size == other.file_size
            && self.identifier == other.identifier
            && self.connections == other.connections
            && self.metadata == other.metadata
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

pub enum ItemMergeResult {
  Same,
  ContainsAll(LocationReference),
  New(Item)
}