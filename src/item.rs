use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde::{Deserialize, Serialize};
use serde_cbor::Value;

/// Merge two values
fn merge_values<F: Fn() -> Vec<String>, F2: Fn() -> Vec<String>>(
  v1: &Value,
  v2: &Value,
  depth: usize,
  v1_contains: &F,
  v2_contains: &F2,
) -> Value {
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
                (1, _, _) => merged_file_names.into_iter().map(|v| v.clone()).collect(),

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
  // pub fn remove_references(&mut self) {
  //   self.reference = Item::NOOP;
  // }

  /// get all the connections that are either `contained_by_up` or `is_to_right`
  pub fn contained_by(&self) -> HashSet<String> {
    let mut ret = HashSet::new();
    for edge in self.connections.iter() {
      if edge.0.is_to() || edge.0.is_up() {
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
      other.body,
      self.body_mime_type == other.body_mime_type,
    ) {
      (None, None, _) => (None, None),
      (None, Some(a), _) => (Some(a), other.body_mime_type.clone()),
      (Some(a), None, _) => (Some(a.clone()), self.body_mime_type.clone()),
      (Some(a), Some(b), true) => (
        Some(merge_values(
          a,
          &b,
          0,
          &|| {
            self
              .connections
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
