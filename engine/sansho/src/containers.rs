//! The container source family — the "likely suspects" turtles: the
//! Sansho data types implemented on the concrete container Rust types
//! (standard-library types — ALL of these implementations live in
//! Sansho, per the placement rule).
//!
//! Value forms (`Option<T>`, `Vec<T>`, slices, sets, maps): the
//! natural call shape — `lookup(&container, …)`; members are OWNED
//! computed values (the member's own `materialize` — a value form has
//! no borrowable storage).
//!
//! Reference forms for the concrete container types the borrowed
//! navigations need (`&'a Option<T>` delegating to the inner,
//! `&'a BTreeSet<String>` yielding borrowed `Cow` strings,
//! `&'a BTreeMap<String, BTreeSet<String>>` yielding borrowed sets):
//! members are BORROWED values implementing the trait (a generic
//! `&'a T` member cannot implement the trait without a blanket impl,
//! so the borrowed forms are per-concrete-type).
//!
//! Determinism: the JSON view of a set or hash map is ordered — a hash
//! set sorts its elements, a hash map sorts by key string (their
//! iteration order is not deterministic, and the JSON view must be).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::error::SanshoError;
use crate::view::{Kind, SanshoNumber, SanshoTrait};
use serde_json::Value as J;

// ---------------------------------------------------------------------------
// Option
// ---------------------------------------------------------------------------

/// The value form: "null or the inner value, on the inner value's own
/// surface" — every probe and every navigation delegates to the inner
/// value's own implementation.
impl<'a, T: SanshoTrait<'a>> SanshoTrait<'a> for Option<T> {
    fn kind(&self) -> Kind {
        match self {
            Some(inner) => inner.kind(),
            None => Kind::Null,
        }
    }

    fn as_str(&self) -> Option<std::borrow::Cow<'_, str>> {
        match self {
            Some(inner) => inner.as_str(),
            None => None,
        }
    }

    fn as_str_ref(&self) -> Option<&'a str> {
        match self {
            Some(inner) => inner.as_str_ref(),
            None => None,
        }
    }

    fn as_sansho_number(&self) -> Option<SanshoNumber> {
        match self {
            Some(inner) => inner.as_sansho_number(),
            None => None,
        }
    }

    fn as_bool(&self) -> Option<bool> {
        match self {
            Some(inner) => inner.as_bool(),
            None => None,
        }
    }

    fn is_null(&self) -> bool {
        self.is_none()
    }

    fn get_key(&self, name: &str) -> Option<impl SanshoTrait<'a> + use<'a, T>> {
        match self {
            Some(inner) => inner.get_key(name),
            None => None,
        }
    }

    fn get_index(&self, index: usize) -> Option<impl SanshoTrait<'a> + use<'a, T>> {
        match self {
            Some(inner) => inner.get_index(index),
            None => None,
        }
    }

    fn entries(&self) -> Vec<(String, impl SanshoTrait<'a> + use<'a, T>)> {
        match self {
            Some(inner) => inner.entries(),
            None => Vec::new(),
        }
    }

    fn elements(&self) -> Vec<impl SanshoTrait<'a> + use<'a, T>> {
        match self {
            Some(inner) => inner.elements(),
            None => Vec::new(),
        }
    }

    fn container_len(&self) -> Option<usize> {
        match self {
            Some(inner) => inner.container_len(),
            None => None,
        }
    }

    fn materialize(&self) -> Result<J, SanshoError> {
        match self {
            Some(inner) => inner.materialize(),
            None => Ok(J::Null),
        }
    }

    fn counts_toward_aggregation() -> bool {
        false
    }
}

/// The reference form: the inner value's own surface, borrowed.
impl<'a, T: SanshoTrait<'a>> SanshoTrait<'a> for &'a Option<T> {
    fn kind(&self) -> Kind {
        match **self {
            Some(ref inner) => inner.kind(),
            None => Kind::Null,
        }
    }

    fn as_str(&self) -> Option<std::borrow::Cow<'_, str>> {
        match **self {
            Some(ref inner) => inner.as_str(),
            None => None,
        }
    }

    fn as_str_ref(&self) -> Option<&'a str> {
        match **self {
            Some(ref inner) => inner.as_str_ref(),
            None => None,
        }
    }

    fn as_sansho_number(&self) -> Option<SanshoNumber> {
        match **self {
            Some(ref inner) => inner.as_sansho_number(),
            None => None,
        }
    }

    fn as_bool(&self) -> Option<bool> {
        match **self {
            Some(ref inner) => inner.as_bool(),
            None => None,
        }
    }

    fn is_null(&self) -> bool {
        self.is_none()
    }

    fn get_key(&self, name: &str) -> Option<impl SanshoTrait<'a> + use<'a, T>> {
        match **self {
            Some(ref inner) => inner.get_key(name),
            None => None,
        }
    }

    fn get_index(&self, index: usize) -> Option<impl SanshoTrait<'a> + use<'a, T>> {
        match **self {
            Some(ref inner) => inner.get_index(index),
            None => None,
        }
    }

    fn entries(&self) -> Vec<(String, impl SanshoTrait<'a> + use<'a, T>)> {
        match **self {
            Some(ref inner) => inner.entries(),
            None => Vec::new(),
        }
    }

    fn elements(&self) -> Vec<impl SanshoTrait<'a> + use<'a, T>> {
        match **self {
            Some(ref inner) => inner.elements(),
            None => Vec::new(),
        }
    }

    fn container_len(&self) -> Option<usize> {
        match **self {
            Some(ref inner) => inner.container_len(),
            None => None,
        }
    }

    fn materialize(&self) -> Result<J, SanshoError> {
        match **self {
            Some(ref inner) => inner.materialize(),
            None => Ok(J::Null),
        }
    }

    fn counts_toward_aggregation() -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// Arrays: Vec<T>, [T], BTreeSet<T>, HashSet<T> (value forms)
// ---------------------------------------------------------------------------

/// The array value-form macro: members are the elements' OWN
/// materialized values (a value form has no borrowable storage).
macro_rules! array_value_form {
    ($t:ty, $len:expr) => {
        impl<'a, T: SanshoTrait<'a>> SanshoTrait<'a> for $t {
            fn kind(&self) -> Kind {
                Kind::Array
            }

            fn as_str(&self) -> Option<std::borrow::Cow<'_, str>> {
                None
            }

            fn as_sansho_number(&self) -> Option<SanshoNumber> {
                None
            }

            fn as_bool(&self) -> Option<bool> {
                None
            }

            fn is_null(&self) -> bool {
                false
            }

            fn get_key(&self, _name: &str) -> Option<impl SanshoTrait<'a> + use<'a, T>> {
                None::<J>
            }

            fn get_index(&self, index: usize) -> Option<impl SanshoTrait<'a> + use<'a, T>> {
                self.iter()
                    .nth(index)
                    .map(|element| element.materialize().unwrap_or(J::Null))
            }

            fn entries(&self) -> Vec<(String, impl SanshoTrait<'a> + use<'a, T>)> {
                Vec::<(String, J)>::new()
            }

            fn elements(&self) -> Vec<impl SanshoTrait<'a> + use<'a, T>> {
                self.iter()
                    .map(|element| element.materialize().unwrap_or(J::Null))
                    .collect()
            }

            fn container_len(&self) -> Option<usize> {
                Some($len(self))
            }

            fn materialize(&self) -> Result<J, SanshoError> {
                let mut out = Vec::new();
                for element in self {
                    out.push(element.materialize()?);
                }
                Ok(J::Array(out))
            }

            fn counts_toward_aggregation() -> bool {
                false
            }
        }
    };
}

array_value_form!(Vec<T>, |v: &Vec<T>| v.len());
array_value_form!([T], |v: &[T]| v.len());
array_value_form!(BTreeSet<T>, |v: &BTreeSet<T>| v.len());

/// The sorted-value-form hash set; the JSON view sorts the elements
/// (a hash set's iteration order is not deterministic, and the array
/// view must be).
impl<'a, T: SanshoTrait<'a> + Ord> SanshoTrait<'a> for HashSet<T> {
    fn kind(&self) -> Kind {
        Kind::Array
    }

    fn as_str(&self) -> Option<std::borrow::Cow<'_, str>> {
        None
    }

    fn as_sansho_number(&self) -> Option<SanshoNumber> {
        None
    }

    fn as_bool(&self) -> Option<bool> {
        None
    }

    fn is_null(&self) -> bool {
        false
    }

    fn get_key(&self, _name: &str) -> Option<impl SanshoTrait<'a> + use<'a, T>> {
        None::<J>
    }

    fn get_index(&self, index: usize) -> Option<impl SanshoTrait<'a> + use<'a, T>> {
        let mut elements: Vec<&T> = self.iter().collect();
        elements.sort();
        elements
            .get(index)
            .map(|element| element.materialize().unwrap_or(J::Null))
    }

    fn entries(&self) -> Vec<(String, impl SanshoTrait<'a> + use<'a, T>)> {
        Vec::<(String, J)>::new()
    }

    fn elements(&self) -> Vec<impl SanshoTrait<'a> + use<'a, T>> {
        let mut elements: Vec<&T> = self.iter().collect();
        elements.sort();
        elements
            .into_iter()
            .map(|element| element.materialize().unwrap_or(J::Null))
            .collect()
    }

    fn container_len(&self) -> Option<usize> {
        Some(self.len())
    }

    fn materialize(&self) -> Result<J, SanshoError> {
        let mut elements: Vec<&T> = self.iter().collect();
        elements.sort();
        let mut out = Vec::new();
        for element in elements {
            out.push(element.materialize()?);
        }
        Ok(J::Array(out))
    }

    fn counts_toward_aggregation() -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// Maps: BTreeMap<K, V>, HashMap<K, V> (value forms)
// ---------------------------------------------------------------------------

/// The ordered map value-form: members are the values' OWN
/// materialized values; keys render through their `Display` form (the
/// map's own key order).
impl<'a, K, V> SanshoTrait<'a> for BTreeMap<K, V>
where
    K: std::fmt::Display + 'a,
    V: SanshoTrait<'a>,
{
    fn kind(&self) -> Kind {
        Kind::Object
    }

    fn as_str(&self) -> Option<std::borrow::Cow<'_, str>> {
        None
    }

    fn as_sansho_number(&self) -> Option<SanshoNumber> {
        None
    }

    fn as_bool(&self) -> Option<bool> {
        None
    }

    fn is_null(&self) -> bool {
        false
    }

    fn get_key(&self, name: &str) -> Option<impl SanshoTrait<'a> + use<'a, K, V>> {
        self.iter()
            .find(|(key, _)| key.to_string() == name)
            .map(|(_, value)| value.materialize().unwrap_or(J::Null))
    }

    fn get_index(&self, _index: usize) -> Option<impl SanshoTrait<'a> + use<'a, K, V>> {
        None::<J>
    }

    fn entries(&self) -> Vec<(String, impl SanshoTrait<'a> + use<'a, K, V>)> {
        self.iter()
            .map(|(key, value)| (key.to_string(), value.materialize().unwrap_or(J::Null)))
            .collect()
    }

    fn elements(&self) -> Vec<impl SanshoTrait<'a> + use<'a, K, V>> {
        Vec::<J>::new()
    }

    fn container_len(&self) -> Option<usize> {
        Some(self.len())
    }

    fn materialize(&self) -> Result<J, SanshoError> {
        let mut out = serde_json::Map::new();
        for (key, value) in self {
            out.insert(key.to_string(), value.materialize()?);
        }
        Ok(J::Object(out))
    }

    fn counts_toward_aggregation() -> bool {
        false
    }
}

/// The hash-map value-form: members are the values' OWN materialized
/// values; keys render through their `Display` form, SORTED by key
/// string (a hash map's iteration order is not deterministic, and the
/// object view must be).
impl<'a, K, V> SanshoTrait<'a> for HashMap<K, V>
where
    K: std::fmt::Display + 'a,
    V: SanshoTrait<'a>,
{
    fn kind(&self) -> Kind {
        Kind::Object
    }

    fn as_str(&self) -> Option<std::borrow::Cow<'_, str>> {
        None
    }

    fn as_sansho_number(&self) -> Option<SanshoNumber> {
        None
    }

    fn as_bool(&self) -> Option<bool> {
        None
    }

    fn is_null(&self) -> bool {
        false
    }

    fn get_key(&self, name: &str) -> Option<impl SanshoTrait<'a> + use<'a, K, V>> {
        self.iter()
            .find(|(key, _)| key.to_string() == name)
            .map(|(_, value)| value.materialize().unwrap_or(J::Null))
    }

    fn get_index(&self, _index: usize) -> Option<impl SanshoTrait<'a> + use<'a, K, V>> {
        None::<J>
    }

    fn entries(&self) -> Vec<(String, impl SanshoTrait<'a> + use<'a, K, V>)> {
        let mut pairs: Vec<(&K, &V)> = self.iter().collect();
        pairs.sort_by(|a, b| a.0.to_string().cmp(&b.0.to_string()));
        pairs
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.materialize().unwrap_or(J::Null)))
            .collect()
    }

    fn elements(&self) -> Vec<impl SanshoTrait<'a> + use<'a, K, V>> {
        Vec::<J>::new()
    }

    fn container_len(&self) -> Option<usize> {
        Some(self.len())
    }

    fn materialize(&self) -> Result<J, SanshoError> {
        let mut pairs: Vec<(&K, &V)> = self.iter().collect();
        pairs.sort_by(|a, b| a.0.to_string().cmp(&b.0.to_string()));
        let mut out = serde_json::Map::new();
        for (key, value) in pairs {
            out.insert(key.to_string(), value.materialize()?);
        }
        Ok(J::Object(out))
    }

    fn counts_toward_aggregation() -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// Reference forms for the concrete borrowed navigations
// ---------------------------------------------------------------------------

/// The borrowed string-target set: elements are BORROWED `Cow` strings
/// (zero-copy — the strings are the set's own).
impl<'a> SanshoTrait<'a> for &'a BTreeSet<String> {
    fn kind(&self) -> Kind {
        Kind::Array
    }

    fn as_str(&self) -> Option<std::borrow::Cow<'_, str>> {
        None
    }

    fn as_sansho_number(&self) -> Option<SanshoNumber> {
        None
    }

    fn as_bool(&self) -> Option<bool> {
        None
    }

    fn is_null(&self) -> bool {
        false
    }

    fn get_key(&self, _name: &str) -> Option<impl SanshoTrait<'a> + use<'a>> {
        None::<std::borrow::Cow<'a, str>>
    }

    fn get_index(&self, index: usize) -> Option<impl SanshoTrait<'a> + use<'a>> {
        self.iter()
            .nth(index)
            .map(|s| std::borrow::Cow::Borrowed(s.as_str()))
    }

    fn entries(&self) -> Vec<(String, impl SanshoTrait<'a> + use<'a>)> {
        Vec::<(String, std::borrow::Cow<'a, str>)>::new()
    }

    fn elements(&self) -> Vec<impl SanshoTrait<'a> + use<'a>> {
        self.iter()
            .map(|s| std::borrow::Cow::Borrowed(s.as_str()))
            .collect()
    }

    fn container_len(&self) -> Option<usize> {
        Some(self.len())
    }

    fn materialize(&self) -> Result<J, SanshoError> {
        Ok(J::Array(
            self.iter().map(|s| J::String(s.clone())).collect(),
        ))
    }

    fn counts_toward_aggregation() -> bool {
        false
    }
}

/// The borrowed edge-type map: members are the BORROWED target sets
/// (`&'a BTreeSet<String>` — their own trait implementation comes
/// along unchanged).
impl<'a> SanshoTrait<'a> for &'a BTreeMap<String, BTreeSet<String>> {
    fn kind(&self) -> Kind {
        Kind::Object
    }

    fn as_str(&self) -> Option<std::borrow::Cow<'_, str>> {
        None
    }

    fn as_sansho_number(&self) -> Option<SanshoNumber> {
        None
    }

    fn as_bool(&self) -> Option<bool> {
        None
    }

    fn is_null(&self) -> bool {
        false
    }

    fn get_key(&self, name: &str) -> Option<impl SanshoTrait<'a> + use<'a>> {
        self.get(name)
    }

    fn get_index(&self, _index: usize) -> Option<impl SanshoTrait<'a> + use<'a>> {
        None::<&'a BTreeSet<String>>
    }

    fn entries(&self) -> Vec<(String, impl SanshoTrait<'a> + use<'a>)> {
        self.iter().map(|(k, v)| (k.clone(), v)).collect()
    }

    fn elements(&self) -> Vec<impl SanshoTrait<'a> + use<'a>> {
        Vec::<&'a BTreeSet<String>>::new()
    }

    fn container_len(&self) -> Option<usize> {
        Some(self.len())
    }

    fn materialize(&self) -> Result<J, SanshoError> {
        let mut out = serde_json::Map::new();
        for (key, value) in *self {
            out.insert(
                key.clone(),
                value.iter().map(|s| J::String(s.clone())).collect(),
            );
        }
        Ok(J::Object(out))
    }

    fn counts_toward_aggregation() -> bool {
        false
    }
}