//! The JSON value source: `serde_json::Value` and `&serde_json::Value`
//! implemented directly on the trait surface.
//!
//! Two forms, both on the trait:
//! - `J` (the value form): the natural call shape —
//!   `lookup(&value, …)`; members are OWNED values (clones) — a value
//!   form has no borrowable storage, its members are computed values.
//! - `&'a J` (the borrowed form): members are BORROWED values — an
//!   `&'a J`'s members are `&'a J` (zero-copy; the item/body
//!   navigations that hold an in-memory JSON tree use this form).

use crate::error::SanshoError;
use crate::view::{Kind, SanshoNumber, SanshoTrait};
use serde_json::Value as J;

/// The value form: members are owned computed values.
impl<'a> SanshoTrait<'a> for J {
    fn kind(&self) -> Kind {
        match self {
            J::Object(_) => Kind::Object,
            J::Array(_) => Kind::Array,
            J::String(_) => Kind::String,
            J::Number(_) => Kind::Number,
            J::Bool(_) => Kind::Bool,
            J::Null => Kind::Null,
        }
    }

    fn as_str(&self) -> Option<std::borrow::Cow<'_, str>> {
        match self {
            J::String(s) => Some(std::borrow::Cow::Borrowed(s)),
            _ => None,
        }
    }

    fn as_sansho_number(&self) -> Option<SanshoNumber> {
        let number = self.as_number()?;
        // exact first: serde_json preserves i64/u64 exactly — report
        // them full-width, never narrowed through f64
        if let Some(v) = number.as_i64() {
            return Some(SanshoNumber::I64(v));
        }
        if let Some(v) = number.as_u64() {
            return Some(SanshoNumber::U64(v));
        }
        number.as_f64().map(SanshoNumber::F64)
    }

    fn as_bool(&self) -> Option<bool> {
        match self {
            J::Bool(value) => Some(*value),
            _ => None,
        }
    }

    fn is_null(&self) -> bool {
        matches!(self, J::Null)
    }

    fn get_key(&self, name: &str) -> Option<impl SanshoTrait<'a> + use<'a>> {
        self.get(name).cloned()
    }

    fn get_index(&self, index: usize) -> Option<impl SanshoTrait<'a> + use<'a>> {
        self.get(index).cloned()
    }

    fn entries(&self) -> Vec<(String, impl SanshoTrait<'a> + use<'a>)> {
        self.as_object()
            .map(|map| {
                map.iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn elements(&self) -> Vec<impl SanshoTrait<'a> + use<'a>> {
        self.as_array()
            .map(|items| items.to_vec())
            .unwrap_or_default()
    }

    fn container_len(&self) -> Option<usize> {
        match self {
            J::Object(map) => Some(map.len()),
            J::Array(items) => Some(items.len()),
            _ => None,
        }
    }

    fn counts_toward_aggregation() -> bool {
        // the value is the CALLER's pre-existing data — nothing is
        // engine-created
        false
    }

    fn materialize(&self) -> Result<J, SanshoError> {
        Ok(self.clone())
    }
}

/// The borrowed form: members are BORROWED values (zero-copy).
impl<'a> SanshoTrait<'a> for &'a J {
    fn kind(&self) -> Kind {
        match **self {
            J::Object(_) => Kind::Object,
            J::Array(_) => Kind::Array,
            J::String(_) => Kind::String,
            J::Number(_) => Kind::Number,
            J::Bool(_) => Kind::Bool,
            J::Null => Kind::Null,
        }
    }

    fn as_str(&self) -> Option<std::borrow::Cow<'_, str>> {
        match self {
            J::String(s) => Some(std::borrow::Cow::Borrowed(s)),
            _ => None,
        }
    }

    fn as_sansho_number(&self) -> Option<SanshoNumber> {
        let number = self.as_number()?;
        if let Some(v) = number.as_i64() {
            return Some(SanshoNumber::I64(v));
        }
        if let Some(v) = number.as_u64() {
            return Some(SanshoNumber::U64(v));
        }
        number.as_f64().map(SanshoNumber::F64)
    }

    fn as_bool(&self) -> Option<bool> {
        match **self {
            J::Bool(value) => Some(value),
            _ => None,
        }
    }

    fn is_null(&self) -> bool {
        matches!(**self, J::Null)
    }

    fn get_key(&self, name: &str) -> Option<impl SanshoTrait<'a> + use<'a>> {
        // the receiver is a &'a J; the member is the borrowed sub-value
        (**self).get(name)
    }

    fn get_index(&self, index: usize) -> Option<impl SanshoTrait<'a> + use<'a>> {
        (**self).get(index)
    }

    fn entries(&self) -> Vec<(String, impl SanshoTrait<'a> + use<'a>)> {
        (**self)
            .as_object()
            .map(|map| map.iter().map(|(k, v)| (k.clone(), v)).collect())
            .unwrap_or_default()
    }

    fn elements(&self) -> Vec<impl SanshoTrait<'a> + use<'a>> {
        (**self)
            .as_array()
            .map(|items| items.iter().map(|v| v).collect())
            .unwrap_or_default()
    }

    fn container_len(&self) -> Option<usize> {
        match *self {
            &J::Object(ref map) => Some(map.len()),
            &J::Array(ref items) => Some(items.len()),
            _ => None,
        }
    }

    fn counts_toward_aggregation() -> bool {
        false
    }

    fn materialize(&self) -> Result<J, SanshoError> {
        Ok((**self).clone())
    }
}