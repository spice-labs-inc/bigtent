//! The materialized tree view: a [`Node`](crate::view::Node)
//! implementation over an in-memory JSON value.
//!
//! This backend is the conformance reference — evaluation here is
//! deliberately simple, over fully materialized structure. The cursor
//! backend (raw CBOR bytes, selective materialization) must agree with
//! this one exactly; the agreement is tested differentially.

use std::borrow::Cow;

use crate::error::SanshoError;
use crate::view::{Kind, Node};

/// A node positioned in a materialized (in-memory) document.
#[derive(Clone, Copy, Debug)]
pub struct MaterializedNode<'d> {
    value: &'d serde_json::Value,
}

impl<'d> MaterializedNode<'d> {
    /// Position at the document root.
    pub fn root(document: &'d serde_json::Value) -> Self {
        MaterializedNode { value: document }
    }
}

impl<'d> Node<'d> for MaterializedNode<'d> {
    fn kind(&self) -> Kind {
        match self.value {
            serde_json::Value::Object(_) => Kind::Object,
            serde_json::Value::Array(_) => Kind::Array,
            serde_json::Value::String(_) => Kind::String,
            serde_json::Value::Number(_) => Kind::Number,
            serde_json::Value::Bool(_) => Kind::Bool,
            serde_json::Value::Null => Kind::Null,
        }
    }

    fn as_str(&self) -> Option<Cow<'d, str>> {
        self.value.as_str().map(Cow::Borrowed)
    }

    fn as_f64(&self) -> Option<f64> {
        self.value.as_f64()
    }

    fn as_bool(&self) -> Option<bool> {
        self.value.as_bool()
    }

    fn is_null(&self) -> bool {
        self.value.is_null()
    }

    fn get_key(&self, name: &str) -> Option<Self> {
        self.value.get(name).map(|v| MaterializedNode { value: v })
    }

    fn get_index(&self, index: usize) -> Option<Self> {
        self.value.get(index).map(|v| MaterializedNode { value: v })
    }

    fn entries(&self) -> Vec<(String, Self)> {
        match self.value.as_object() {
            Some(map) => map
                .iter()
                .map(|(k, v)| (k.clone(), MaterializedNode { value: v }))
                .collect(),
            None => Vec::new(),
        }
    }

    fn elements(&self) -> Vec<Self> {
        match self.value.as_array() {
            Some(items) => items
                .iter()
                .map(|v| MaterializedNode { value: v })
                .collect(),
            None => Vec::new(),
        }
    }

    fn container_len(&self) -> Option<usize> {
        match self.value {
            serde_json::Value::Object(map) => Some(map.len()),
            serde_json::Value::Array(items) => Some(items.len()),
            _ => None,
        }
    }

    fn counts_toward_aggregation() -> bool {
        false
    }

    fn materialize(&self) -> Result<serde_json::Value, SanshoError> {
        Ok(self.value.clone())
    }
}
