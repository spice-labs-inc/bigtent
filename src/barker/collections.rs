//! Shaping a collection into a summary: grouping, deduplication, flattening.
//!
//! The Starlark standard library already has `sorted`, `min`, `max`, `len`,
//! `enumerate`, `zip` and `reversed`. These are the operations that turn a stream
//! into groups before any arithmetic happens, which is what a summary needs.

use crate::barker::values::{as_number, as_string};
use starlark::environment::GlobalsBuilder;
use starlark::starlark_module;
use starlark::values::list::ListRef;
use starlark::values::Value;

/// A Starlark error as the `anyhow` error these natives return.
fn to_anyhow(error: starlark::Error) -> anyhow::Error {
    anyhow::anyhow!("{error}")
}

/// The elements of a list, or an error naming what arrived instead.
fn elements<'v>(value: Value<'v>) -> anyhow::Result<Vec<Value<'v>>> {
    let Some(list) = ListRef::from_value(value) else {
        anyhow::bail!("a list is required, found `{}`", value.get_type());
    };
    Ok(list.iter().collect())
}

#[starlark_module]
pub fn collections(builder: &mut GlobalsBuilder) {
    /// Drop repeated values, keeping the first of each and the original order.
    ///
    /// Values are compared by their rendered form, so a string and the number
    /// that prints the same are distinct.
    fn unique(values: Value) -> anyhow::Result<serde_json::Value> {
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for element in elements(values)? {
            let key = element.to_repr();
            if seen.insert(key) {
                out.push(element.to_json_value()?);
            }
        }
        Ok(serde_json::Value::Array(out))
    }

    /// Flatten one level of nesting.
    ///
    /// A list of lists becomes a list of their elements; an element that is not
    /// a list stays as it is.
    fn flatten(values: Value) -> anyhow::Result<serde_json::Value> {
        let mut out = Vec::new();
        for element in elements(values)? {
            match ListRef::from_value(element) {
                Some(inner) => {
                    for member in inner.iter() {
                        out.push(member.to_json_value()?);
                    }
                }
                None => out.push(element.to_json_value()?),
            }
        }
        Ok(serde_json::Value::Array(out))
    }

    /// Split a list into consecutive lists of at most `size` elements.
    fn chunks(values: Value, size: Value) -> anyhow::Result<serde_json::Value> {
        let size = as_number(size).ok_or_else(|| anyhow::anyhow!("a number is required"))? as usize;
        if size == 0 {
            anyhow::bail!("a chunk size is at least 1");
        }
        let elements = elements(values)?;
        let mut out = Vec::new();
        for group in elements.chunks(size) {
            let mut chunk = Vec::with_capacity(group.len());
            for element in group {
                chunk.push(element.to_json_value()?);
            }
            out.push(serde_json::Value::Array(chunk));
        }
        Ok(serde_json::Value::Array(out))
    }

    /// Group values by a key.
    ///
    /// `key` is a function of one element returning the group it belongs to; the
    /// result maps each key's rendered form to the list of values that produced
    /// it, in first-seen order of both.
    fn group_by<'v>(
        values: Value<'v>,
        key: Value<'v>,
        eval: &mut starlark::eval::Evaluator<'v, '_, '_>,
    ) -> anyhow::Result<serde_json::Value> {
        let mut groups: Vec<(String, Vec<serde_json::Value>)> = Vec::new();
        for element in elements(values)? {
            let named = eval.eval_function(key, &[element], &[]).map_err(to_anyhow)?;
            let group = named.to_repr();
            match groups.iter_mut().find(|(name, _)| *name == group) {
                Some((_, members)) => members.push(element.to_json_value()?),
                None => groups.push((group, vec![element.to_json_value()?])),
            }
        }
        let mut out = serde_json::Map::new();
        for (name, members) in groups {
            out.insert(name, serde_json::Value::Array(members));
        }
        Ok(serde_json::Value::Object(out))
    }

    /// Whether any element satisfies a predicate.
    fn any_of<'v>(
        values: Value<'v>,
        predicate: Value<'v>,
        eval: &mut starlark::eval::Evaluator<'v, '_, '_>,
    ) -> anyhow::Result<bool> {
        for element in elements(values)? {
            if eval.eval_function(predicate, &[element], &[]).map_err(to_anyhow)?.to_bool() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Whether every element satisfies a predicate.
    fn all_of<'v>(
        values: Value<'v>,
        predicate: Value<'v>,
        eval: &mut starlark::eval::Evaluator<'v, '_, '_>,
    ) -> anyhow::Result<bool> {
        for element in elements(values)? {
            if !eval.eval_function(predicate, &[element], &[]).map_err(to_anyhow)?.to_bool() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// How many elements satisfy a predicate.
    fn count_if<'v>(
        values: Value<'v>,
        predicate: Value<'v>,
        eval: &mut starlark::eval::Evaluator<'v, '_, '_>,
    ) -> anyhow::Result<i64> {
        let mut n = 0;
        for element in elements(values)? {
            if eval.eval_function(predicate, &[element], &[]).map_err(to_anyhow)?.to_bool() {
                n += 1;
            }
        }
        Ok(n)
    }

    /// The first element satisfying a predicate, or `None`.
    fn find_first<'v>(
        values: Value<'v>,
        predicate: Value<'v>,
        eval: &mut starlark::eval::Evaluator<'v, '_, '_>,
    ) -> anyhow::Result<serde_json::Value> {
        for element in elements(values)? {
            if eval.eval_function(predicate, &[element], &[]).map_err(to_anyhow)?.to_bool() {
                return element.to_json_value();
            }
        }
        Ok(serde_json::Value::Null)
    }
}

/// A list of strings, for the functions that only make sense over text.
pub fn as_strings(value: Value) -> anyhow::Result<Vec<String>> {
    let mut out = Vec::new();
    for element in elements(value)? {
        out.push(as_string(element)?.to_string());
    }
    Ok(out)
}
