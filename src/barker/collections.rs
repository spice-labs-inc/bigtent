//! Shaping a collection into a summary: grouping, deduplication, flattening.
//!
//! The Starlark standard library already has `sorted`, `min`, `max`, `len`,
//! `enumerate`, `zip` and `reversed`. These are the operations that turn a stream
//! into groups before any arithmetic happens, which is what a summary needs.

use crate::barker::error::BarkerError;
use crate::barker::values::{key_of, list_argument, string_argument};
use starlark::environment::GlobalsBuilder;
use starlark::starlark_module;
use starlark::values::list::ListRef;
use starlark::values::Value;



/// The elements of a list, or an error naming what arrived instead.
fn elements<'v>(
    function: &'static str,
    value: Value<'v>,
) -> Result<Vec<Value<'v>>, BarkerError> {
    list_argument(function, "values", value)
}

#[starlark_module]
pub fn collections(builder: &mut GlobalsBuilder) {
    /// Drop repeated values, keeping the first of each and the original order.
    ///
    /// Identity is the value's own — a string is the string, a number the
    /// number — so `"42"` and `42` are two members, exactly as a Python set
    /// treats them. The VALUES are returned untouched (a native Starlark list):
    /// nothing is translated.
    fn unique<'v>(
        values: Value<'v>,
        eval: &mut starlark::eval::Evaluator<'v, '_, '_>,
    ) -> starlark::Result<Value<'v>> {
        let heap = eval.heap();
        let mut seen = std::collections::HashSet::new();
        let mut out: Vec<Value<'v>> = Vec::new();
        for element in elements("unique", values)? {
            if seen.insert(key_of(element)?) {
                out.push(element);
            }
        }
        Ok(heap.alloc(out))
    }

    /// Flatten one level of nesting.
    ///
    /// A list of lists becomes a list of their elements; an element that is not
    /// a list stays as it is. The elements are returned untouched — native
    /// Starlark values, no translation.
    fn flatten<'v>(
        values: Value<'v>,
        eval: &mut starlark::eval::Evaluator<'v, '_, '_>,
    ) -> starlark::Result<Value<'v>> {
        let heap = eval.heap();
        let mut out: Vec<Value<'v>> = Vec::new();
        for element in elements("flatten", values)? {
            match ListRef::from_value(element) {
                Some(inner) => out.extend(inner.iter()),
                None => out.push(element),
            }
        }
        Ok(heap.alloc(out))
    }

    /// Split a list into consecutive lists of at most `size` elements.
    ///
    /// A native list of native lists; the elements are untouched.
    fn chunks<'v>(
        values: Value<'v>,
        size: Value<'v>,
        eval: &mut starlark::eval::Evaluator<'v, '_, '_>,
    ) -> starlark::Result<Value<'v>> {
        let heap = eval.heap();
        // a WHOLE, finite, positive size: a fractional or non-finite value is
        // refused naming the value, never truncated into a different size
        let size = crate::barker::values::integer_argument("chunks", "size", size)?;
        if size < 1 {
            return Err(BarkerError::value(
                "chunks",
                format!("a chunk size is at least 1, found {size}"),
            )
            .into());
        }
        let size = size as usize;
        let elements = elements("chunks", values)?;
        let mut out: Vec<Value<'v>> = Vec::new();
        for group in elements.chunks(size) {
            out.push(heap.alloc(group.to_vec()));
        }
        Ok(heap.alloc(out))
    }

    /// Group values by a key.
    ///
    /// `key` is a function of one element returning the group it belongs to.
    /// The result is a NATIVE Starlark dict whose keys are the classifier's own
    /// returned values — a string stays a string, a number a number, a boolean
    /// a boolean — so `groups["MIT"]` finds what a developer put in. Only a
    /// value with no scalar identity is keyed by its JSON text; one that cannot
    /// be rendered is an error. The member lists are the original values, in
    /// first-seen order.
    fn group_by<'v>(
        values: Value<'v>,
        key: Value<'v>,
        eval: &mut starlark::eval::Evaluator<'v, '_, '_>,
    ) -> starlark::Result<Value<'v>> {
        let heap = eval.heap();
        // (the classified key, the original classifier result, the members)
        let mut groups: Vec<(crate::barker::values::KeyValue, Value<'v>, Vec<Value<'v>>)> =
            Vec::new();
        for element in elements("group_by", values)? {
            let named = eval.eval_function(key, &[element], &[])?;
            let classified = key_of(named)?;
            match groups.iter_mut().find(|(k, _, _)| *k == classified) {
                Some((_, _, members)) => members.push(element),
                None => groups.push((classified, named, vec![element])),
            }
        }
        let mut entries: Vec<(Value<'v>, Value<'v>)> = Vec::with_capacity(groups.len());
        for (_, named, members) in groups {
            entries.push((named, heap.alloc(members)));
        }
        Ok(heap.alloc(starlark::values::dict::AllocDict(entries)))
    }

    /// Whether any element satisfies a predicate.
    fn any_of<'v>(
        values: Value<'v>,
        predicate: Value<'v>,
        eval: &mut starlark::eval::Evaluator<'v, '_, '_>,
    ) -> starlark::Result<bool> {
        for element in elements("any_of", values)? {
            if eval.eval_function(predicate, &[element], &[])?.to_bool() {
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
    ) -> starlark::Result<bool> {
        for element in elements("all_of", values)? {
            if !eval.eval_function(predicate, &[element], &[])?.to_bool() {
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
    ) -> starlark::Result<i64> {
        let mut n = 0;
        for element in elements("count_if", values)? {
            if eval.eval_function(predicate, &[element], &[])?.to_bool() {
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
    ) -> starlark::Result<Value<'v>> {
        for element in elements("find_first", values)? {
            if eval.eval_function(predicate, &[element], &[])?.to_bool() {
                // the ORIGINAL value, untouched — a native Starlark value
                return Ok(element);
            }
        }
        Ok(eval.heap().alloc(starlark::values::none::NoneType))
    }
}

/// A list of strings, for the functions that only make sense over text.
pub fn as_strings(function: &'static str, value: Value) -> Result<Vec<String>, BarkerError> {
    let mut out = Vec::new();
    for element in elements(function, value)? {
        out.push(string_argument(function, "values", element)?.to_string());
    }
    Ok(out)
}
