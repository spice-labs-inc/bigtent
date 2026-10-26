//! Reading Starlark values the way Barker's natives need them.
//!
//! `#[starlark_module]` unpacks only `i32`/`i64`/`usize`-shaped arguments out of
//! the box, and the enum that accepts both integers and floats (`NumRef`) is
//! crate-private. So a native takes a [`Value`] and converts here, which keeps
//! the conversion in one place instead of repeated in every function.
//!
//! The conversions are deliberately lenient about which number type arrives and
//! strict about anything else: a Starlark int and a Starlark float are both
//! numbers to a caller, while a string or a list is a mistake that says so.

use starlark::values::float::StarlarkFloat;
use starlark::values::list::ListRef;
use starlark::values::{Value, ValueLike};

/// A Starlark number — integer or float — as an `f64`.
///
/// A float is read through `StarlarkFloat` rather than through the value's JSON
/// form, and that is deliberate: JSON has no NaN and no infinity, so a JSON round
/// trip would refuse exactly the values `stats.is_nan` and `stats.is_finite` exist
/// to detect. A native that guards against a NaN has to be able to receive one.
///
/// `None` for anything that is not a number.
pub fn as_number(value: Value) -> Option<f64> {
    if let Some(float) = value.downcast_ref::<StarlarkFloat>() {
        return Some(float.0);
    }
    // Starlark integers are arbitrary precision; `i64` carries what a query
    // computes in practice, including epoch milliseconds, which do not fit in an
    // `i32`.
    value.unpack_i32().map(f64::from).or_else(|| {
        value
            .to_json_value()
            .ok()
            .and_then(|json| json.as_i64())
            .map(|integer| integer as f64)
    })
}

/// A Starlark integer as an `i64`.
pub fn as_integer(value: Value) -> Option<i64> {
    value
        .unpack_i32()
        .map(i64::from)
        .or_else(|| value.to_json_value().ok().and_then(|json| json.as_i64()))
}

/// A Starlark list as a sequence of numbers.
///
/// An element that is not a number is an error naming its position, because a
/// silently skipped element would change every statistic computed from it.
pub fn as_numbers(value: Value) -> anyhow::Result<Vec<f64>> {
    let Some(list) = ListRef::from_value(value) else {
        anyhow::bail!("a list of numbers is required, found `{}`", value.get_type());
    };
    let mut numbers = Vec::with_capacity(list.len());
    for (position, element) in list.iter().enumerate() {
        let Some(number) = as_number(element) else {
            anyhow::bail!(
                "element {position} is not a number, found `{}`",
                element.get_type()
            );
        };
        numbers.push(number);
    }
    Ok(numbers)
}

/// A Starlark string as a `&str`.
pub fn as_string<'v>(value: Value<'v>) -> anyhow::Result<&'v str> {
    value
        .unpack_str()
        .ok_or_else(|| anyhow::anyhow!("a string is required, found `{}`", value.get_type()))
}

/// A Starlark string's bytes, for the functions that work on content.
pub fn as_bytes<'v>(value: Value<'v>) -> anyhow::Result<&'v [u8]> {
    Ok(as_string(value)?.as_bytes())
}
