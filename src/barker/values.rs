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

use crate::barker::error::BarkerError;
use starlark::values::float::StarlarkFloat;
use starlark::values::dict::DictRef;
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
pub fn as_numbers(function: &'static str, value: Value) -> Result<Vec<f64>, BarkerError> {
    let Some(list) = ListRef::from_value(value) else {
        return Err(BarkerError::type_error(function, "values", "a list", value));
    };
    let mut numbers = Vec::with_capacity(list.len());
    for (position, element) in list.iter().enumerate() {
        let Some(number) = as_number(element) else {
            return Err(BarkerError::Type {
                function,
                argument: "values",
                expected: "a list of numbers",
                found: format!("`{}` at element {position}", element.get_type()),
            });
        };
        numbers.push(number);
    }
    Ok(numbers)
}

/// A Starlark string as a `&str`.
pub fn as_string<'v>(function: &'static str, value: Value<'v>) -> Result<&'v str, BarkerError> {
    value
        .unpack_str()
        .ok_or_else(|| BarkerError::type_error(function, "text", "a string", value))
}

/// A Starlark string's bytes, for the functions that work on content.
pub fn as_bytes<'v>(function: &'static str, value: Value<'v>) -> Result<&'v [u8], BarkerError> {
    Ok(as_string(function, value)?.as_bytes())
}

/// The deepest a value may nest when converted to a JSON row.
///
/// Starlark's own `to_json_value` recurses through serde with no bound, so a
/// script that nests a value a thousand deep (`x = [x]` a thousand times)
/// overflowed the thread's stack and aborted the whole server. This walker
/// refuses beyond the limit — an error, which the emit turns into an error
/// row — instead of dying. Sixty-four matches the expression parser's
/// nesting limit in spirit: scripts nest by construction, not by thousands.
pub const MAX_JSON_DEPTH: usize = 64;

/// A Starlark value as a JSON value, refusing to recurse past
/// [`MAX_JSON_DEPTH`].
pub fn to_json_limited(value: Value, depth: usize) -> anyhow::Result<serde_json::Value> {
    if depth > MAX_JSON_DEPTH {
        anyhow::bail!(
            "the emitted value nests deeper than {MAX_JSON_DEPTH} levels"
        );
    }
    // the scalars, in the same order `as_number`/`as_string` read them
    if let Some(text) = value.unpack_str() {
        return Ok(serde_json::Value::String(text.to_string()));
    }
    if let Some(flag) = value.unpack_bool() {
        return Ok(serde_json::Value::Bool(flag));
    }
    if let Some(number) = value.downcast_ref::<StarlarkFloat>() {
        return Ok(serde_json::json!(number.0));
    }
    if let Some(integer) = value.unpack_i32() {
        return Ok(serde_json::json!(integer));
    }
    if value.get_type() == "NoneType" {
        return Ok(serde_json::Value::Null);
    }
    // the containers: a dict's keys are strings by construction
    if let Some(dict) = DictRef::from_value(value) {
        let mut object = serde_json::Map::new();
        for (key, member) in dict.iter() {
            // PYTHON'S RULE, which is the reference behavior for a dict
            // becoming JSON: every key becomes a string; the accepted key
            // types are str, int, float, bool and None; anything else is a
            // TypeError ("keys must be str, int, float, bool or None").
            let text = match key_of(key)? {
                KeyValue::Text(text) => text,
                KeyValue::Integer(integer) => integer.to_string(),
                KeyValue::Float(bits) => python_float_text(f64::from_bits(bits as u64)),
                KeyValue::Bool(flag) => flag.to_string(),
                KeyValue::Null => "null".to_string(),
                KeyValue::Rendered(_) => anyhow::bail!(
                    "keys must be str, int, float, bool or None, not {}",
                    key.get_type()
                ),
            };
            object.insert(text, to_json_limited(member, depth + 1)?);
        }
        return Ok(serde_json::Value::Object(object));
    }
    if let Some(list) = ListRef::from_value(value) {
        let mut items = Vec::with_capacity(list.len());
        for element in list.iter() {
            items.push(to_json_limited(element, depth + 1)?);
        }
        return Ok(serde_json::Value::Array(items));
    }
    // the exotic remainder (bytes, tuples with mixed content, native types):
    // starlark's own conversion, which cannot nest deeper than the value
    // already is — and scripts build their values through the constructors
    // above
    value.to_json_value()
}

/// What a value IS, as a key or a set member.
///
/// The classification decides what the key *is*, not merely what it prints as:
/// a string is the string, a number the number, a boolean the boolean — so a
/// Starlark dict keyed by these keeps `42` and `"42"` distinct, exactly as a
/// Python dict does. Only a value with no scalar identity is `Rendered`, by its
/// JSON text — and a value that cannot be rendered is an ERROR, never a silent
/// empty key.
///
/// `Hash`/`Eq` are derived: the variant is part of the identity, so set
/// membership distinguishes the types too.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum KeyValue {
    /// A string value: the string itself.
    Text(String),
    /// An integer, as an integer.
    Integer(i64),
    /// A float, as a float.
    Float(i64),
    /// A boolean.
    Bool(bool),
    /// `None` — its own identity, rendered `null` at the JSON boundary.
    Null,
    /// Anything without a scalar identity: its JSON representation.
    Rendered(String),
}

/// The key a value presents as, or an error when it has no representation.
///
/// THE one classification every keying and set-membership site uses, so the
/// functions cannot disagree about what a value is called.
pub fn key_of(value: Value) -> anyhow::Result<KeyValue> {
    if let Some(text) = value.unpack_str() {
        return Ok(KeyValue::Text(text.to_string()));
    }
    if let Some(flag) = value.unpack_bool() {
        return Ok(KeyValue::Bool(flag));
    }
    if let Some(integer) = value.unpack_i32() {
        return Ok(KeyValue::Integer(i64::from(integer)));
    }
    if value.get_type() == "NoneType" {
        return Ok(KeyValue::Null);
    }
    if value.get_type() == "NoneType" {
        return Ok(KeyValue::Null);
    }
    if let Some(float) = value.downcast_ref::<StarlarkFloat>() {
        // a float's identity is its bit pattern, so NaN and -0.0 behave as a
        // set member should
        return Ok(KeyValue::Float(float.0.to_bits() as i64));
    }
    // anything else: its JSON representation, or an error — never a silent
    // empty key
    let rendered = value
        .to_json_value()
        .map_err(|error| BarkerError::unrepresentable(
            "the value has no representation",
            error.to_string(),
        ))?;
    Ok(KeyValue::Rendered(rendered.to_string()))
}

/// A float as Python's JSON encoder writes it when it is a KEY: the shortest
/// round-tripping form, with the JavaScript spellings for the non-finite values
/// (`NaN`, `Infinity`, `-Infinity`).
fn python_float_text(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value == f64::INFINITY {
        "Infinity".to_string()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        format!("{value:?}")
    }
}

/// A Starlark number as `f64`, or a TYPE error naming the argument.
pub fn number_argument(
    function: &'static str,
    argument: &'static str,
    value: Value,
) -> Result<f64, BarkerError> {
    as_number(value)
        .ok_or_else(|| BarkerError::type_error(function, argument, "a number", value))
}

/// A Starlark string as `&str`, or a TYPE error naming the argument.
pub fn string_argument<'v>(
    function: &'static str,
    argument: &'static str,
    value: Value<'v>,
) -> Result<&'v str, BarkerError> {
    value
        .unpack_str()
        .ok_or_else(|| BarkerError::type_error(function, argument, "a string", value))
}

/// A Starlark list as its elements, or a TYPE error naming the argument.
pub fn list_argument<'v>(
    function: &'static str,
    argument: &'static str,
    value: Value<'v>,
) -> Result<Vec<Value<'v>>, BarkerError> {
    match starlark::values::list::ListRef::from_value(value) {
        Some(list) => Ok(list.iter().collect()),
        None => Err(BarkerError::type_error(function, argument, "a list", value)),
    }
}

/// A number argument as a WHOLE integer, or an error naming the value the
/// script actually passed.
///
/// The `as` cast this replaces truncated (`1.9` became `1`) and saturated
/// non-finite values (`NaN` became `0`, `inf` became `i64::MAX`) — so a script
/// asking for something impossible got a plausible number instead of a
/// refusal, often with a message naming a number it never passed.
///
/// Refused, each naming the offending value: a non-number (type error), a
/// non-finite value, a fractional value, and one outside `i64`.
pub fn integer_argument(
    function: &'static str,
    argument: &'static str,
    value: Value,
) -> Result<i64, BarkerError> {
    let number = as_number(value)
        .ok_or_else(|| BarkerError::type_error(function, argument, "a number", value))?;
    let shown = show_number(number);
    if !number.is_finite() {
        return Err(BarkerError::value(
            function,
            format!("`{argument}` must be a finite number, found {shown}"),
        ));
    }
    if number.fract() != 0.0 {
        return Err(BarkerError::value(
            function,
            format!("`{argument}` must be a whole number, found {shown}"),
        ));
    }
    if number < i64::MIN as f64 || number > i64::MAX as f64 {
        return Err(BarkerError::range(
            function,
            format!("`{argument}` is out of range: {shown}"),
        ));
    }
    Ok(number as i64)
}

/// A number as a script would write it — `nan` and `inf` in the spellings a
/// Starlark script uses, so an error message matches what the author typed.
fn show_number(number: f64) -> String {
    if number.is_nan() {
        "nan".to_string()
    } else if number == f64::INFINITY {
        "inf".to_string()
    } else if number == f64::NEG_INFINITY {
        "-inf".to_string()
    } else {
        format!("{number}")
    }
}
