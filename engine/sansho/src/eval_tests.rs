//! Tests for the `to_string` rendering and its escape.
//!
//! The requirement is the JMESPath specification's `to_string` function: the
//! JSON-encoded value of the argument, with a string argument returned as it
//! is. The compliance corpus pins five of its behaviours (`to_string('foo')`,
//! ``to_string(`1.2`)``, ``to_string(`[0, 1]`)``, and the two function
//! projections `decimals[].to_string(@)` and `numbers[].to_string(@)`); it
//! covers none of the escaping, the control range, the member keys, the
//! integral floats, or the containers' member rendering. These tests cover
//! those, because a corpus that does not exercise a behaviour cannot referee
//! it.

use super::*;

/// A carrier over a borrowed document value — the shape the evaluator
/// hands around.
fn carrier(value: &serde_json::Value) -> SanshoCarrier<'_> {
    Rc::new(value)
}

/// The rendering of one value, at the top level (`quote_string` false) or
/// as a container member (`quote_string` true).
fn render(value: &serde_json::Value, quote_string: bool) -> String {
    let value = carrier(value);
    string_for(&value, quote_string).expect("the value renders")
}

// Requirement: `to_string` returns the JSON text of a non-string argument,
// and a string argument as it is. What: the top-level rendering of a
// string, which is the argument's own text — unquoted and unescaped.
// Why: the specification's string special case is the one place a string
// is NOT JSON text, and quoting it would be wrong (`to_string('foo')` is
// `"foo"`, not `"\"foo\""`).
//
// LLM section: at the top level a string is returned raw, including
// characters that WOULD need escaping inside a container. The same value
// rendered as a member is quoted and escaped (see
// `a_member_string_is_quoted_and_escaped`), which is why the rendering
// takes a `quote_string` flag rather than being a single function of the
// value.
#[test]
fn a_string_argument_is_returned_as_it_is() {
    assert_eq!(render(&serde_json::json!("foo"), false), "foo");
    assert_eq!(render(&serde_json::json!(""), false), "");
    assert_eq!(render(&serde_json::json!("a\"b"), false), "a\"b");
    assert_eq!(render(&serde_json::json!("a\\b"), false), "a\\b");
    assert_eq!(render(&serde_json::json!("a\nb"), false), "a\nb");
    assert_eq!(render(&serde_json::json!("✓foo"), false), "✓foo");
}

// Requirement: a string that is a member of a container is part of that
// container's JSON text, so it must be quoted and escaped. What: the
// member rendering of the strings that need escapes. Why: a raw quote or
// backslash inside a JSON string produces invalid JSON, and a raw control
// character is forbidden by the format outright.
//
// LLM section: the expectations are the reference implementation's. The
// escapes are the two JSON forbids structurally (the quotation mark and
// the reverse solidus), the five control characters with a short form
// (`\b \f \n \r \t`), and `\u00XX` for the rest of the control range.
// Everything above U+001F — including all non-ASCII text — is legal raw.
#[test]
fn a_member_string_is_quoted_and_escaped() {
    assert_eq!(render(&serde_json::json!("foo"), true), "\"foo\"");
    assert_eq!(render(&serde_json::json!("a\"b"), true), "\"a\\\"b\"");
    assert_eq!(render(&serde_json::json!("a\\b"), true), "\"a\\\\b\"");
    assert_eq!(render(&serde_json::json!("a\nb"), true), "\"a\\nb\"");
    assert_eq!(render(&serde_json::json!("a\rb"), true), "\"a\\rb\"");
    assert_eq!(render(&serde_json::json!("a\tb"), true), "\"a\\tb\"");
    assert_eq!(render(&serde_json::json!("a\u{8}b"), true), "\"a\\bb\"");
    assert_eq!(render(&serde_json::json!("a\u{c}b"), true), "\"a\\fb\"");
    assert_eq!(render(&serde_json::json!("a\u{0}b"), true), "\"a\\u0000b\"");
    assert_eq!(render(&serde_json::json!("a\u{1f}b"), true), "\"a\\u001fb\"");
    // above the control range: raw, no escape
    assert_eq!(render(&serde_json::json!("é"), true), "\"é\"");
    assert_eq!(render(&serde_json::json!("🎉"), true), "\"🎉\"");
    assert_eq!(render(&serde_json::json!("a\u{7f}b"), true), "\"a\u{7f}b\"");
}

// Requirement: `to_string` of null is the JSON text `null`. What: the null
// and boolean renderings. Why: null is a value with a JSON text, not an
// absent string; an empty rendering would be a wrong value rather than an
// error, which is the worst failure mode.
//
// LLM section: null renders as `null` both at the top level and as a
// member, because its JSON text is the same in both positions.
#[test]
fn null_and_booleans_render_as_their_json_text() {
    assert_eq!(render(&serde_json::Value::Null, false), "null");
    assert_eq!(render(&serde_json::Value::Null, true), "null");
    assert_eq!(render(&serde_json::json!(true), false), "true");
    assert_eq!(render(&serde_json::json!(false), false), "false");
}

// Requirement: `to_string` of a number is its JSON number text. What: the
// number renderings, including the integral floats. Why: an integral
// float is written with its decimal point in JSON (`1.0`), and a
// rendering that drops it (`1`) is a different JSON value.
//
// LLM section: the rendering is `Debug`, not `Display` — `Display` drops
// the decimal point of an integral value and never uses exponent notation.
// A non-finite float has no JSON text at all (the format carries neither
// NaN nor the infinities), so it renders as `null`; the materialization
// mapping renders the same value as `0`, which is a known divergence.
// Magnitudes with a positive exponent are rendered without the reference's
// `+` (`1e300`, not `1e+300`) — an accepted divergence.
#[test]
fn numbers_render_as_their_json_text() {
    assert_eq!(render(&serde_json::json!(0), false), "0");
    assert_eq!(render(&serde_json::json!(-1), false), "-1");
    assert_eq!(render(&serde_json::json!(3), false), "3");
    assert_eq!(render(&serde_json::json!(1.2), false), "1.2");
    assert_eq!(render(&serde_json::json!(-1.5), false), "-1.5");
    assert_eq!(render(&serde_json::json!(1.0), false), "1.0");
    assert_eq!(render(&serde_json::json!(3.0), false), "3.0");
    assert_eq!(render(&serde_json::json!(-0.0), false), "-0.0");
    assert_eq!(render(&serde_json::json!(0.1 + 0.2), false), "0.30000000000000004");
    assert_eq!(render(&serde_json::json!(1e300), false), "1e300");
    assert_eq!(render(&serde_json::json!(1e-300), false), "1e-300");
}

// Requirement: `to_string` of an array or object is its JSON text. What:
// the container renderings — compact, recursively rendered, and correct
// when empty. Why: the container text must agree with what the
// materialization produces for the same value, so the members are
// rendered by the same rules and the separators carry no whitespace.
//
// LLM section: the members are rendered with `quote_string` true, so a
// string member is quoted. The separators are `,` and `:` with no spaces,
// matching the reference (`[0,1]`, `{"a":1}`).
#[test]
fn containers_render_compactly_and_recursively() {
    assert_eq!(render(&serde_json::json!([]), false), "[]");
    assert_eq!(render(&serde_json::json!({}), false), "{}");
    assert_eq!(render(&serde_json::json!([0, 1]), false), "[0,1]");
    assert_eq!(render(&serde_json::json!({"a": 1}), false), "{\"a\":1}");
    assert_eq!(render(&serde_json::json!(["a"]), false), "[\"a\"]");
    assert_eq!(
        render(&serde_json::json!([[1], {"a": [2]}]), false),
        "[[1],{\"a\":[2]}]"
    );
    assert_eq!(
        render(&serde_json::json!([null, true, "x"]), false),
        "[null,true,\"x\"]"
    );
}

// Requirement: an object's member names are strings in the JSON text, so
// they need the same escaping as a member value. What: keys carrying the
// characters that require escapes. Why: a key is interpolated into the
// JSON text, and an unescaped key produces invalid JSON exactly as an
// unescaped value does.
//
// LLM section: the key path and the value path share one escape function,
// so they cannot drift; this test pins the key path, which a value-only
// test would not reach.
#[test]
fn object_keys_are_escaped() {
    assert_eq!(render(&serde_json::json!({"a\"b": 1}), false), "{\"a\\\"b\":1}");
    assert_eq!(render(&serde_json::json!({"a\\b": 1}), false), "{\"a\\\\b\":1}");
    assert_eq!(render(&serde_json::json!({"a\nb": 1}), false), "{\"a\\nb\":1}");
    assert_eq!(render(&serde_json::json!({"a\u{0}b": 1}), false), "{\"a\\u0000b\":1}");
    assert_eq!(render(&serde_json::json!({"é": 1}), false), "{\"é\":1}");
}

// Requirement: a CBOR byte string has no JSON representation, so its
// rendering is a placeholder rather than an invented value. What: the
// byte-string rendering. Why: JMESPath defines no behaviour for a byte
// string (JSON has no such type), so the placeholder is a declared
// choice; the test exists so the choice is pinned rather than accidental.
//
// LLM section: the placeholder is the quoted text `"Binary_data"`, which
// is what the member and top-level positions both produce, since the arm
// ignores `quote_string`.
#[test]
fn a_byte_string_renders_as_the_placeholder() {
    let bytes = serde_cbor::Value::Bytes(vec![0xff, 0xfe, 0x00]);
    let value: SanshoCarrier<'_> = Rc::new(&bytes);
    assert_eq!(
        string_for(&value, false).expect("renders"),
        "\"Binary_data\""
    );
}

// Requirement: the escape function escapes exactly what JSON forbids and
// nothing else. What: `escape_string` directly, over the full range it
// distinguishes. Why: the function is shared by the key path and the value
// path, so a defect in it corrupts every container rendering; testing it
// directly names the unit when a container test fails.
//
// LLM section: the output carries NO surrounding quotes — the caller owns
// those, so that one function serves both a member value and a member key.
#[test]
fn escape_string_escapes_only_what_json_forbids() {
    assert_eq!(escape_string(""), "");
    assert_eq!(escape_string("plain"), "plain");
    assert_eq!(escape_string("\""), "\\\"");
    assert_eq!(escape_string("\\"), "\\\\");
    assert_eq!(escape_string("\u{8}"), "\\b");
    assert_eq!(escape_string("\u{c}"), "\\f");
    assert_eq!(escape_string("\n"), "\\n");
    assert_eq!(escape_string("\r"), "\\r");
    assert_eq!(escape_string("\t"), "\\t");
    assert_eq!(escape_string("\u{0}"), "\\u0000");
    assert_eq!(escape_string("\u{1}"), "\\u0001");
    assert_eq!(escape_string("\u{1f}"), "\\u001f");
    // the boundary: U+0020 is the first character above the range
    assert_eq!(escape_string("\u{20}"), " ");
    assert_eq!(escape_string("\u{7f}"), "\u{7f}");
    assert_eq!(escape_string("é"), "é");
    assert_eq!(escape_string("🎉"), "🎉");
}
