//! Pattern matching and encoding.
//!
//! Starlark has no pattern matching of any kind — `str` offers `startswith`,
//! `endswith`, `find` and `split` on literals, and nothing else — so filtering by
//! identifier shape means chains of prefix tests. These are the three operations
//! that replace them, plus the encodings that move a value between its bytes and
//! its text form.

use crate::barker::error::BarkerError;
use crate::barker::values::{as_bytes, as_string};
use starlark::environment::GlobalsBuilder;
use starlark::starlark_module;
use starlark::values::Value;

#[starlark_module]
pub fn patterns(builder: &mut GlobalsBuilder) {
    /// Whether `s` matches `pattern` anywhere in it.
    ///
    /// The pattern is a Rust `regex` expression, compiled per call: a script
    /// that matches in a loop should hoist the compiled form, but there is no
    /// way to hold a compiled pattern here without a value type for it.
    fn matches(text: Value, pattern: Value) -> starlark::Result<bool> {
        let regex = compile("matches", as_string("matches", pattern)?)?;
        Ok(regex.is_match(as_string("matches", text)?))
    }

    /// Every match of `pattern` in `s`, in order.
    fn find_all(text: Value, pattern: Value) -> starlark::Result<Vec<String>> {
        let regex = compile("find_all", as_string("find_all", pattern)?)?;
        Ok(regex
            .find_iter(as_string("find_all", text)?)
            .map(|found| found.as_str().to_string())
            .collect())
    }

    /// Replace every match of `pattern` in `s` with `replacement`.
    fn replace_re(text: Value, pattern: Value, replacement: Value) -> starlark::Result<String> {
        let regex = compile("replace_re", as_string("replace_re", pattern)?)?;
        Ok(regex
            .replace_all(as_string("replace_re", text)?, as_string("replace_re", replacement)?)
            .into_owned())
    }

    /// The text's bytes in the url-safe base64 alphabet, as sansho renders a byte
    /// string.
    fn base64url_encode(text: Value) -> starlark::Result<String> {
        Ok(sansho::cbor::base64url_encode(as_bytes("base64url_encode", text)?))
    }

    /// The hex digits of the text's bytes.
    fn hex_encode(text: Value) -> starlark::Result<String> {
        Ok(hex::encode(as_bytes("hex_encode", text)?))
    }

    /// The text a hex string carries.
    fn hex_decode(text: Value) -> starlark::Result<String> {
        let bytes = hex::decode(as_string("hex_decode", text)?).map_err(|error| BarkerError::Value {
            function: "hex_decode",
            detail: format!("not hexadecimal: {error}"),
        })?;
        String::from_utf8(bytes)
            .map_err(|error| {
                BarkerError::Value {
                    function: "hex_decode",
                    detail: format!("the bytes are not UTF-8: {error}"),
                }
                .into()
            })
    }
}

/// Compile a pattern, reporting a bad one as the caller's error rather than a
/// panic — and as the PATTERN kind, so a caller can tell "your regex is bad"
/// from "your data is bad".
fn compile(function: &'static str, pattern: &str) -> Result<regex::Regex, BarkerError> {
    regex::Regex::new(pattern).map_err(|error| BarkerError::Pattern {
        function,
        pattern: pattern.to_string(),
        reason: error.to_string(),
    })
}
