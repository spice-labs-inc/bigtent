//! Pattern matching and encoding.
//!
//! Starlark has no pattern matching of any kind — `str` offers `startswith`,
//! `endswith`, `find` and `split` on literals, and nothing else — so filtering by
//! identifier shape means chains of prefix tests. These are the three operations
//! that replace them, plus the encodings that move a value between its bytes and
//! its text form.

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
    fn matches(text: Value, pattern: Value) -> anyhow::Result<bool> {
        let regex = compile(as_string(pattern)?)?;
        Ok(regex.is_match(as_string(text)?))
    }

    /// Every match of `pattern` in `s`, in order.
    fn find_all(text: Value, pattern: Value) -> anyhow::Result<Vec<String>> {
        let regex = compile(as_string(pattern)?)?;
        Ok(regex
            .find_iter(as_string(text)?)
            .map(|found| found.as_str().to_string())
            .collect())
    }

    /// Replace every match of `pattern` in `s` with `replacement`.
    fn replace_re(text: Value, pattern: Value, replacement: Value) -> anyhow::Result<String> {
        let regex = compile(as_string(pattern)?)?;
        Ok(regex
            .replace_all(as_string(text)?, as_string(replacement)?)
            .into_owned())
    }

    /// The text's bytes in the url-safe base64 alphabet, as sansho renders a byte
    /// string.
    fn base64url_encode(text: Value) -> anyhow::Result<String> {
        Ok(sansho::cbor::base64url_encode(as_bytes(text)?))
    }

    /// The hex digits of the text's bytes.
    fn hex_encode(text: Value) -> anyhow::Result<String> {
        Ok(hex::encode(as_bytes(text)?))
    }

    /// The text a hex string carries.
    fn hex_decode(text: Value) -> anyhow::Result<String> {
        let bytes = hex::decode(as_string(text)?).map_err(|e| anyhow::anyhow!("not hex: {e}"))?;
        String::from_utf8(bytes).map_err(|e| anyhow::anyhow!("not UTF-8: {e}"))
    }
}

/// Compile a pattern, reporting a bad one as the caller's error rather than a
/// panic.
fn compile(pattern: &str) -> anyhow::Result<regex::Regex> {
    regex::Regex::new(pattern).map_err(|e| anyhow::anyhow!("bad pattern `{pattern}`: {e}"))
}
