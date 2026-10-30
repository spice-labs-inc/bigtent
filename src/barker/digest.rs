//! Digests, and the two identifier forms built on them.
//!
//! Every hash here comes from [`coordinates`](https://github.com/spice-labs-inc/coordinates)
//! — the canonical definition of the identifiers the ecosystem agrees on — so
//! this module is naming and framing, not hashing.
//!
//! Three levels, and the difference between them is the point:
//!
//! - `hash.*` is the **bare digest** of the text's bytes. `sha256("abc")` is the
//!   SHA-256 of those three bytes.
//! - `gitoid.*` is a **git object id**: the digest of `"blob <len>\0"` followed by
//!   the content, in the corpus's form `gitoid:blob:sha256:<hex>`.
//! - `swh.*` is the **Software Heritage content identifier**: `swh:1:cnt:<hex>`,
//!   where the hash is the *git blob* digest — the same digest as the gitoid, under
//!   a different prefix.
//!
//! So a SWHID for content and the gitoid of that content share their hash exactly,
//! and differ only in the prefix: `gitoid:blob:sha1:ce013625…` and
//! `swh:1:cnt:ce013625…` are the same content. That is not obvious — SWH calls the
//! quantity `sha1_git`, which reads like "the git-flavoured sha1" rather than "the
//! git blob id" — and getting it wrong yields an identifier that resolves to
//! nothing.

use crate::barker::values::as_bytes;
use starlark::environment::GlobalsBuilder;
use starlark::starlark_module;
use starlark::values::Value;

/// The `swh:1:cnt:` prefix that wraps a content's git blob digest.
fn swh_of(gitoid: &str) -> String {
    // a gitoid is `gitoid:blob:<algo>:<hex>`; a SWHID is the same hex with its own
    // prefix, so the hex is taken from the last colon onwards
    let hex = gitoid.rsplit(':').next().unwrap_or_default();
    format!("swh:1:cnt:{hex}")
}

#[starlark_module]
pub fn digest(builder: &mut GlobalsBuilder) {
    /// The SHA-1 digest of the text's bytes, as hex — no framing.
    fn sha1(text: Value) -> anyhow::Result<String> {
        Ok(coordinates::sha1(as_bytes("sha1", text)?))
    }

    /// The SHA-256 digest of the text's bytes, as hex — no framing.
    fn sha256(text: Value) -> anyhow::Result<String> {
        Ok(coordinates::sha256(as_bytes("sha256", text)?))
    }

    /// The SHA-512 digest of the text's bytes, as hex.
    fn sha512(text: Value) -> anyhow::Result<String> {
        Ok(coordinates::sha512(as_bytes("sha512", text)?))
    }

    /// The MD5 digest of the text's bytes, as hex.
    fn md5(text: Value) -> anyhow::Result<String> {
        Ok(coordinates::md5(as_bytes("md5", text)?))
    }

    /// The BLAKE3 digest of the text's bytes, as hex.
    ///
    /// BLAKE3 is not among the identifiers the `coordinates` crate defines, so
    /// this is the one digest computed here rather than there.
    fn blake3(text: Value) -> anyhow::Result<String> {
        Ok(::blake3::hash(as_bytes("blake3", text)?).to_hex().to_string())
    }

    /// The git blob id with SHA-1, in the corpus's form:
    /// `gitoid:blob:sha1:<40 hex>`.
    fn gitoid_sha1(text: Value) -> anyhow::Result<String> {
        Ok(coordinates::gitoid_blob_sha1(as_bytes("gitoid_sha1", text)?))
    }

    /// The git blob id with SHA-256, in the corpus's form:
    /// `gitoid:blob:sha256:<64 hex>`.
    fn gitoid_sha256(text: Value) -> anyhow::Result<String> {
        Ok(coordinates::gitoid_blob_sha256(as_bytes("gitoid_sha256", text)?))
    }

    /// The Software Heritage content identifier with SHA-1:
    /// `swh:1:cnt:<40 hex>`.
    ///
    /// The hash is the GIT BLOB digest — the same one the gitoid above carries —
    /// so the two identifiers describe the same content and differ only in their
    /// prefix.
    fn swh_sha1(text: Value) -> anyhow::Result<String> {
        Ok(swh_of(&coordinates::gitoid_blob_sha1(as_bytes("swh_sha1", text)?)))
    }

    /// The Software Heritage content identifier with SHA-256:
    /// `swh:1:cnt:<64 hex>`, over the git blob digest as above.
    fn swh_sha256(text: Value) -> anyhow::Result<String> {
        Ok(swh_of(&coordinates::gitoid_blob_sha256(as_bytes("swh_sha256", text)?)))
    }
}
