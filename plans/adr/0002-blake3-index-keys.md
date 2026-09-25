# ADR 0002: BLAKE3-128 Index Keys

* Status: Proposed
* Deciders: project owner

## Human-readable record

### Context

Currently, the hardcoded identifier index keys are the 16-byte MD5 digest
of the identifier, chosen for speed and width, not security. A faster
modern hash at the same 16-byte width keeps the 32-byte index entry
(16 key + 8 data-file hash + 8 offset) and the existing comparison and
binary-search rules unchanged.

### Decision

Version 4 is the default for newly written clusters, and all version 4
clusters written by BigTent use truncated BLAKE3 for the index keys: the
first 16 bytes of the BLAKE3 digest over the UTF-8 bytes of the
identifier, compared as unsigned byte strings. The key algorithm used by a
cluster is specified in the cluster's `.grc` file, and readers use the
algorithm the file specifies. Version 3 clusters with MD5 keys remain
readable.

### Consequences

Positive: faster key computation at unchanged entry width and comparison
rules. Negative: a new `blake3` dependency. Neutral: data integrity remains
SHA256-based, and the index layout is unchanged apart from key derivation.

## LLM summary

BigTent's v4 writer uses truncated BLAKE3 index keys —
BLAKE3(identifier UTF-8)[0..16], compared as unsigned byte strings — and v4
is the default for new clusters. The index key algorithm is carried in the
`.grc` file and readers follow it; v3 stays readable with MD5.