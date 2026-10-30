# SPEC-0001: Sansho — a JMESPath projection engine for CBOR data

Status: Accepted — wording agreed; implementation plan to follow.

## 1. Summary

Sansho is a stand-alone projection engine for single CBOR documents. It
parses a JMESPath expression, compiles it into a walk program, and evaluates
that program against a forward-only CBOR decoder over a byte slice,
materializing only the values the expression selects. Compiled programs are
cached and reused. Sansho implements the complete JMESPath language.

(Sansho: the Japanese pepper.)

## 2. Scope and interface

- Sansho evaluates **one document per evaluation**. Applying an expression
  across a collection of documents is the caller's concern.
- Inputs:
  1. a byte slice containing exactly one CBOR document (§4), and
  2. a JMESPath expression, either as text or as a previously compiled
     program.
- Output: a JSON value — the projection result.
- Sansho is a library with no I/O of its own: no network, no filesystem, no
  clock, no external state. Evaluation is deterministic: identical bytes and
  an identical expression yield identical results.
- Sansho is a stand-alone crate: it must not depend, directly or
  transitively, on any other BigTent crate or module. It is consumed
  one-way by its host application; extraction into its own project is a
  mechanical move. Its only dependencies are its own declared external
  crates: a JMESPath parser, a CBOR decoder, and development/test
  dependencies.

## 3. Language

- Sansho implements the **complete JMESPath language** as published at
  jmespath.org. The specification is pinned by date, and the official
  compliance corpus is pinned by commit and vendored with the project.
- Every language feature — selectors, indices, slices, wildcard and flatten
  projections, filters, pipes, multiselect, literals, comparators, the
  complete function library, and expression references — must behave
  exactly as the specification defines, including rejection of expressions
  the specification declares invalid.
- Null and missing-value semantics follow the specification exactly: a path
  that selects nothing evaluates to `null`, and projections drop `null`
  elements. There are no additional omission or defaulting rules.
- Deviations from the JMESPath specification are not permitted; changing
  the language means changing this specification.

## 4. Data model: CBOR viewed as JSON

Sansho evaluates over the JMESPath (JSON) data model while the document
itself is CBOR. The mapping is normative and follows RFC 8949 §4.2
(converting from CBOR to JSON):

| CBOR                              | JSON view                     |
|-----------------------------------|-------------------------------|
| map, definite length              | object                        |
| array, definite length            | array                         |
| text string                       | string                        |
| unsigned / negative integer, float| number                        |
| duplicate keys within one map     | last key wins                 |
| byte string                       | string (base64url-encoded)    |
| bignum (tags 2 and 3)             | number                        |
| false (simple value 20)           | `false`                       |
| true (simple value 21)            | `true`                        |
| null (simple value 22)            | `null`                        |
| undefined (simple value 23)       | evaluation error (no JSON equivalent) |
| any other tag                     | evaluation error              |
| any indefinite-length encoding    | rejected at input             |

Precision note: JMESPath defines numbers as IEEE-754 doubles. Integers
outside the exact double range may lose precision through this mapping;
that consequence is accepted and documented here.

## 5. Execution requirements

1. **Single pass.** An evaluation consumes each input byte at most once.
2. **Selective materialization.** Parts of the document the expression does
   not select must not be materialized; memory use is proportional to the
   output, not the input.
3. **Tree view.** The evaluator operates through a read-only tree-view
   abstraction; the abstraction, not any particular implementation, is the
   specified surface. Implementations include a cursor over the input byte
   slice (forward-only CBOR decoding) and a materialized in-memory view.
   Evaluation of the same program over the same document through any
   implementation of the abstraction must produce identical results.
   Strings may be borrowed from the input byte slice and are copied only
   when they appear in the output.
4. **Compiled-program cache.** Compiled programs are cached, keyed by the
   canonicalized expression text and the engine version. A cached program
   and a freshly compiled program must behave identically.
5. **Resource limits.** Parsed-expression size, nesting depth, evaluation
   instruction count, and output size must each be bounded; the bounds
   must be configurable.

## 6. Conformance and acceptance

A conforming Sansho build is green on the vendored JMESPath compliance
corpus — including the corpus's `invalid` cases — evaluated through every
shipped tree-view implementation with identical results. Verification
strategy beyond the corpus is the subject of the implementation plan.

## Appendix A — a representative item (truncated)

A real item as it appears on disk (CBOR) and over the wire (JSON). Elisions
are marked with `…`. Expressions from §2 illustrating this data:
`body.file_names[?starts_with(@, 'gitoid:')]` and `body.mime_type[0]`.

```json
{
  "identifier": "gitoid:blob:sha1:30e65ad24f4b4d799e52cfd70fcbebc0490b7343",
  "connections": [
    ["alias:from", "gitoid:blob:sha1:30e65ad24f4b4d799e52cfd70fcbebc0490b7343"],
    ["alias:from", "md5:4d921bf0ce238d1a96bdf65000fde565"],
    ["alias:from", "sha1:e9f38efe19210f3b63a72699a69eb261176d251e"],
    ["alias:from", "sha256:1fdd9c82d9ca39cc1e3fb2a49d52fa72eb12b52aecb2eaf65a059d237444394e"],
    ["alias:from", "sha512:e3da06bbc04c175638ea6368e774941faf97ff6ceb40ccb04…"],
    "… further connections elided …"
  ],
  "body_mime_type": "application/vnd.cc.goatrodeo",
  "body": {
    "extra": {},
    "file_names": [
      "com/groupbyinc/flux/common/apache/logging/log4j/core/lookup/JndiLookup.java",
      "gitoid:blob:sha256:005a5131bc1c950b2b4d1081a95bd21f52e45c308d9633465fbc240f8433c9e6!$org/apache/logging/log4j/core/lookup/JndiLookup.java",
      "gitoid:blob:sha256:007015fe7b249408bff2b406245d6213d211f27c1286e1ecaac05a63462186bc!$org/apache/logging/log4j/core/lookup/JndiLookup.java",
      "gitoid:blob:sha256:00c36799e22d2851b263d27b998e2d631a953fb40dce487ded120c37156a9373!$org/apache/logging/log4j/core/lookup/JndiLookup.java",
      "logging-log4j2-log4j-2.10.0/log4j-core/src/main/java/org/apache/logging/log4j/core/lookup/JndiLookup.java",
      "logging-log4j2-log4j-2.11.2/log4j-core/src/main/java/org/apache/logging/log4j/core/lookup/JndiLookup.java",
      "logging-log4j2-log4j-2.13.3/log4j-core/src/main/java/org/apache/logging/log4j/core/lookup/JndiLookup.java",
      "org/apache/logging/log4j/core/lookup/JndiLookup.java",
      "… thousands of further entries elided …"
    ],
    "file_size": 3050,
    "mime_type": ["text/x-java-source"]
  }
}
```

Note the `file_names` shape: plain source paths and merge-disambiguated
`gitoid:…!$path` entries mixed in one array. Prefix predicates over this
field are a primary use case; a single such item exceeds 190 KB when
serialized in full.
