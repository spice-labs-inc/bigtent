# Sansho errors: location, path and reason

Every failure the engine produces is one of five categories, and **every category carries
where it happened and why**, as structure rather than as a sentence.

## The five categories

| Category | When | Location | Reason |
|---|---|---|---|
| `input` | the bytes are not a document | byte offset `at` | `InputFault` |
| `parse` | the expression is not JMESPath | `span` in the expression | `ParseFault` |
| `compile` | it parses but the specification forbids it | `span` in the expression | `CompileFault` |
| `evaluation` | the document defeats the expression | `span` **and** a document `path` | `EvaluationFault` |
| `limit` | a configured bound was exceeded | `span`, when known | `LimitFault` |

`category()` returns the lowercase name; it is what an error row's `kind` field carries.

## Two locations, not one

An evaluation failure needs both:

* **`span`** — the bytes of the expression that failed, taken from the selector the
  evaluator was applying. The parser computed it while parsing; the lowering kept it; the
  evaluator attached it at the single place a fault becomes an error.
* **`path`** — the steps into the *document* that reached the value, rendered like
  `body.file_names[3]`. `Path::root()` is the document itself.

A carrier answering through the trait surface has a value and no position, so it reports
`span: None` / `path: None` rather than inventing byte zero. `SanshoError::with_span` and
`with_path` let the layer that does know fill them in.

## Why the spans are real

`Program` nodes keep the postfix list as `Spanned<Postfix>`, and `Spine.edges` is a list of
`(Edge, Span)` pairs. The parser derives each span from the two ends it already holds —
`input` before the selector and the remainder after it — so nothing is reconstructed and no
combinator was restructured.

## Serialisation is the type

`SanshoError` derives `Serialize` with `#[serde(tag = "category")]`; optional fields
(`span`, `path`, `source`) are omitted when absent. An error row's `data` field **is** the
serialised error, so a field added to a fault appears in the wire form with no second place
to remember.

## Rendering is a library's

`SanshoError` implements `miette::Diagnostic`: `code()` is the category, `help()` carries a
hint where one exists, and `labels()` reports the span with its length. `Display` stays one
line for logs, and the error carries `source: Arc<str>` — the expression — so a renderer can
point at the span. Expressions are short; the text costs one allocation, shared by every
error from the same query.

## The rules this shape exists to enforce

1. **No prose in a reason.** A fault is an enum variant; `found` is the value's actual kind,
   not a sentence about it.
2. **No invented location.** A span that is not known is `None`, never `0..0`.
3. **No `format!` at a construction site.** Text is derived by `Display`; nothing builds it.
4. **No partial results.** A limit aborts the whole evaluation.
