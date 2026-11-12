# Sansho errors — LLM companion

## Shape

```rust
SanshoError::Input      { at: usize, reason: InputFault }
SanshoError::Parse      { span: Option<Span>, source: Option<Arc<str>>, reason: ParseFault }
SanshoError::Compile    { span: Option<Span>, source: Option<Arc<str>>, reason: CompileFault }
SanshoError::Evaluation { span: Option<Span>, path: Option<Path>, source: Option<Arc<str>>,
                          reason: EvaluationFault }
SanshoError::Limit      { span: Option<Span>, reason: LimitFault }
```

`Span { start, end }` is a byte range in the expression. `Path(Vec<Step>)` with
`Step::Field(String)` / `Step::Index(usize)` is the document location; `Display` renders
`body.file_names[3]`, and `Path::root()` is the document itself.

## Invariants — do not violate

* A reason is an ENUM. Never add a `String` detail field that restates a type check; the
  `TypeMismatch { expected, found, function }` shape exists for that, and `found` is the
  carrier's actual kind.
* A span that is not known is `None`. Never `Span::new(0, 0)`.
* Never `format!` at a construction site. `Display` derives text; `with_span` / `with_path`
  fill what the carrier could not know.
* `Serialize` is derived with `#[serde(tag = "category")]`; optional fields are
  `skip_serializing_if = "Option::is_none"`. An error row's `data` is
  `serde_json::to_value(&error)` — do not hand-build row data.
* `miette::Diagnostic` is implemented on `SanshoError` itself. `labels()` maps `Span` to
  `miette::SourceSpan` because the source text is carried as `Arc<str>`.

## Where the spans come from

Parser computes `Span::of(start, matched)` from `input`/`rest`; AST keeps
`Vec<Spanned<Postfix>>`; lowering produces `Spine.edges: Vec<(Edge, Span)>`; the evaluator
attaches the selector's span and the walked `Path` in `eval_spine`/`continue_*` when a
`Stop::Error` leaves. Adding a new edge means giving it a span in `compile_program` —
a missing span is a compile error, not a silent `None`.

## Test contract

Tests assert `category()`, the fault variant, and the span/path. A test that asserts a
MESSAGE is asserting the rendering, not the failure; only rendering tests may read text.
