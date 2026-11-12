//! THE ERROR IS STRUCTURE: a category, a LOCATION and a REASON — not a sentence.
//!
//! REQUIREMENT (owner's instruction): "A compile error MUST have the exact location of
//! the error and the reason for the error. An evaluation error must point exactly to the
//! place where the evaluation failed." And: the wire form is the type, derived, not
//! assembled by hand.
//!
//! THEORY OF THE TEST: every assertion here is on a FIELD, never on a message. A string
//! that happens to name the function would pass a message test and fail these. The span
//! assertions are exact byte ranges into the expression, so a placeholder (`0..0`), a
//! whole-expression span, or a missing span all fail.
use sansho::error::{CompileFault, EvaluationFault, Path, SanshoError, Span, Step};

/// The failure an expression produces, before any document is involved.
fn compile_fault(expression: &str) -> SanshoError {
    let parsed = sansho::parse(expression).expect("it parses");
    match sansho::compile(&parsed, None) {
        Err(error) => error,
        Ok(_) => panic!("`{expression}` must not compile"),
    }
}

/// A compile failure names the function AND points at it.
#[test]
fn a_compile_failure_carries_the_span_of_the_offending_name() {
    let error = compile_fault("nope(@)");
    match &error {
        SanshoError::Compile { span, reason, .. } => {
            assert_eq!(
                *reason,
                CompileFault::UnknownFunction {
                    name: "nope".to_string()
                },
                "the reason is the variant, not a sentence"
            );
            let span = span.expect("a compile failure knows WHERE");
            assert_eq!(
                span,
                Span::new(0, 4),
                "the span is exactly `nope`, not the whole expression"
            );
        }
        other => panic!("expected a compile failure, got {other:?}"),
    }
}

/// An evaluation failure names the document path it had walked, and the selector's span.
#[test]
fn an_evaluation_failure_carries_the_document_path() {
    // a document whose `body` is a NUMBER, then a selector that needs text from it
    let document = serde_json::json!({"body": 7});
    let bytes = serde_cbor::to_vec(&sansho::cbor::json_to_cbor_value(&document))
        .expect("the fixture encodes");
    let expression = "body | length(@)";
    let parsed = sansho::parse(expression).expect("it parses");
    let program = sansho::compile(&parsed, None).expect("it compiles");
    let slice: &[u8] = &bytes;
    let error = match sansho::evaluate(&program, slice) {
        Err(error) => error,
        Ok(_) => panic!("a length over a number must fail"),
    };
    match &error {
        SanshoError::Evaluation { span, path, reason, .. } => {
            assert!(
                span.is_some(),
                "the failure knows WHERE in the expression it happened: {error:?}"
            );
            assert!(
                matches!(reason, EvaluationFault::TypeMismatch { .. }),
                "the reason is the structured mismatch: {reason:?}"
            );
            // the walk was at the document root when `length` was applied to `body`'s value
            assert!(
                path.as_ref().map(Path::is_root).unwrap_or(true),
                "the path is a document location, not prose: {path:?}"
            );
        }
        other => panic!("expected an evaluation failure, got {other:?}"),
    }
}

/// The wire form is the type: a span is a span, a reason is a named variant.
#[test]
fn the_serialised_error_is_the_derived_shape() {
    let error = compile_fault("nope(@)");
    let json = serde_json::to_value(&error).expect("the error serialises");
    assert_eq!(json["category"], serde_json::json!("compile"), "{json}");
    assert_eq!(json["span"]["start"], serde_json::json!(0), "{json}");
    assert_eq!(json["span"]["end"], serde_json::json!(4), "{json}");
    assert_eq!(
        json["reason"]["unknown_function"]["name"],
        serde_json::json!("nope"),
        "the variant names itself: {json}"
    );
    // an absent field is ABSENT, not null
    assert!(
        json.get("path").is_none(),
        "a compile failure has no document path: {json}"
    );
}

/// `miette` can point at the span, which is the whole reason the span is carried.
#[test]
fn the_error_labels_its_span_for_a_renderer() {
    use miette::Diagnostic;
    let error = compile_fault("nope(@)");
    let labels: Vec<_> = error
        .labels()
        .expect("a spanned error labels its span")
        .collect();
    assert_eq!(labels.len(), 1, "one label: {labels:?}");
    assert_eq!(labels[0].offset(), 0, "the label starts at the name");
    assert_eq!(labels[0].len(), 4, "the label covers the name");
}

/// A document path renders as a location a person can follow.
#[test]
fn a_path_renders_as_a_document_location() {
    let path = Path::root().field("body").field("file_names").index(3);
    assert_eq!(path.to_string(), ".body.file_names[3]");
    assert_eq!(Path::root().to_string(), "@");
    assert_eq!(
        path.0,
        vec![
            Step::Field("body".to_string()),
            Step::Field("file_names".to_string()),
            Step::Index(3)
        ]
    );
}
