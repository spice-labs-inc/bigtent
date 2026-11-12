//! `as_array_str`: the lenient read of an array's text.
//!
//! The strict read is `as_elements` followed by `as_str`, which answers an
//! input error for an element it cannot read. `as_array_str` is for scanning:
//! it yields the text elements in document order and passes over everything
//! else — other kinds, and text that is not valid UTF-8.

use sansho::view::SanshoTrait;

/// A CBOR array of five elements: text, a byte string, a number, a text item
/// whose bytes are not valid UTF-8, and text again.
fn mixed_array() -> Vec<u8> {
    let mut bytes = vec![0x85_u8]; // array(5)
    bytes.extend_from_slice(&[0x63, b'a', b'b', b'c']); // "abc"
    bytes.extend_from_slice(&[0x43, 0xff, 0xfe, 0x00]); // byte string (major 2)
    bytes.extend_from_slice(&[0x01]); // the integer 1
    bytes.extend_from_slice(&[0x63, 0xff, 0xfe, 0x00]); // text(3), invalid UTF-8
    bytes.extend_from_slice(&[0x63, b'x', b'y', b'z']); // "xyz"
    bytes
}

// Requirement: scanning an array for strings must not stop at an element it
// cannot read. What: the byte source's `as_array_str` over an array that
// mixes every awkward kind. Why: a byte string is octets and never text; a
// number is not text; and a text item whose bytes are not UTF-8 is exactly
// the case the strict path reports as an input error — the lenient read has
// to pass over it and keep going, or a scan would silently truncate at the
// first damaged element.
//
// LLM section: the array is [text, byte string, number, invalid text, text].
// The expected yield is ["abc", "xyz"] — the two readable text elements, in
// document order, with the other three skipped.
#[test]
fn only_readable_text_elements_are_yielded() {
    let bytes = mixed_array();
    let slice: &[u8] = &bytes;
    let text: Vec<std::borrow::Cow<str>> = slice
        .as_array_str()
        .expect("an array answers")
        .collect();
    assert_eq!(text, vec!["abc", "xyz"]);
}

// Requirement: the read must work over both array encodings. What: an
// indefinite-length array, whose elements end at the break byte rather than
// at a count from the header. Why: the SPIKE's walk used the element count as
// its loop bound, which silently yields nothing for an indefinite array; this
// pins that the implemented walk does not depend on a count.
//
// LLM section: 0x9f opens an indefinite array, 0xff breaks it. The same two
// readable elements are expected, with the number in the middle skipped.
#[test]
fn an_indefinite_array_ends_at_its_break() {
    let mut bytes = vec![0x9f_u8]; // array(*)
    bytes.extend_from_slice(&[0x63, b'a', b'b', b'c']); // "abc"
    bytes.extend_from_slice(&[0x01]); // a number, skipped
    bytes.extend_from_slice(&[0x63, b'x', b'y', b'z']); // "xyz"
    bytes.push(0xff); // the break
    let slice: &[u8] = &bytes;
    let text: Vec<std::borrow::Cow<str>> = slice
        .as_array_str()
        .expect("an array answers")
        .collect();
    assert_eq!(text, vec!["abc", "xyz"]);
}

// Requirement: a non-array is not a silent empty result. What: `as_array_str`
// over a document that is not an array. Why: an empty iteration and "this is
// not an array" are different answers, and a caller scanning for a prefix must
// be able to tell an empty array from a document it cannot scan.
//
// LLM section: a single text item (not an array) answers an error; an empty
// array answers an empty iteration — the two cases are distinguished.
#[test]
fn a_non_array_answers_an_error_and_an_empty_array_does_not() {
    let text_item: Vec<u8> = vec![0x63, b'a', b'b', b'c'];
    let slice: &[u8] = &text_item;
    assert!(
        slice.as_array_str().is_err(),
        "a text item is not an array and must answer an error"
    );

    let empty: Vec<u8> = vec![0x80]; // array(0)
    let slice: &[u8] = &empty;
    let yielded: Vec<std::borrow::Cow<str>> = slice
        .as_array_str()
        .expect("an empty array is an array")
        .collect();
    assert!(
        yielded.is_empty(),
        "an empty array yields nothing and is not an error"
    );
}

// Requirement: every source that holds its own text answers this read, and the
// answer travels through a carrier. What: the same array read through the
// VALUE sources and through the carrier the evaluator hands back. Why:
// `SanshoCarrier` is its own implementation of the trait — it delegates — so a
// method that is not forwarded there falls back to the default, and the
// default is an error. A caller would then be unable to read an array it had
// just evaluated.
//
// LLM section: the document is read from `&serde_json::Value` directly and
// from the carrier that `evaluate` returns, and both must yield the text
// elements. The carrier path is the one that catches a missing forward.
#[test]
fn the_value_source_and_a_carrier_both_answer() {
    let document = serde_json::json!({"names": ["a", 3, "b", true]});

    assert!(
        document.as_array_str().is_err(),
        "an object is not an array and must answer an error"
    );

    let program = sansho::compile(&sansho::parse("names").unwrap(), None).unwrap();
    let carrier = sansho::evaluate(&program, &document).expect("evaluates");
    let text: Vec<std::borrow::Cow<str>> = carrier
        .as_array_str()
        .expect("the carrier forwards the read")
        .collect();
    assert_eq!(text, vec!["a", "b"]);
}

// Requirement: a computed array answers the read too. What: the projection
// result — a vector of carriers held by the engine — read with `as_array_str`.
// Why: that array holds carriers rather than document text, and the strings
// live in them; a source that cannot answer for its own storage would make the
// read unavailable for exactly the arrays an expression produces.
//
// LLM section: `names[*]` projects four elements of mixed kinds; the yield is
// the two text ones. The projection result is a vector of carriers, so this
// exercises the container implementation rather than a document source.
#[test]
fn a_computed_array_yields_its_own_text() {
    let document = serde_json::json!({"names": ["a", 3, "b", true]});
    let bytes = serde_cbor::to_vec(&sansho::cbor::json_to_cbor_value(&document)).unwrap();
    let program = sansho::compile(&sansho::parse("names[*]").unwrap(), None).unwrap();
    let carrier = sansho::evaluate(&program, bytes.as_slice()).expect("evaluates");
    let text: Vec<std::borrow::Cow<str>> = carrier
        .as_array_str()
        .expect("a computed array answers")
        .collect();
    assert_eq!(text, vec!["a", "b"]);
}

// ------------------------------------------------- a chunked element advances

/// A CHUNKED ELEMENT MUST ADVANCE THE READER.
///
/// REQUIREMENT (third review, finding 7): the array reader's chunked-text arm
/// did nothing — no `skip()`, no assembly — so the decoder never left the
/// chunked item's head. In an indefinite-length array that re-reads the same
/// head forever (a HANG on valid CBOR); in a definite-length array it spends the
/// element counter without moving, so every element after the chunked one is
/// silently dropped — the truncation this lenient read exists to avoid.
///
/// THEORY OF THE TEST: one array per encoding, each holding a chunked text
/// followed by the text "z". The lenient read cannot lend a `&str` for the
/// chunked item, so it passes over it — but it MUST pass over it, and the "z"
/// after it must still be yielded. The definite case is the truncation trap; the
/// indefinite case is the hang trap, which is why it runs on a worker thread
/// with a deadline instead of in the test thread.
///
/// LLM section: expected yield for both arrays is exactly ["z"]. A reader that
/// fails to advance yields [] for the definite array (the counter is consumed by
/// re-reading the head) and never returns for the indefinite array.
#[test]
fn a_chunked_element_does_not_swallow_what_follows_it() {
    // array(2): chunked text 'a'+'b' (0x7F … 0xFF), then "z"
    let definite = vec![
        0x82, 0x7F, 0x61, b'a', 0x61, b'b', 0xFF, 0x61, b'z',
    ];
    let slice: &[u8] = &definite;
    let yielded: Vec<std::borrow::Cow<str>> = slice
        .as_array_str()
        .expect("an array answers")
        .collect();
    assert_eq!(
        yielded,
        vec!["z"],
        "the element after the chunked text is read"
    );

    // the same array, indefinite: 0x9F … 0xFF
    let indefinite = vec![
        0x9F, 0x7F, 0x61, b'a', 0x61, b'b', 0xFF, 0x61, b'z', 0xFF,
    ];
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let yielded: Vec<String> = (indefinite.as_slice())
            .as_array_str()
            .expect("an array answers")
            .map(|text| text.into_owned())
            .collect();
        let _ = sender.send(yielded);
    });
    let yielded = receiver
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("the indefinite read terminates instead of re-reading the chunked head");
    assert_eq!(yielded, vec!["z".to_string()]);
}
