//! A FAILED PURL READ IS NOT ALWAYS "NO PURLS.TXT".
//!
//! REQUIREMENT (error-swallowing sweep, item 3): both HTTP surfaces answered
//! `Err(_) => (404, "no purls.txt")` for EVERY failure. A permission error, a truncated
//! read and a missing file then look identical to a client, and a broken deployment hides
//! behind a routine 404.
//!
//! THEORY OF THE TEST: the mapping is asserted on the ERROR KIND — a `NotFound` io error is
//! a 404, and every other io error is a 500 that names the cause. `PermissionDenied` is the
//! discriminator: with the old arms it produced 404, so this test fails against them.
use bigtent::server::purl_failure;

/// A missing file is the only 404.
#[test]
fn a_missing_purl_file_is_a_404() {
    let error = anyhow::Error::new(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "no such file",
    ));
    let (status, body) = purl_failure(&error);
    assert_eq!(status, axum::http::StatusCode::NOT_FOUND);
    assert_eq!(body.0, "no purls.txt");
}

/// A read that failed for another reason is the SERVER's problem, and says so.
#[test]
fn a_purl_file_that_cannot_be_read_is_a_500_that_names_the_cause() {
    let error = anyhow::Error::new(std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "permission denied",
    ));
    let (status, body) = purl_failure(&error);
    assert_eq!(
        status,
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        "a permission failure is not an absence"
    );
    assert!(
        body.0.contains("permission denied"),
        "the body names the cause: {}",
        body.0
    );
    assert_ne!(body.0, "no purls.txt", "and does not call it absent");
}

/// A missing file WRAPPED in context is still a missing file.
#[test]
fn the_kind_is_found_through_the_context_chain() {
    let error = anyhow::Error::new(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "gone",
    ))
    .context("reading the merged purl list");
    let (status, _) = purl_failure(&error);
    assert_eq!(status, axum::http::StatusCode::NOT_FOUND);
}
