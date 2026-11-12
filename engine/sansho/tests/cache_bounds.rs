//! The program cache at its boundaries.
//!
//! REQUIREMENT (second review, findings 7 and 8), decided by the owner:
//! * `put` is IDEMPOTENT — a second insert for a key already present keeps the
//!   first program, counts nothing, and never evicts. Before, two threads that
//!   missed on the same expression both inserted: the `order` list held the key
//!   twice (corrupting LRU order), and at capacity 1 the second insert evicted
//!   the first thread's just-cached program.
//! * `ProgramCache::new(0)` caches NOTHING — a zero bound must not hold one
//!   program, which is what the eviction loop's `None => break` allowed.
//!
//! THEORY OF THE TEST: each case pins the bound at its boundary, where a cache
//! stops being a cache — capacity 1, capacity 0 — plus a control that ordinary
//! eviction still happens, so the fix cannot be "never evict".

use sansho::cache::ProgramCache;

/// The entry count the cache reports.
fn entries(cache: &ProgramCache) -> usize {
    cache.stats().3
}

/// FINDING 7: a redundant insert must not evict or duplicate. At capacity 1 the
/// second `put` for the same expression used to evict the first program.
#[test]
fn a_duplicate_insert_keeps_the_first_program_and_evicts_nothing() {
    let cache = ProgramCache::new(1);
    let parsed = sansho::parse("foo").expect("parses");

    // compile_cached compiles and puts
    cache.compile_cached("foo").expect("compiles");
    assert_eq!(entries(&cache), 1, "one entry after the first compile");

    // a second compilation of the SAME expression inserts again
    cache.compile_cached("foo").expect("compiles");

    let (hits, misses, evictions, count) = cache.stats();
    assert_eq!(count, 1, "still one entry — the key was already present");
    assert_eq!(
        evictions, 0,
        "a duplicate insert must not evict (hits {hits}, misses {misses})"
    );
    // and the entry is still reachable
    let program = sansho::compile(&parsed, Some(&cache)).map(|_| ()).is_ok();
    assert!(program, "the cached program is still usable");
}

/// FINDING 8: a zero bound caches nothing. Before, `new(0)` held one program.
#[test]
fn a_zero_bound_caches_nothing() {
    let cache = ProgramCache::new(0);
    cache.compile_cached("foo").expect("compiles");
    assert_eq!(
        entries(&cache),
        0,
        "a zero bound must not hold a program: {:?}",
        cache.stats()
    );

    // and a second compile does not start holding one either
    cache.compile_cached("bar").expect("compiles");
    assert_eq!(entries(&cache), 0, "still nothing: {:?}", cache.stats());
}

/// THE CONTROL: at capacity 2, a third distinct expression still evicts the
/// least-recently-used one — the fix must not disable eviction.
#[test]
fn distinct_expressions_still_evict_the_least_recently_used() {
    let cache = ProgramCache::new(2);
    cache.compile_cached("a").expect("compiles");
    cache.compile_cached("b").expect("compiles");
    assert_eq!(entries(&cache), 2);

    // touch "a" so "b" is least-recently-used, then add "c"
    cache.compile_cached("a").expect("compiles");
    cache.compile_cached("c").expect("compiles");

    let (_, _, evictions, count) = cache.stats();
    assert_eq!(count, 2, "the bound holds");
    assert_eq!(evictions, 1, "exactly one eviction: {:?}", cache.stats());
}

/// FINDING 7, the concurrent case: two threads compiling the SAME expression
/// produce ONE entry and count one miss, not two.
#[test]
fn concurrent_compiles_of_one_expression_insert_once() {
    let cache = std::sync::Arc::new(ProgramCache::new(2));
    // a barrier so every thread reaches `compile_cached` together: the window
    // between `get` and `put` is otherwise too small to overlap by accident
    let start = std::sync::Arc::new(std::sync::Barrier::new(4));
    let mut handles = Vec::new();
    for _ in 0..4 {
        let cache = std::sync::Arc::clone(&cache);
        let start = std::sync::Arc::clone(&start);
        handles.push(std::thread::spawn(move || {
            start.wait();
            // the SAME expression text: the canonical key is the same
            cache.compile_cached("foo.bar").expect("compiles");
        }));
    }
    for handle in handles {
        handle.join().expect("the thread finishes");
    }
    let (_, misses, evictions, count) = cache.stats();
    assert_eq!(count, 1, "one entry for one expression: {:?}", cache.stats());
    assert_eq!(
        misses, 1,
        "one miss — the duplicate inserts count nothing: {:?}",
        cache.stats()
    );
    assert_eq!(evictions, 0, "nothing was evicted: {:?}", cache.stats());
}
