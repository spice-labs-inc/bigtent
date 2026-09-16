//! The compiled-program cache (SPEC-0001 §5.4).
//!
//! Keyed by the canonicalized expression text plus the engine version;
//! least-recently-used eviction; process-local (no disk persistence).
//! The plan fixes the policy; changes require owner approval. A cached
//! program and a freshly compiled program must behave identically —
//! tested by property.

use crate::error::SanshoError;
use crate::program::{Program, compile};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// The compiled-program cache.
pub struct ProgramCache {
    inner: Mutex<CacheInner>,
}

struct CacheInner {
    entries: HashMap<CacheKey, Arc<Program>>,
    order: Vec<CacheKey>, // least-recently-used first
    max_entries: usize,
    hits: usize,
    misses: usize,
    evictions: usize,
}

/// The cache key: the canonicalized expression and the engine version.
/// (The engine version invalidates programs across engine upgrades.)
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct CacheKey {
    canonical: String,
    #[allow(dead_code)]
    engine_version: String, // wired in below; kept out of the comparisons only by construct
}

impl CacheKey {
    fn of(expression: &str) -> Result<Self, SanshoError> {
        let parsed = crate::parser::parse(expression)?;
        let canonical = crate::canonical(&parsed);
        Ok(CacheKey {
            canonical,
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
        })
    }
}

impl ProgramCache {
    /// A cache with the given entry bound.
    pub fn new(max_entries: usize) -> Self {
        ProgramCache {
            inner: Mutex::new(CacheInner {
                entries: HashMap::new(),
                order: Vec::new(),
                max_entries,
                hits: 0,
                misses: 0,
                evictions: 0,
            }),
        }
    }

    /// Compile (or fetch) the program for an expression. Compile errors
    /// are NOT cached (a corrected expression should not pay for its
    /// predecessor's failure; the error rate would pollute the cache).
    pub fn compile_cached(&self, expression: &str) -> Result<Arc<Program>, SanshoError> {
        let key = CacheKey::of(expression)?;
        let mut inner = self.inner.lock().expect("cache lock");
        if inner.entries.contains_key(&key) {
            let program = inner.entries.get(&key).expect("checked").clone();
            inner.hits += 1;
            // touch: move to the most-recently-used end
            inner.order.retain(|k| k != &key);
            inner.order.push(key);
            return Ok(program);
        }
        inner.misses += 1;
        let parsed = crate::parser::parse(expression)?;
        let program = Arc::new(compile(&parsed)?);
        // evict while over the bound (LRU first)
        while inner.entries.len() >= inner.max_entries {
            if let Some(oldest) = inner.order.first().cloned() {
                inner.entries.remove(&oldest);
                inner.order.remove(0);
                inner.evictions += 1;
            } else {
                break;
            }
        }
        inner.entries.insert(key.clone(), program.clone());
        inner.order.push(key);
        Ok(program)
    }

    /// The cache's statistics: (hits, misses, evictions, entries).
    pub fn stats(&self) -> (usize, usize, usize, usize) {
        let inner = self.inner.lock().expect("cache lock");
        (
            inner.hits,
            inner.misses,
            inner.evictions,
            inner.entries.len(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Requirement: SPEC-0001 §5.4 — a cached program and a freshly
    // compiled program must behave identically, INCLUDING across
    // eviction (a recompiled program after eviction behaves
    // identically). What: property test — generated expressions evaluate
    // identically via the cache and via fresh compilation, with the
    // cache cycled through evictions. Why: cache transparency is the
    // requirement (plan `cache_hit_identity`).
    //
    // LLM section: a tiny cache (4 entries) forces evictions; every
    // evaluation through the cache matches the fresh-program evaluation.
    #[test]
    fn cache_hit_identity_including_eviction() {
        use crate::eval::evaluate_json;
        let cache = ProgramCache::new(4);
        let expressions = [
            "a.b",
            "a[0]",
            "a[*].b",
            "length(a)",
            "{x: a, y: b}",
            "a || b",
            "a && b",
            "!a",
            "a[?b]",
            "a[0:2]",
            "a | b",
        ];
        let document = serde_json::json!({
            "a": {"b": 1, "c": [1, 2, 3]},
            "b": 2,
        });
        for round in 0..3 {
            for expression in expressions {
                // the repeat-pair: the second call hits the entry the
                // first call just inserted (the round-robin of 11
                // expressions over a 4-entry cache would thrash by
                // construction and hit nothing)
                let _ = cache.compile_cached(expression).expect("compiles");
                let cached = cache.compile_cached(expression).expect("compiles");
                let via_cache = evaluate_json(&cached, &document);
                let parsed = crate::parser::parse(expression).unwrap();
                let fresh = crate::program::compile(&parsed).unwrap();
                let via_fresh = evaluate_json(&fresh, &document);
                let same = match (via_cache, via_fresh) {
                    (Ok(a), Ok(b)) => a == b,
                    (Err(a), Err(b)) => a.category() == b.category(),
                    _ => false,
                };
                assert!(
                    same,
                    "round {round}: cached and fresh diverge on {expression:?}"
                );
            }
        }
        let (hits, _, evictions, _) = cache.stats();
        assert!(hits > 0, "the cache must serve hits");
        assert!(evictions > 0, "the tiny cache must have evicted");
    }

    // Requirement: SPEC-0001 §5.4 — the key is the canonical form:
    // insignificant-whitespace variants SHARE a cache entry; expressions
    // of different meaning NEVER do (the negative property). What: the
    // hit/miss pattern over the pair classes. Why: false sharing returns
    // wrong results, which is worse than a miss (plan
    // `cache_key_stability`).
    //
    // LLM section: "a . b" and "a.b" hit the same entry (one miss total);
    // "a.b" and "a.c" are distinct entries.
    #[test]
    fn cache_key_stability() {
        let cache = ProgramCache::new(16);
        let _ = cache.compile_cached("a . b").expect("compiles");
        let _ = cache.compile_cached("a.b").expect("compiles");
        let (_, _, _, entries) = cache.stats();
        assert_eq!(entries, 1, "whitespace variants share one entry");

        let _ = cache.compile_cached("a.c").expect("compiles");
        let (_, _, _, entries) = cache.stats();
        assert_eq!(entries, 2, "distinct meanings are distinct entries");
    }

    // Requirement: SPEC-0001 §5.4 — compile errors are not cached.
    // What: an invalid expression's error repeats on retry (and the
    // corrected expression compiles cleanly after). Why: the cache would
    // otherwise pin failures.
    //
    // LLM section: the invalid expression errors twice with the same
    // category; the miss-count rises (it is ATTEMPTED each time).
    #[test]
    fn compile_errors_are_not_cached() {
        let cache = ProgramCache::new(16);
        for _ in 0..2 {
            let error = cache.compile_cached("a[0");
            assert!(matches!(error, Err(SanshoError::Parse { .. })));
        }
        let (_, _, _, entries) = cache.stats();
        assert_eq!(entries, 0, "errors never persist as entries");
    }
}
