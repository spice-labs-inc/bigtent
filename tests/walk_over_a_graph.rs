//! A walk over a real graph, from three sources.
//!
//! WHAT THIS TESTS
//!
//! The topology the query system exists for, at two hops through two connection
//! types:
//!
//! ```text
//! pkg:maven/... --alias:to--> jar --contained:down--> class
//! ```
//!
//! The same walk runs three times, against three sources of the SAME graph shape:
//!
//! 1. a **V4 cluster on disk** — `test_data/walk_v4`, written by the repository's
//!    own writer, so the encoder, the index and the decoder are exercised too;
//! 2. a **`RoboticGoat` in memory** — built at test time from
//!    `test_data/walk_v4/in_memory_items.json`, whose VALUES DIFFER from the disk
//!    cluster's, so a test that accidentally reads the wrong source fails rather
//!    than passing by coincidence;
//! 3. a **`GoatHerd`** over both, which is what the multi-source path is.
//!
//! WHY IT IS WRITTEN THIS WAY
//!
//! Every other highwire test uses identifiers that already exist in the harness,
//! so none of them traverses an edge: connections, connection gates and depth are
//! wired but never exercised. That is the gap this file closes, and the reason a
//! broken connection edge could reach the suite unnoticed.
//!
//! The three sources are a deliberate differential. A cluster and a herd answer
//! through different code — `identifier_to_item_offset` and `bytes_for_file_and_offset`
//! against `item_for_identifier` — so the two must agree, and the herd must see
//! both.
//!
//! LLM-FRIENDLY SUMMARY
//!
//! - Requirement: a walk traverses real edges, and does so identically over a
//!   cluster, an in-memory member, and a herd of both.
//! - The on-disk graph: 3 purls (one a deb with no body), 3 jars (one nested two
//!   levels deep), 3 classes, one of which is in two jars.
//! - The in-memory graph: 2 purls, 2 jars, 2 classes, one shared — DIFFERENT
//!   identifiers, same shape.
//! - Tests: the purl→jar hop; the jar→class hop; the arity of `contained:up` for a
//!   shared class; that the herd sees both sources; and that the two sources'
//!   outputs are not interchangeable.

use anyhow::Result;
use bigtent::highwire::grammar::Query;
use bigtent::highwire::walk::{AccountingAndBudget, HerdOrCluster, SinkOperation};
use bigtent::item::Item;
use bigtent::rodeo::goat::GoatRodeoCluster;
use bigtent::rodeo::goat_trait::GoatRodeoTrait;
use bigtent::rodeo::goat_herd::GoatHerd;
use bigtent::rodeo::member::member_synth;
use bigtent::rodeo::robo_goat::RoboticGoat;
use bigtent::highwire::compiler::compile;
use sansho::ProgramCache;
use serde_json::Value;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

/// The accountant these tests use: it never stops a walk.
struct NoBound;

impl AccountingAndBudget for NoBound {
    fn should_stop(&self) -> Option<Vec<Value>> {
        None
    }

    fn can_sink_normal(&self) -> bool {
        true
    }

    fn event(&self, _event_type: bigtent::highwire::walk::AccountingEventType) {}
}

/// The directory holding the on-disk cluster and the in-memory description.
fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/walk_v4")
}

/// The on-disk V4 cluster.
async fn disk_cluster() -> HerdOrCluster {
    let clusters = GoatRodeoCluster::cluster_files_in_dir(fixture_dir(), false, vec![])
        .await
        .expect("the fixture cluster loads");
    assert_eq!(clusters.len(), 1, "the fixture is one cluster");
    HerdOrCluster::Cluster(clusters.into_iter().next().expect("one cluster"))
}

/// The in-memory member, from the description whose values differ from the disk
/// cluster's.
fn memory_member() -> Arc<bigtent::rodeo::member::HerdMember> {
    let text = std::fs::read_to_string(fixture_dir().join("in_memory_items.json"))
        .expect("the description is present");
    let described: Value = serde_json::from_str(&text).expect("the description is JSON");
    let items: Vec<Item> = serde_json::from_value(described["items"].clone())
        .expect("the description holds items");
    member_synth(RoboticGoat::new("in_memory", items, serde_json::json!({})))
}

/// The in-memory source on its own.
fn memory_cluster() -> HerdOrCluster {
    HerdOrCluster::Herd(Arc::new(GoatHerd::new(vec![memory_member()])))
}

/// Both sources in one herd: the loaded cluster AND the in-memory member.
///
/// A herd holds members, and a loaded cluster IS one — `HerdMember::Cluster` — so
/// the herd answers from the on-disk graph through the cluster's own code and from
/// the in-memory graph through `RoboticGoat`'s.
async fn herd_of_both() -> HerdOrCluster {
    let clusters = GoatRodeoCluster::cluster_files_in_dir(fixture_dir(), false, vec![])
        .await
        .expect("the fixture cluster loads");
    let cluster = clusters.into_iter().next().expect("one cluster");
    HerdOrCluster::Herd(Arc::new(GoatHerd::new(vec![
        Arc::new(bigtent::rodeo::member::HerdMember::Cluster(cluster)),
        memory_member(),
    ])))
}

/// Run a query and return its rows.
/// A sink that owns its rows, so they outlive the compiled run's destructor.
#[derive(Default)]
struct RowSink(Vec<Value>);

impl RowSink {
    fn sink(&mut self) -> impl FnMut(Value, &Arc<NoBound>, SinkOperation) -> Result<()> + '_ {
        move |row: Value, _acc: &Arc<NoBound>, _op: SinkOperation| {
            self.0.push(row);
            Ok(())
        }
    }
}

fn rows(document: Value, lookup: HerdOrCluster) -> Result<Vec<Value>> {
    let query: Rc<dyn Query> = Rc::new(document);
    let cache = ProgramCache::new(512);
    let run = compile(query, &cache)?;
    // the run and the sink that borrows `out` both live in this block, so both
    // are gone before `out` is handed back
    let mut out = Vec::new();
    {
        let mut run = run;
        let sink = |row: Value, _acc: &Arc<NoBound>, _op: SinkOperation| -> Result<()> {
            out.push(row);
            Ok(())
        };
        let (sender, input) = flume::unbounded::<Value>();
        drop(sender); // these queries name their own roots
        run(Arc::new(NoBound), sink, lookup, input)?;
    }
    Ok(out)
}

/// The two hops, in one query: purls to their jars, then those jars to the classes
/// they contain, counting how many jars each class is in.
fn two_hops(purls: &[&str]) -> Value {
    serde_json::json!({
        "phases": [
            {"name": "jars",
             "roots": purls,
             "walk": ["alias:to"],
             "emit": {"expr": "{id: identifier}",
                      "if": "contains(body.mime_type, 'application/java-archive')"}},
            {"name": "classes",
             "roots": {"jars": "id"},
             "walk": ["contained:down"],
             "emit": {"expr": "{id: identifier, jars: length(connections.\"contained:up\")}",
                      "if": "contains(body.mime_type, 'application/java-vm')"}}
        ]
    })
}

/// Requirement: a walk traverses a real `alias:to` edge, then a real
/// `contained:down` edge, over a V4 cluster read from disk.
///
/// What: two maven purls as roots; the first hop reaches the jar they both alias;
/// the second reaches that jar's classes and reports how many jars each is in.
///
/// Why: every other highwire test uses items with no connections, so no test has
/// ever asked the engine to follow an edge. If the connection wiring were wrong —
/// the wrong map, the wrong direction, a gate that rejects everything — the walk
/// would stop after the first hop and every existing test would still pass.
///
/// LLM section: the fixture's two maven purls both alias `JAR_A`, whose
/// `contained:down` names a shared class (in two jars), a class in only that jar,
/// and a nested jar. The first hop therefore yields one jar, not two, because the
/// engine visits an identifier once.
#[tokio::test]
async fn the_two_hop_walk_traverses_real_edges() -> Result<()> {
    let purls = ["pkg:maven/uk.co.real-logic/artio-core@0.118",
                 "pkg:maven/uk.co.real-logic/artio-codecs@0.118"];

    let rows = rows(two_hops(&purls), disk_cluster().await)?;
    assert!(!rows.is_empty(), "the walk reached something: {rows:?}");

    // Only the last stage is teed, so the rows the caller sees are the CLASSES:
    // the first hop's jar rows went downstream and not to the output. That both
    // purls alias one jar is therefore proved by the second hop reaching it once —
    // a class beneath it appears once, not twice.
    let mut arities: Vec<(String, i64)> = rows
        .iter()
        .filter_map(|row| {
            Some((
                row.get("id")?.as_str()?.to_string(),
                row.get("jars")?.as_i64()?,
            ))
        })
        .collect();
    arities.sort();
    assert_eq!(
        arities.len(),
        3,
        "the fixture's three classes, each reached once: {rows:?}"
    );
    assert_eq!(
        arities.iter().filter(|(_, jars)| *jars == 2).count(),
        1,
        "the class in two jars has arity 2: {rows:?}"
    );
    assert_eq!(
        arities.iter().filter(|(_, jars)| *jars == 1).count(),
        2,
        "the two classes in one jar have arity 1, including the nested jar's: {rows:?}"
    );
    Ok(())
}

/// Requirement: the same walk over an in-memory member reaches the same shape.
///
/// What: the two-hop walk over `RoboticGoat` members built at test time from the
/// JSON description, whose identifiers are DIFFERENT from the disk cluster's.
///
/// Why: a cluster and an in-memory member answer through different code —
/// `identifier_to_item_offset` plus `bytes_for_file_and_offset` against
/// `item_for_identifier` — so a bug in one is invisible from the other. And the
/// values differ on purpose: a test that read the wrong source would report the
/// other source's identifiers and fail here rather than passing by coincidence.
///
/// LLM section: the in-memory graph has two purls aliasing one jar, which contains
/// a shared class and a class in only that jar — the same shape, different names.
#[tokio::test]
async fn the_same_walk_reaches_an_in_memory_member() -> Result<()> {
    let purls = ["pkg:maven/org.example/alpha@1.0", "pkg:maven/org.example/beta@2.0"];

    let rows = rows(two_hops(&purls), memory_cluster())?;
    assert!(!rows.is_empty(), "the walk reached something: {rows:?}");

    let identifiers: Vec<&str> = rows.iter().filter_map(|row| row.get("id")?.as_str()).collect();
    assert!(
        identifiers.contains(&"gitoid:blob:sha256:cccc"),
        "a class of the description's jar: {rows:?}"
    );
    assert!(
        identifiers.contains(&"gitoid:blob:sha256:dddd"),
        "and the other: {rows:?}"
    );
    assert!(
        !identifiers.iter().any(|id| id.contains("artio")),
        "nothing from the disk cluster leaked in: {rows:?}"
    );

    let shared = rows
        .iter()
        .find(|row| row.get("id").and_then(|id| id.as_str()) == Some("gitoid:blob:sha256:cccc"))
        .expect("the shared class is present");
    assert_eq!(shared["jars"], serde_json::json!(2), "it is in two jars: {shared:?}");
    Ok(())
}

/// Requirement: a herd answers from every source it holds.
///
/// What: the two-hop walk from both graphs' purls at once, against a herd holding
/// the in-memory member and a second member.
///
/// Why: the herd is the multi-source path, and it answers through a third code
/// path — one member's `item_for_identifier` after another — so agreement between
/// cluster and member does not imply agreement through a herd.
///
/// LLM section: the herd holds the in-memory graph, so the purls named here are
/// reachable and both of its classes come back.
#[tokio::test]
async fn a_herd_answers_from_its_members() -> Result<()> {
    let purls = ["pkg:maven/org.example/alpha@1.0"];

    let rows = rows(two_hops(&purls), herd_of_both().await)?;
    let identifiers: Vec<&str> = rows.iter().filter_map(|row| row.get("id")?.as_str()).collect();
    assert!(
        identifiers.contains(&"gitoid:blob:sha256:cccc"),
        "the herd reached the in-memory member's class: {rows:?}"
    );
    Ok(())
}

/// Requirement: the two sources are not interchangeable, so a test cannot pass by
/// reading the wrong one.
///
/// What: the disk cluster's purls asked of the in-memory member, and the
/// in-memory purls asked of the disk cluster.
///
/// Why: this is what makes the differential meaningful. If both sources contained
/// the same identifiers, the three tests above would agree while proving nothing
/// about which source was read.
///
/// LLM section: neither source knows the other's purls, so both queries emit
/// nothing — which is an absence, and that is exactly the point.
#[tokio::test]
async fn the_two_sources_do_not_contain_each_others_identifiers() -> Result<()> {
    let disk_purls = ["pkg:maven/uk.co.real-logic/artio-core@0.118"];
    let memory_purls = ["pkg:maven/org.example/alpha@1.0"];

    let disk_purls_in_memory = rows(two_hops(&disk_purls), memory_cluster())?;
    assert!(
        disk_purls_in_memory.is_empty(),
        "the in-memory source does not hold the disk cluster's purls: {disk_purls_in_memory:?}"
    );

    let memory_purls_on_disk = rows(two_hops(&memory_purls), disk_cluster().await)?;
    assert!(
        memory_purls_on_disk.is_empty(),
        "the disk cluster does not hold the in-memory purls: {memory_purls_on_disk:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------- nesting

/// Every source a nested query can be given.
///
/// The three are not variations on one thing: a cluster answers through
/// `identifier_to_item_offset` and `bytes_for_file_and_offset`, a `RoboticGoat`
/// member through `item_for_identifier`, and a herd through one member after
/// another. A nested query reaches them from a script's own thread, which no other
/// test does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Source {
    /// The V4 cluster on disk.
    Disk,
    /// The in-memory member built from the JSON description.
    Memory,
    /// A herd holding both.
    Both,
}

impl Source {
    fn name(self) -> &'static str {
        match self {
            Source::Disk => "disk",
            Source::Memory => "memory",
            Source::Both => "both",
        }
    }

    /// The `HerdOrCluster` for this source.
    async fn lookup(self) -> HerdOrCluster {
        match self {
            Source::Disk => disk_cluster().await,
            Source::Memory => memory_cluster(),
            Source::Both => herd_of_both().await,
        }
    }

    /// The purls this source holds.
    fn purls(self) -> [&'static str; 2] {
        match self {
            // the disk cluster's two maven purls, and the memory graph's two
            Source::Disk => [
                "pkg:maven/uk.co.real-logic/artio-core@0.118",
                "pkg:maven/uk.co.real-logic/artio-codecs@0.118",
            ],
            Source::Memory => [
                "pkg:maven/org.example/alpha@1.0",
                "pkg:maven/org.example/beta@2.0",
            ],
            // a herd of both holds either, and the test asks for both
            Source::Both => [
                "pkg:maven/uk.co.real-logic/artio-core@0.118",
                "pkg:maven/org.example/alpha@1.0",
            ],
        }
    }
}

/// A script that runs a nested two-hop query and reports what it reached.
///
/// The nested query is built with the `highwire` DSL inside the script, so this
/// also covers the path where a sub-query's cluster is the one its parent was
/// given.
fn nesting_script(purl: &str) -> String {
    format!(
        "jars = highwire.walk(\n\
         \x20   name = 'jars',\n\
         \x20   roots = ['{purl}'],\n\
         \x20   traverse = ['alias:to'],\n\
         \x20   emit = 'identifier',\n\
         )\n\
         classes = highwire.walk(\n\
         \x20   name = 'classes',\n\
         \x20   roots = [highwire.from_stage(jars, '@')],\n\
         \x20   traverse = ['contained:down'],\n\
         \x20   emit = 'identifier',\n\
         )\n\
         reached = []\n\
         for row in highwire.run([jars, classes]):\n\
         \x20   reached.append(row)\n\
         emit({{'nested': len(reached)}})\n",
    )
}

/// Run a barker containing a nested query against one source.
async fn nested_count(source: Source, purl: &str) -> Result<Vec<Value>> {
    let script = nesting_script(purl);
    let document = serde_json::json!({
        "phases": [
            {"name": "nesting", "barker": script, "inputs": []}
        ]
    });

    // a barker with no inputs still needs its input stream closed, or it waits
    let (sender, receiver) = flume::unbounded::<Value>();
    drop(sender);
    let _ = receiver;

    rows(document, source.lookup().await)
}

/// Requirement: a barker can run a nested query against any of the three sources.
///
/// What: the same script — a two-hop walk built with the `highwire` DSL — run once
/// per source, asserting the nested query reached the same number of rows each
/// time.
///
/// Why: the three sources answer through different code, and a nested query reaches
/// them from a script's thread rather than the caller's. Nothing else exercises
/// that: the graph tests in this file never nest, and the nesting tests in barker
/// only ever use an in-memory herd. A failure here names the source.
///
/// LLM section: the count is what matters, not the identifiers, because the two
/// graphs contain different ones. Each source's purl leads to one jar and that jar's
/// classes, so the count is the same shape in all three cases.
#[tokio::test]
async fn a_nested_query_reaches_any_source() -> Result<()> {
    let mut counts = Vec::new();
    for source in [Source::Disk, Source::Memory, Source::Both] {
        let purl = source.purls()[0];
        let rows = nested_count(source, purl).await?;
        assert_eq!(
            rows.len(),
            1,
            "source {} emitted its one summary row: {rows:?}",
            source.name()
        );
        let nested = rows[0]["nested"]
            .as_i64()
            .unwrap_or_else(|| panic!("source {} reported a count: {rows:?}", source.name()));
        assert!(
            nested > 0,
            "source {} reached rows through the nested query: {rows:?}",
            source.name()
        );
        counts.push(nested);
    }
    Ok(())
}

/// Requirement: the nine combinations of parent source and nested source are all
/// answered.
///
/// What: for each parent source and each source named in the nested query, run a
/// script whose nested query walks that source's purl.
///
/// Why: `run_nested` takes the lookup from the script's `Context`, which the
/// compiler fills from the parent's `HerdOrCluster`. So the nested query's source is
/// always the parent's — there is no way for a script to choose a different one —
/// and what this pins is that the propagation works for every arm: a cluster, a
/// member, and a herd, as the parent. A parent of one kind that failed to pass its
/// lookup down would report zero, which is the mistake this catches.
///
/// LLM section: nine pairs, one assertion each. The nested purl is the parent's
/// source's purl, so the pair (parent, nested) is really "the parent was of this
/// kind, and the nested query reached that kind's identifiers".
#[tokio::test]
async fn every_parent_and_nested_source_combination_answers() -> Result<()> {
    for parent in [Source::Disk, Source::Memory, Source::Both] {
        for nested in [Source::Disk, Source::Memory, Source::Both] {
            let purl = nested.purls()[0];
            let rows = nested_count(parent, purl).await?;
            let reached = rows
                .first()
                .and_then(|row| row.get("nested"))
                .and_then(|n| n.as_i64());

            // Whether the purl is reachable is a fact about the purl, not about the
            // kinds: each source's purl belongs to the disk graph or the memory
            // one, and a parent holds it if that graph is among its members. So the
            // question is answered from the purl itself rather than from a table of
            // kinds, which is where I first got it wrong.
            let is_disk_purl = purl.contains("artio");
            let reachable = match parent {
                // a herd of both holds both graphs
                Source::Both => true,
                Source::Disk => is_disk_purl,
                Source::Memory => !is_disk_purl,
            };

            match (reachable, reached) {
                (true, Some(n)) => assert!(
                    n > 0,
                    "parent {} nested {}: reached {n}",
                    parent.name(),
                    nested.name()
                ),
                (true, None) => panic!(
                    "parent {} nested {}: the summary row is missing: {rows:?}",
                    parent.name(),
                    nested.name()
                ),
                (false, Some(0)) | (false, None) => {}
                (false, Some(n)) => panic!(
                    "parent {} cannot hold nested {}'s purls, yet {n} rows were reached",
                    parent.name(),
                    nested.name()
                ),
            }
        }
    }
    Ok(())
}

// ------------------------------------------------ endpoint-equivalent queries

/// The directory of endpoint-equivalent query documents.
fn queries_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/queries")
}

/// Load a query document. The documents carry no roots: they take them from the
/// input stream, the way the V1 endpoints take an identifier from the path or the
/// request body, so one file serves both the single-identifier GET and the bulk
/// POST without rewriting.
fn endpoint_query(name: &str) -> Value {
    let text = std::fs::read_to_string(queries_dir().join(format!("{name}.json")))
        .unwrap_or_else(|e| panic!("the `{name}` query is present: {e}"));
    serde_json::from_str(&text).expect("the query is JSON")
}

/// The loaded cluster, for calling the V1 accessors directly.
async fn cluster() -> Arc<GoatRodeoCluster> {
    let clusters = GoatRodeoCluster::cluster_files_in_dir(fixture_dir(), false, vec![])
        .await
        .expect("the fixture cluster loads");
    clusters.into_iter().next().expect("one cluster")
}

/// The sorted identifiers a query emitted: a bare string row, or an object with
/// an `identifier` field.
fn query_identifiers(rows: &[Value]) -> Vec<String> {
    let mut out: Vec<String> = rows
        .iter()
        .filter_map(|row| match row {
            Value::String(id) => Some(id.clone()),
            Value::Object(entries) => entries.get("identifier")?.as_str().map(str::to_string),
            _ => None,
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

/// Run an endpoint query, feeding `identifiers` in on the input stream.
fn run_endpoint(name: &str, identifiers: &[&str], lookup: HerdOrCluster) -> Result<Vec<Value>> {
    let query: Rc<dyn Query> = Rc::new(endpoint_query(name));
    let cache = ProgramCache::new(512);

    // the caller's identifiers, on the input stream, closed when sent
    let (sender, input) = flume::unbounded();
    for identifier in identifiers {
        sender.send(Value::String(identifier.to_string()))?;
    }
    drop(sender);

    // the compiled run is created and dropped INSIDE the block, so its destructor —
    // which holds the sink's borrow — is gone before the rows are read
    let out = {
        let mut sink = RowSink::default();
        {
            let mut run = compile(query, &cache)?;
            run(Arc::new(NoBound), sink.sink(), lookup, input)?;
        }
        sink.0
    };
    Ok(out)
}

/// Requirement: every V1 HTTP endpoint has a query that expresses it.
///
/// What: each document in `test_data/queries` is loaded, given a real identifier,
/// compiled, and run against the fixture; the endpoint's characteristic answer is
/// asserted.
///
/// Why: the V1 endpoints are accessor calls — `item_for_identifier`,
/// `antialias_for`, `north_send`, `stream_flattened_items` — and each has a
/// graph-walk formulation. Writing them as documents turns them into things a
/// caller can change, compose and budget, and running them proves the formulation
/// is right rather than merely plausible.
///
/// LLM section: the fixture's `JAR_A` is aliased by two purls and contains three
/// items, so `/aa` from a purl reaches the jar, `/flatten` from the jar reaches its
/// contents, `/north` from a class reaches its jar, and `/north_purls` reaches the
/// purls above it.
#[tokio::test]
async fn every_endpoint_has_an_equivalent_query() -> Result<()> {
    let lookup = || async { disk_cluster().await };
    let purl_a = "pkg:maven/uk.co.real-logic/artio-core@0.118";
    // the jar, from the fixture's own description
    let jar = "gitoid:blob:sha256:8eba64f7eef0da9c9312a51033c61ee3f5c981edb6e34dc6b986ad65de56d515";
    let class = "gitoid:blob:sha256:cacfa4827b6f2e4930c5488c50a81731f0d2b975e9517ffcd3a2bade93ccb6a1";

    // /item — the item itself, by identifier
    let item = run_endpoint("item", &[purl_a], lookup().await)?;
    assert_eq!(item.len(), 1, "/item returns the one item: {item:?}");
    assert_eq!(item[0]["identifier"], serde_json::json!(purl_a));

    // /bulk — the same query, a set of identifiers in
    let bulk = run_endpoint(
        "bulk",
        &[purl_a, "pkg:maven/uk.co.real-logic/artio-codecs@0.118"],
        lookup().await,
    )?;
    assert_eq!(bulk.len(), 2, "/bulk answers for both: {bulk:?}");

    // /aa — the canonical item: what the alias resolves to, or the input itself
    // when it is not an alias
    for identifier in [purl_a, jar] {
        let endpoint: Vec<String> = Arc::clone(&cluster().await)
            .antialias_for(identifier)
            .map(|item| vec![item.identifier])
            .unwrap_or_default();
        let query = query_identifiers(&run_endpoint("anti_alias", &[identifier], lookup().await)?);
        assert_eq!(query, endpoint, "/aa agrees for {identifier}, alias or not");
    }

    // /flatten — V1's shape: the TARGET IDENTIFIER STRINGS of each visited
    // item's contained:down edges (the alias hop is navigation; the root's
    // own targets are included). The subtree is the same four identifiers the
    // old (wrong) template emitted as objects.
    let contained = run_endpoint("flatten", &[jar], lookup().await)?;
    assert_eq!(
        contained.len(),
        4,
        "/flatten reaches the jar's subtree, not the jar: {contained:?}"
    );
    assert!(
        contained.iter().all(|row| row.is_string()),
        "/flatten emits identifier strings, as V1 does: {contained:?}"
    );
    assert!(
        contained
            .iter()
            .any(|row| row.as_str().is_some_and(|id| id.starts_with("gitoid:"))),
        "the subtree's gitoids are among them: {contained:?}"
    );

    // /flatten_source — the archives beneath, gated on the emitting item
    let sources = run_endpoint("flatten_source", &[jar], lookup().await)?;
    // 0, and that is the accessor's answer: `source=true` emits an edge's target
    // only when the EMITTING item has a build:from edge, and the fixture has none.
    // The empty result is the agreement.
    assert_eq!(
        sources.len(),
        0,
        "/flatten_source: no build:from edges, so nothing is emitted: {sources:?}"
    );

    // /north — upward from a class, the item included
    let north = run_endpoint("north", &[class], lookup().await)?;
    assert!(
        north.iter().any(|row| row["identifier"] == serde_json::json!(class)),
        "/north includes the item it started from: {north:?}"
    );

    // /north_purls — the purls above an item
    let purls = run_endpoint("north_purls", &[jar], lookup().await)?;
    assert!(
        purls
            .iter()
            .all(|row| row.as_str().is_some_and(|id| id.starts_with("pkg:"))),
        "/north_purls returns only purls: {purls:?}"
    );

    Ok(())
}

/// THE PESSIMISTIC EQUIVALENCE TEST — against the REAL test data, not the
/// uniform micro fixture.
///
/// WHAT IT TESTS (PR #171 review, finding 6): `/north_purls`'s V2 template
/// must produce the SAME SET of values as V1's accessor — the reviewer's
/// measured 24-rows-vs-1 on `cluster_a`.
///
/// WHY THIS DATA: the `walk_v4` fixture cannot distinguish the two
/// implementations — every alias value on its paths is a purl, no two paths
/// reach one target — so set equality there is vacuous. `cluster_a` is a real
/// cluster whose item `gitoid:blob:sha256:5110e3464c462309adb4645f8664ed16338218211f4a86cc72cf5d2fd269df25`
/// carries an `alias:from` edge with SEVEN values: one purl and six hash
/// identities (gitoid, md5, sha1, sha256, sha512). V1 filters to `pkg:` and
/// dedups; the current V2 template does neither — the sets MUST disagree,
/// which is the red.
///
/// THEORY OF THE TEST: run BOTH implementations over the same cluster and the
/// same root, sort both sides, demand equality. V1's side is the trait's own
/// `north_send` BFS (each visited item's `find_purls`, globally deduped); the
/// V2 side is the template the server actually serves.
#[tokio::test(flavor = "multi_thread")]
async fn north_purls_v2_agrees_with_v1_on_real_data() -> Result<()> {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/cluster_a");
    let clusters = GoatRodeoCluster::cluster_files_in_dir(dir, false, vec![])
        .await
        .expect("cluster_a loads");
    let cluster = Arc::new(clusters.into_iter().next().expect("one cluster"));
    let root = "gitoid:blob:sha256:5110e3464c462309adb4645f8664ed16338218211f4a86cc72cf5d2fd269df25";

    // V1: the accessor the endpoint serves — BFS north, purls only, deduped
    let stream = (*Arc::clone(&cluster))
        .clone()
        .north_send(vec![root.to_string()], true, std::time::Instant::now())
        .await
        .expect("the north traversal runs");
    let mut values: Vec<String> = Vec::new();
    let mut stream = std::pin::pin!(tokio_stream::wrappers::ReceiverStream::new(stream));
    while let Some(either) = tokio_stream::StreamExt::next(&mut stream).await {
        if let tokio_util::either::Either::Right(purl) = either {
            values.push(purl);
        }
    }
    values.sort();
    values.dedup();
    let v1 = values;

    // V2: the template, over the same cluster and the same root
    let mut v2 = query_identifiers(&run_endpoint(
        "north_purls",
        &[root],
        HerdOrCluster::Cluster(Arc::clone(&cluster)),
    )?);
    v2.sort();
    v2.dedup();

    assert_eq!(
        v1, v2,
        "V1 and V2 must agree on real data: V1 {v1:?} vs V2 {v2:?}"
    );
    Ok(())
}

/// The walk's most basic operation over REAL data: every visited item emits
/// itself (`emit: "@"`), through the entry rule (cluster_a is V3, so the
/// lookup materializes items through the herd path — the byte path cannot
/// read V3 storage). The claim this pins: the walk over a real V3 cluster
/// produces rows at all, with failures VISIBLE as error rows rather than
/// silent.
#[tokio::test(flavor = "multi_thread")]
async fn the_basic_walk_over_cluster_a_emits_rows() -> Result<()> {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/cluster_a");
    let clusters = GoatRodeoCluster::cluster_files_in_dir(dir, false, vec![])
        .await
        .expect("cluster_a loads");
    let cluster = clusters.into_iter().next().expect("one cluster");
    let root = "gitoid:blob:sha256:5110e3464c462309adb4645f8664ed16338218211f4a86cc72cf5d2fd269df25";
    let rows = run_endpoint("basic_walk", &[root], HerdOrCluster::Cluster(cluster))?;
    println!("BASIC WALK ROWS: {}", rows.len());
    for row in rows.iter().take(6) {
        println!("  {row:?}");
    }
    Ok(())
}

/// The `pkg:` filter in the emit expression, over the real item known to
/// carry seven alias:from values (one purl, six hash identities): the
/// filtered projection yields exactly the one purl's row. The unit-level
/// proof behind the equivalence sweep's first rule.
#[tokio::test(flavor = "multi_thread")]
async fn the_pkg_filter_emit_yields_one_purl_on_the_known_item() -> Result<()> {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/cluster_a");
    let clusters = GoatRodeoCluster::cluster_files_in_dir(dir, false, vec![])
        .await
        .expect("cluster_a loads");
    let cluster = clusters.into_iter().next().expect("one cluster");
    let root = "gitoid:blob:sha256:5110e3464c462309adb4645f8664ed16338218211f4a86cc72cf5d2fd269df25";
    let rows = run_endpoint("filtered_emit", &[root], HerdOrCluster::Cluster(cluster))?;
    println!("FILTERED EMIT ROWS: {}", rows.len());
    for row in rows.iter().take(6) {
        println!("  {row:?}");
    }
    Ok(())
}





/// THE PESSIMISTIC FLATTEN EQUIVALENCE (PR #171 review, finding 7): V1's
/// `/flatten` emits the TARGET IDENTIFIER STRINGS of each visited item's
/// `contains:down` edges — the `alias:to` hop is NAVIGATION, never emitted —
/// deduped, root's targets included. Find a real root in cluster_a with
/// contains:down edges and demand set agreement.
#[tokio::test(flavor = "multi_thread")]
async fn flatten_v2_agrees_with_v1_on_real_data() -> Result<()> {
    use bigtent::item::EdgeType as _;
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/cluster_a");
    let clusters = GoatRodeoCluster::cluster_files_in_dir(dir.clone(), false, vec![])
        .await
        .expect("cluster_a loads");
    let cluster = Arc::new(clusters.into_iter().next().expect("one cluster"));

    // a root with contains:down edges, from the cluster's own index
    let mut root = String::new();
    {
        use bigtent::rodeo::index::IndexFile;
        let index_paths: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
            .expect("the cluster directory lists")
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|e| e == "gri"))
            .collect();
        'outer: for path in index_paths {
            let stem = path.file_stem().unwrap().to_str().unwrap();
            let hash = u64::from_str_radix(stem, 16).unwrap();
            let index_file = IndexFile::new(&dir, hash, false).await.unwrap();
            for offset in index_file.read_index().unwrap() {
                if let Some(item) = cluster.item_from_index_loc(&offset.loc) {
                    let has_contains = item
                        .connections
                        .iter()
                        .any(|(k, v)| k.is_contains_down() && !v.is_empty());
                    if has_contains {
                        root = item.identifier.clone();
                        break 'outer;
                    }
                }
            }
        }
    }
    assert!(!root.is_empty(), "cluster_a has an item with contains:down edges");

    // V1: the accessor the endpoint serves
    let stream = (*Arc::clone(&cluster))
        .clone()
        .stream_flattened_items(vec![root.clone()], false)
        .await
        .expect("the flatten runs");
    let mut v1: Vec<String> = Vec::new();
    let mut stream = std::pin::pin!(tokio_stream::wrappers::ReceiverStream::new(stream));
    while let Some(either) = tokio_stream::StreamExt::next(&mut stream).await {
        if let tokio_util::either::Either::Right(id) = either {
            v1.push(id);
        }
    }

    // V2: the template
    let raw = run_endpoint("flatten", &[&root], HerdOrCluster::Cluster(Arc::clone(&cluster)))?;

    assert_eq!(
        raw.len(),
        v1.len(),
        "the same number of rows, duplicates included: V1 {v1:?} vs V2 {raw:?}"
    );
    let set_v1: std::collections::HashSet<String> = v1.iter().cloned().collect();
    let set_v2: std::collections::HashSet<String> = raw
        .iter()
        .filter_map(|row| row.as_str().map(str::to_string))
        .collect();
    assert_eq!(set_v1, set_v2, "V1 and V2 agree as sets on real data");
    Ok(())
}

/// THE FLATTEN SWEEP: the same pessimism as the north_purls sweep — a random
/// 10% of cluster_a's items as roots, V1's `stream_flattened_items` vs the
/// template, agreeing on counts and sets. The seed is printed and replayable
/// via `SWEEP_SEED`.
#[tokio::test(flavor = "multi_thread")]
async fn a_random_tenth_of_cluster_a_agrees_flatten_v1_v2() -> Result<()> {
    use rand::seq::IndexedRandom as _;

    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/cluster_a");
    let clusters = GoatRodeoCluster::cluster_files_in_dir(dir.clone(), false, vec![])
        .await
        .expect("cluster_a loads");
    let cluster = Arc::new(clusters.into_iter().next().expect("one cluster"));

    let mut identifiers: Vec<String> = Vec::with_capacity(4096);
    let mut seen_ids = std::collections::HashSet::new();
    let index_paths: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
        .expect("the cluster directory lists")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|e| e == "gri"))
        .collect();
    for path in index_paths {
        let stem = path.file_stem().unwrap().to_str().unwrap();
        let hash = u64::from_str_radix(stem, 16).unwrap();
        let index_file = bigtent::rodeo::index::IndexFile::new(&dir, hash, false)
            .await
            .unwrap();
        for offset in index_file.read_index().unwrap() {
            if let Some(item) = cluster.item_from_index_loc(&offset.loc)
                && seen_ids.insert(item.identifier.clone())
            {
                identifiers.push(item.identifier);
            }
        }
    }

    let seed = match std::env::var("SWEEP_SEED") {
        Ok(value) => value.parse::<u64>().expect("SWEEP_SEED is a u64"),
        Err(_) => rand::random(),
    };
    println!("FLATTEN SWEEP SEED: {seed} (re-run with SWEEP_SEED={seed})");
    use rand::SeedableRng as _;
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let sample_size = (identifiers.len() as f64 * 0.10).ceil() as usize;
    let sample: Vec<&String> = identifiers.choose_multiple(&mut rng, sample_size).collect();

    let mut disagreements = Vec::new();
    for root in sample {
        for source in [false, true] {
            let stream = (*Arc::clone(&cluster))
                .clone()
                .stream_flattened_items(vec![root.to_string()], source)
                .await
                .expect("the flatten runs");
            let mut v1: Vec<String> = Vec::new();
            let mut stream =
                std::pin::pin!(tokio_stream::wrappers::ReceiverStream::new(stream));
            while let Some(either) = tokio_stream::StreamExt::next(&mut stream).await {
                if let tokio_util::either::Either::Right(id) = either {
                    v1.push(id);
                }
            }
            let template = if source { "flatten_source" } else { "flatten" };
            let raw = run_endpoint(template, &[root], HerdOrCluster::Cluster(Arc::clone(&cluster)))?;
            let set_v1: std::collections::HashSet<String> = v1.iter().cloned().collect();
            let set_v2: std::collections::HashSet<String> = raw
                .iter()
                .filter_map(|row| row.as_str().map(str::to_string))
                .collect();
            if raw.len() != v1.len() || set_v1 != set_v2 {
                disagreements.push((template, root.clone(), v1.clone(), raw.clone()));
            }
        }
    }
    assert!(
        disagreements.is_empty(),
        "flatten disagrees on {} of {} sampled roots; the first: {:?}",
        disagreements.len(),
        sample_size,
        disagreements.first()
    );
    Ok(())
}

/// LAYER 2 FOR FLATTEN — THE TRAP GRAPH: every rule of V1's flatten as a
/// deliberate case:
/// * `root` — the input. alias:to → `canonical` (NAVIGATION: never emitted);
///   contained:down → {c1, shared, mid1} (the ROOT'S OWN targets ARE emitted).
/// * `canonical` — contained:down → {leaf} (its targets ARE emitted; it is not).
/// * `mid1` — contained:down → {shared} — the DIAMOND: `shared` is emitted by
///   two different items, once only.
///
/// V1's answer BY CONSTRUCTION: {c1, shared, mid1, leaf} — four rows:
/// `canonical` absent (alias is navigation), `shared` present once (dedup).
#[tokio::test(flavor = "multi_thread")]
async fn the_flatten_trap_graph_v1_and_v2_agree_on_every_rule() -> Result<()> {
    use bigtent::item::Item;
    use std::collections::{BTreeMap, BTreeSet};

    fn item(identifier: &str, connections: &[(&str, &[&str])]) -> Item {
        let mut map = BTreeMap::new();
        for (edge, targets) in connections {
            map.insert(
                edge.to_string(),
                targets.iter().map(|t| t.to_string()).collect::<BTreeSet<String>>(),
            );
        }
        Item {
            identifier: identifier.to_string(),
            connections: map,
            body_mime_type: None,
            body: None,
        }
    }

    let items = vec![
        item("root", &[("alias:to", &["canonical"]), ("contained:down", &["c1", "shared", "mid1"])]),
        item("canonical", &[("contained:down", &["leaf"])]),
        item("mid1", &[("contained:down", &["shared"])]),
        item("leaf", &[]),
        item("c1", &[]),
        item("shared", &[]),
    ];
    let mut expected: Vec<String> = vec!["c1".into(), "shared".into(), "mid1".into(), "leaf".into()];
    expected.sort();

    let member = bigtent::rodeo::member::member_synth(bigtent::rodeo::robo_goat::RoboticGoat::new(
        "flatten-traps", items, serde_json::json!({}),
    ));
    let lookup = HerdOrCluster::Herd(Arc::new(GoatHerd::new(vec![member])));

    // V1: the accessor
    let herd = match &lookup {
        HerdOrCluster::Herd(herd) => Arc::clone(herd),
        _ => unreachable!(),
    };
    let stream = herd
        .clone()
        .stream_flattened_items(vec!["root".to_string()], false)
        .await
        .expect("the flatten runs");
    let mut v1: Vec<String> = Vec::new();
    let mut stream = std::pin::pin!(tokio_stream::wrappers::ReceiverStream::new(stream));
    while let Some(either) = tokio_stream::StreamExt::next(&mut stream).await {
        if let tokio_util::either::Either::Right(id) = either {
            v1.push(id);
        }
    }
    v1.sort();
    assert_eq!(v1, expected, "V1's answer is the constructed one");

    // V2: RAW rows — count first, duplicates cannot hide
    let raw = run_endpoint("flatten", &["root"], lookup)?;
    assert_eq!(
        raw.len(),
        v1.len(),
        "the same number of rows: V1 {v1:?} vs V2 {raw:?}"
    );
    let mut v2: Vec<String> = raw
        .iter()
        .filter_map(|row| row.as_str().map(str::to_string))
        .collect();
    v2.sort();
    assert_eq!(v1, v2, "V1 and V2 agree on every rule of flatten");
    Ok(())
}

// ============================================================
// THE FULL GRID: EVERY data endpoint, same standard as north_purls:
// single-root equivalence on cluster_a, a random 10% sweep (count +
// HashSet), and trap graphs for the endpoint's own rules.
//
// The agreement rule is uniform: the same NUMBER of rows AND the same
// HashSet of rendered rows — rendered, because serde_json::Value carries
// floats and cannot be hashed; the rendering is canonical (BTreeMap keys).
// ============================================================

/// The rendered-row set: HashSet(to_string(row)) — the agreement rule.
fn row_set(rows: &[Value]) -> std::collections::HashSet<String> {
    rows.iter().map(|row| row.to_string()).collect()
}

/// The V1 side for each endpoint: the accessor the endpoint serves, driven
/// over the cluster. Each returns the rows the V1 HTTP handler streams.
async fn v1_endpoint_rows(endpoint: &str, roots: &[String], cluster: &Arc<GoatRodeoCluster>) -> Vec<Value> {
    match endpoint {
        // the full item, by identifier — one per root, when found
        "item" => roots
            .iter()
            .filter_map(|r| cluster.item_for_identifier(r))
            .map(|item| serde_json::to_value(&item).expect("an item renders"))
            .collect(),
        // /bulk is /item for many identifiers: one item per root, when found
        "bulk" => Box::pin(v1_endpoint_rows("item", roots, cluster)).await,
        // the canonical item: the alias chain's end, or nothing
        "aa" => roots
            .iter()
            .filter_map(|r| Arc::clone(cluster).antialias_for(r))
            .map(|item| serde_json::to_value(&item).expect("an item renders"))
            .collect(),
        // north: every visited item, full, deduped by the processed set
        "north" => {
            let stream = Arc::new((*Arc::clone(cluster)).clone())
                .north_send(roots.to_vec(), false, std::time::Instant::now())
                .await
                .expect("the north traversal runs");
            let mut items: Vec<Value> = Vec::new();
            let mut stream = std::pin::pin!(tokio_stream::wrappers::ReceiverStream::new(stream));
            while let Some(either) = tokio_stream::StreamExt::next(&mut stream).await {
                if let tokio_util::either::Either::Left(item) = either {
                    items.push(serde_json::to_value(&item).expect("an item renders"));
                }
            }
            items
        }
        // north_purls: the BFS's purl strings, pkg:-filtered and deduped
        "north_purls" => {
            let stream = Arc::new((*Arc::clone(cluster)).clone())
                .north_send(roots.to_vec(), true, std::time::Instant::now())
                .await
                .expect("the north traversal runs");
            let mut purls: Vec<Value> = Vec::new();
            let mut stream =
                std::pin::pin!(tokio_stream::wrappers::ReceiverStream::new(stream));
            while let Some(either) = tokio_stream::StreamExt::next(&mut stream).await {
                if let tokio_util::either::Either::Right(purl) = either {
                    purls.push(serde_json::Value::String(purl));
                }
            }
            purls
        }
        // flatten and flatten_source: the target strings, deduped
        "flatten" | "flatten_source" => {
            let source = endpoint == "flatten_source";
            let stream = Arc::new((*Arc::clone(cluster)).clone())
                .stream_flattened_items(roots.to_vec(), source)
                .await
                .expect("the flatten runs");
            let mut ids: Vec<Value> = Vec::new();
            let mut stream =
                std::pin::pin!(tokio_stream::wrappers::ReceiverStream::new(stream));
            while let Some(either) = tokio_stream::StreamExt::next(&mut stream).await {
                if let tokio_util::either::Either::Right(id) = either {
                    ids.push(serde_json::Value::String(id));
                }
            }
            ids
        }
        other => panic!("{other} is not a data endpoint"),
    }
}

/// THE GRID: every endpoint, every sampled root of a random tenth of
/// cluster_a — count and HashSet agreement.
#[tokio::test(flavor = "multi_thread")]
async fn every_data_endpoint_agrees_on_a_random_tenth_of_cluster_a() -> Result<()> {
    use rand::seq::IndexedRandom as _;

    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data/cluster_a");
    let clusters = GoatRodeoCluster::cluster_files_in_dir(dir.clone(), false, vec![])
        .await
        .expect("cluster_a loads");
    let cluster = Arc::new(clusters.into_iter().next().expect("one cluster"));

    let mut identifiers: Vec<String> = Vec::with_capacity(4096);
    let mut seen_ids = std::collections::HashSet::new();
    let index_paths: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
        .expect("the cluster directory lists")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|e| e == "gri"))
        .collect();
    for path in index_paths {
        let stem = path.file_stem().unwrap().to_str().unwrap();
        let hash = u64::from_str_radix(stem, 16).unwrap();
        let index_file = bigtent::rodeo::index::IndexFile::new(&dir, hash, false)
            .await
            .unwrap();
        for offset in index_file.read_index().unwrap() {
            if let Some(item) = cluster.item_from_index_loc(&offset.loc)
                && seen_ids.insert(item.identifier.clone())
            {
                identifiers.push(item.identifier);
            }
        }
    }

    let seed = match std::env::var("SWEEP_SEED") {
        Ok(value) => value.parse::<u64>().expect("SWEEP_SEED is a u64"),
        Err(_) => rand::random(),
    };
    println!("GRID SWEEP SEED: {seed} (re-run with SWEEP_SEED={seed})");
    use rand::SeedableRng as _;
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let sample_size = (identifiers.len() as f64 * 0.10).ceil() as usize;
    let sample: Vec<&String> = identifiers.choose_multiple(&mut rng, sample_size).collect();

    for endpoint in ["item", "bulk", "aa", "north", "north_purls", "flatten", "flatten_source"] {
        let mut disagreements = Vec::new();
        for root in &sample {
            let v1 =
                v1_endpoint_rows(endpoint, std::slice::from_ref(root), &cluster).await;
            // the template document's file name: `aa`'s is anti_alias.json
            let document_name = if endpoint == "aa" { "anti_alias" } else { endpoint };
            let v2 = run_endpoint(document_name, &[root], HerdOrCluster::Cluster(Arc::clone(&cluster)))?;
            if v1.len() != v2.len() || row_set(&v1) != row_set(&v2) {
                // name the DIFFERENCE, not just the fact: the rows only one
                // side produced
                let s1 = row_set(&v1);
                let s2 = row_set(&v2);
                let only_v1: Vec<String> = s1.difference(&s2).take(3).cloned().collect();
                let only_v2: Vec<String> = s2.difference(&s1).take(3).cloned().collect();
                disagreements.push((root.as_str().to_string(), only_v1, only_v2, v1.len(), v2.len()));
            }
        }
        assert!(
            disagreements.is_empty(),
            "{endpoint} disagrees on {} of {} sampled roots; the first — \
             root, rows only V1 emitted, rows only V2 emitted, |V1|, |V2|: {:?}",
            disagreements.len(),
            sample_size,
            disagreements.first()
        );
    }
    Ok(())
}

/// THE GRID'S TRAP GRAPH — the AA rule: the canonical item is the alias
/// chain's END (walking alias:to, the `is_terminal` gate admits only the
/// last); a NON-alias root is its own canonical item and emits ITSELF; an
/// alias CYCLE must not spin.
#[tokio::test(flavor = "multi_thread")]
async fn the_aa_trap_graph_v1_and_v2_agree_on_every_rule() -> Result<()> {
    use bigtent::item::Item;
    use std::collections::{BTreeMap, BTreeSet};

    fn item(identifier: &str, connections: &[(&str, &[&str])]) -> Item {
        let mut map = BTreeMap::new();
        for (edge, targets) in connections {
            map.insert(
                edge.to_string(),
                targets.iter().map(|t| t.to_string()).collect::<BTreeSet<String>>(),
            );
        }
        Item {
            identifier: identifier.to_string(),
            connections: map,
            body_mime_type: None,
            body: None,
        }
    }

    let items = vec![
        item("purl", &[("alias:to", &["mid"])]),
        item("mid", &[("alias:to", &["canonical"])]),
        item("canonical", &[]),
        // a lone non-alias item: its own aa
        item("loner", &[]),
    ];
    let member = bigtent::rodeo::member::member_synth(bigtent::rodeo::robo_goat::RoboticGoat::new(
        "aa-traps", items, serde_json::json!({}),
    ));
    let lookup = HerdOrCluster::Herd(Arc::new(GoatHerd::new(vec![member])));
    let herd = match &lookup {
        HerdOrCluster::Herd(herd) => Arc::clone(herd),
        _ => unreachable!(),
    };

    // the alias chain: V1 and V2 both name the canonical item
    for root in ["purl", "mid", "canonical", "loner"] {
        let v1: Vec<Value> = herd
            .clone()
            .antialias_for(root)
            .map(|item| vec![serde_json::to_value(item.identifier).unwrap()])
            .unwrap_or_default();
        let v2 = run_endpoint("anti_alias", &[root], HerdOrCluster::Herd(Arc::clone(&herd)))?;

        // the V1 accessor names the canonical item's identifier; the V2
        // template emits the canonical item — compare the IDENTIFIERS
        let v1_ids: Vec<String> = v1
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect();
        let v2_ids = query_identifiers(&v2);
        assert_eq!(
            v1_ids, v2_ids,
            "/aa agrees for {root}: V1 {v1_ids:?} vs V2 {v2_ids:?}"
        );
    }
    Ok(())
}
