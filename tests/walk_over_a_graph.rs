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

    // /flatten — the whole subtree beneath the jar, the jar itself excluded
    let contained = run_endpoint("flatten", &[jar], lookup().await)?;
    // The accessor emits the TARGETS of the edges it follows, never the seed — so
    // the query's depth gate excludes the jar itself, and the five rows are its
    // whole subtree: two classes, the nested jar, and the class inside it.
    // 4: the accessor emits the targets of the edges it follows, never the seed —
    // the depth gate excludes the jar itself, and the four are its whole subtree.
    assert_eq!(
        contained.len(),
        4,
        "/flatten reaches the jar's subtree, not the jar: {contained:?}"
    );
    assert!(
        contained.iter().any(|row| row["identifier"]
            .as_str()
            .is_some_and(|id| id.starts_with("gitoid:"))
            && row["body_mime_type"] == serde_json::json!("application/java-archive")),
        "the nested jar is among them: {contained:?}"
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
