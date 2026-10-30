//! LAYER 3 — THE PROPERTY TEST: for randomly generated graphs, V1's
//! `north_purls` accessor, V2's template, and an independent reference model
//! of V1's documented rule must ALL agree as sets.
//!
//! REQUIREMENT (PR #171 review, finding 6, remediation layer 3): the
//! equivalence must hold for every graph shape, not only the ones a
//! hand-built fixture or a real cluster happens to contain. The rules under
//! test are V1's own (`src/item.rs::find_purls` + the north BFS):
//!   1. each visited item contributes the `pkg:`-prefixed targets of its own
//!      `alias:from` edges — and ONLY those;
//!   2. a purl is emitted at most ONCE per request, no matter how many
//!      items carry it or how many paths reach them (global dedup);
//!   3. the north traversal follows `alias:to`, `tag:from` and `*:up` edges.
//!
//! THEORY OF THE TEST: the generator produces random small graphs — random
//! north edges of each type (including dangling targets), random `alias:from`
//! values drawn from a mixed pool (purls and non-purl noise). The reference
//! model computes the expected set from the SAME rules, written independently
//! in this file; the V1 accessor runs over the same graph through the real
//! herd; the V2 side runs the template the server actually serves. Three-way
//! agreement on every generated case; proptest shrinks any disagreement to a
//! minimal reproducing graph.

use bigtent::highwire::walk::HerdOrCluster;
use bigtent::item::Item;
use bigtent::rodeo::goat_herd::GoatHerd;
use bigtent::rodeo::goat_trait::GoatRodeoTrait;

#[allow(unused_imports)]
use bigtent::rodeo::goat_herd::GoatHerd as _GoatHerdForTrait;
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

/// The pool of `alias:from` values: two purls and three noise values, so the
/// `pkg:` filter has constant work to do.
const VALUES: [&str; 5] = [
    "pkg:a",
    "pkg:b",
    "gitoid:blob:sha256:noise",
    "md5:noise",
    "sha1:noise",
];

/// The north edges a generated item may carry to another item.
const NORTH_EDGES: [&str; 3] = ["alias:to", "tag:from", "contained:up"];

/// A generated graph: items 0..n, random north edges between them (dangling
/// targets allowed — both implementations must skip missing items the same
/// way), and random `alias:from` values per item.
#[derive(Clone, Debug)]
struct Graph {
    items: Vec<Item>,
    root: String,
}

impl Graph {
    /// The reference model: V1's rules, written out independently.
    fn expected_purls(&self) -> HashSet<String> {
        let mut found = HashSet::new();
        let mut queue: Vec<&Item> = Vec::new();
        if let Some(first) = self.items.iter().find(|i| i.identifier == self.root) {
            queue.push(first);
        }
        let mut visited: HashSet<String> = HashSet::new();
        while let Some(item) = queue.pop() {
            if !visited.insert(item.identifier.clone()) {
                continue;
            }
            // rule 1: the item's own alias:from values, pkg:-filtered
            for (edge, targets) in &item.connections {
                if edge == "alias:from" {
                    for target in targets {
                        if target.starts_with("pkg:") {
                            found.insert(target.clone()); // rule 2: set = dedup
                        }
                    }
                }
            }
            // rule 3: north edges
            for (edge, targets) in &item.connections {
                if edge == "alias:to" || edge == "tag:from" || edge.ends_with(":up") {
                    for target in targets {
                        if let Some(next) =
                            self.items.iter().find(|i| &i.identifier == target)
                        {
                            queue.push(next);
                        }
                    }
                }
            }
        }
        found
    }

    fn as_member(&self) -> Arc<bigtent::rodeo::member::HerdMember> {
        bigtent::rodeo::member::member_synth(bigtent::rodeo::robo_goat::RoboticGoat::new(
            "generated",
            self.items.clone(),
            serde_json::json!({}),
        ))
    }
}

/// The generator.
fn graph_strategy() -> impl Strategy<Value = Graph> {
    (
        2usize..6,
        prop::collection::vec(
            (
                0usize..6,               // the source item
                // EVERY north edge is reachable — `contained:up` included,
                // which a 0..2 index could never select
                prop::option::of(0usize..NORTH_EDGES.len()),
                // the target may name NO item: `n` is the item count, so a
                // target of `n` or more dangles, and both implementations must
                // skip a missing item the same way
                0usize..8,
                // the WHOLE pool, so `md5:noise` and `sha1:noise` (the values a
                // narrow range made unreachable) can appear
                prop::option::of(0usize..VALUES.len()),
                prop::option::of(0usize..VALUES.len()),
            ),
            2..8,
        ),
        0usize..2, // the root item
    )
        .prop_map(|(n, edges, root)| {
            let mut items: Vec<Item> = (0..n)
                .map(|i| Item {
                    identifier: format!("i{i}"),
                    connections: BTreeMap::new(),
                    body_mime_type: None,
                    body: None,
                })
                .collect();
            for (source, edge, target, slot1, slot2) in edges {
                if let Some(edge_index) = edge {
                    let edge_name = NORTH_EDGES[edge_index % NORTH_EDGES.len()];
                    let target_id = format!("i{}", target);
                    items[source % n]
                        .connections
                        .entry(edge_name.to_string())
                        .or_default()
                        .insert(target_id);
                }
                let mut values: BTreeSet<String> = BTreeSet::new();
                for slot in [slot1, slot2].into_iter().flatten() {
                    values.insert(VALUES[slot % VALUES.len()].to_string());
                }
                if !values.is_empty() {
                    items[source % n]
                        .connections
                        .insert("alias:from".to_string(), values);
                }
            }
            Graph {
                items,
                root: format!("i{root}"),
            }
        })
}

/// The test's no-op accountant: the compiler is generic over it, so an
/// integration test brings its own.
#[derive(Default)]
struct NoBudget;

impl bigtent::highwire::walk::AccountingAndBudget for NoBudget {
    fn should_stop(&self) -> Option<Vec<serde_json::Value>> {
        None
    }
    fn can_sink_normal(&self) -> bool {
        true
    }
    fn event(&self, _event_type: bigtent::highwire::walk::AccountingEventType) {}
}

/// V2's side: the template the server serves, run over the generated graph.
///
/// The sink pattern mirrors the other integration tests: the collector
/// OUTLIVES the run binding, whose destructor holds the sink's borrow.
fn v2_rows(
    graph: &Graph,
    lookup: HerdOrCluster,
    template_name: &str,
) -> Result<Vec<String>, anyhow::Error> {
    struct Rows(Vec<serde_json::Value>);
    impl Rows {
        fn sink(&mut self) -> impl FnMut(serde_json::Value, &std::sync::Arc<dyn bigtent::highwire::walk::AccountingAndBudget + Send + Sync>, bigtent::highwire::walk::SinkOperation) -> Result<(), anyhow::Error> + '_ {
            move |row, _a, _op| {
                self.0.push(row);
                Ok(())
            }
        }
    }

    let text = std::fs::read_to_string(format!("test_data/queries/{template_name}.json"))
        .expect("the template is present");
    let document: serde_json::Value = serde_json::from_str(&text)?;
    let query: std::rc::Rc<dyn bigtent::highwire::grammar::Query> = std::rc::Rc::new(document);
    let cache = sansho::cache::ProgramCache::new(512);

    let (sender, input) = flume::unbounded();
    sender.send(serde_json::Value::String(graph.root.clone()))?;
    drop(sender);

    let mut rows = Rows(Vec::new());
    {
        let mut run = bigtent::highwire::compiler::compile::<_>(query, &cache)?;
        run(std::sync::Arc::new(NoBudget), rows.sink(), lookup, input)?;
    }
    let mut v2: Vec<String> = rows
        .0
        .iter()
        .filter_map(|row| row.as_str().map(str::to_string))
        .collect();
    v2.sort();
    Ok(v2)
}

/// V1's flatten over a generated graph, through a dedicated runtime.
fn runtime_block(herd: Arc<GoatHerd>, graph: &Graph) -> Vec<String> {
    tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(async {
            let stream = herd
                .clone()
                .stream_flattened_items(vec![graph.root.clone()], false)
                .await
                .expect("the flatten runs");
            let mut out: Vec<String> = Vec::new();
            let mut stream = std::pin::pin!(tokio_stream::wrappers::ReceiverStream::new(stream));
            while let Some(either) = tokio_stream::StreamExt::next(&mut stream).await {
                if let tokio_util::either::Either::Right(id) = either {
                    out.push(id);
                }
            }
            out.sort();
            out.dedup();
            out
        })
}

/// V1's side: the accessor the endpoint serves, over the same graph.
async fn v1_rows(graph: &Graph, lookup: HerdOrCluster) -> Vec<String> {
    let herd = match &lookup {
        HerdOrCluster::Herd(herd) => Arc::clone(herd),
        _ => unreachable!("generated graphs are herds"),
    };
    let stream = herd
        .clone()
        .north_send(vec![graph.root.clone()], true, std::time::Instant::now())
        .await
        .expect("the north traversal runs");
    let mut v1: Vec<String> = Vec::new();
    let mut stream = std::pin::pin!(tokio_stream::wrappers::ReceiverStream::new(stream));
    while let Some(either) = tokio_stream::StreamExt::next(&mut stream).await {
        if let tokio_util::either::Either::Right(purl) = either {
            v1.push(purl);
        }
    }
    v1.sort();
    v1.dedup();
    v1
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn three_ways_agree_on_every_generated_graph(graph in graph_strategy()) {
        let expected = graph.expected_purls();
        let member = graph.as_member();
        let lookup = HerdOrCluster::Herd(Arc::new(GoatHerd::new(vec![member])));

        // the reference model's set
        let mut reference: Vec<String> = expected.iter().cloned().collect();
        reference.sort();

        // V1: the real accessor, over the SAME lookup the V2 leg uses — a
        // second `as_member()` would clone the item list and quietly give the
        // two implementations different graphs
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let v1 = runtime.block_on(v1_rows(&graph, lookup.clone()));
        assert_eq!(v1, reference, "V1 matches the reference model — graph {graph:?}");

        // V2: the template
        let v2 = v2_rows(&graph, lookup, "north_purls").expect("the template runs");
        assert_eq!(v2, reference, "V2 matches the reference model — graph {graph:?}");
    }
}

/// THE FLATTEN PROPERTY: the same three-way agreement for `/flatten` —
/// V1's accessor, the template, and an independent reference model of V1's
/// flatten rules: each visited item contributes the target STRINGS of its
/// `contains:down` edges (the alias hop is navigation, never emitted),
/// deduped globally.
fn flatten_strategy() -> impl Strategy<Value = Graph> {
    // ITEMS HAVE ROLES, as the corpora do. An ALIAS is a naming item — a purl
    // or a hash identity — with `alias:to` pointing at content and `alias:from`
    // carrying its names; it never contains anything and is never contained. A
    // CONTENT item may carry `contained:down` to its members (files, classes,
    // nested archives) and may be the target of both an alias and a
    // containment edge.
    //
    // Without the roles the generator produced impossible items — an item that
    // aliases and is contained, an item containing itself — and the property
    // test "found" a V1/V2 divergence that exists only on that impossible data.
    (
        2usize..6,
        prop::collection::vec(
            (
                0usize..6,                   // the source item
                prop::option::of(0usize..3), // 0 alias:to, 1 contained:down, 2 none
                0usize..8,                   // the target (may dangle)
            ),
            2..8,
        ),
        0usize..2, // the root item
    )
        .prop_map(|(n, edges, root)| {
            let mut items: Vec<Item> = (0..n)
                .map(|i| Item {
                    identifier: format!("i{i}"),
                    connections: BTreeMap::new(),
                    body_mime_type: None,
                    body: None,
                })
                .collect();
            // even indices are ALIASES, odd are CONTENT
            let is_alias = |index: usize| index.is_multiple_of(2);
            for (source, edge, target) in edges {
                let source = source % n;
                match edge {
                    // an ALIAS aliases content: the target must be content, and
                    // a target that dangles is still a legal shape
                    Some(0) if is_alias(source) => {
                        if target < n && is_alias(target) {
                            continue; // an alias does not alias an alias
                        }
                        items[source]
                            .connections
                            .entry("alias:to".to_string())
                            .or_default()
                            .insert(format!("i{target}"));
                    }
                    // CONTENT contains content: strictly downward, so the graph
                    // is a DAG like the corpora's (a nested archive is deeper)
                    Some(1) if !is_alias(source) && target > source => {
                        items[source]
                            .connections
                            .entry("contained:down".to_string())
                            .or_default()
                            .insert(format!("i{target}"));
                    }
                    _ => continue,
                }
            }
            Graph {
                items,
                root: format!("i{root}"),
            }
        })
}

fn expected_flatten(graph: &Graph) -> HashSet<String> {
    // V1's rule, mirrored EXACTLY: a round-based BFS in which a target is
    // emitted only when it is in neither THIS round's set nor the processed set.
    // Two consequences the worklist model this replaces got wrong — and the
    // property test found both:
    //   * SELF-EDGE: a target that is the item being processed is skipped, so
    //     `i0 -> i0` emits nothing;
    //   * SAME-ROUND PICKUP: an item is moved to `processed` as soon as it is
    //     handled, so a later item in the same round that points at it skips it,
    //     while a target queued for the NEXT round is not yet skipped.
    let mut emitted: HashSet<String> = HashSet::new();
    let mut processed: HashSet<String> = HashSet::new();
    let mut to_find: HashSet<String> = HashSet::new();
    to_find.insert(graph.root.clone());
    while !to_find.is_empty() {
        let mut next: HashSet<String> = HashSet::new();
        for identifier in to_find.clone() {
            let Some(item) = graph.items.iter().find(|i| i.identifier == identifier) else {
                continue;
            };
            for (edge, targets) in &item.connections {
                for target in targets {
                    if to_find.contains(target) || processed.contains(target) {
                        continue;
                    }
                    if edge == "contained:down" {
                        next.insert(target.clone());
                        emitted.insert(target.clone());
                    } else if edge == "alias:to" {
                        // the alias hop is NAVIGATION: enqueued, never emitted
                        next.insert(target.clone());
                    }
                }
            }
            processed.insert(identifier);
        }
        to_find = next;
    }
    emitted
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn flatten_three_ways_agree_on_every_generated_graph(graph in flatten_strategy()) {
        let expected = expected_flatten(&graph);
        let mut reference: Vec<String> = expected.iter().cloned().collect();
        reference.sort();

        let member = graph.as_member();
        let lookup = HerdOrCluster::Herd(Arc::new(GoatHerd::new(vec![member])));

        // V1: the real accessor — the stream is tokio mpsc, so drive it with
        // a blocking_recv loop rather than a stream
        let herd = match &lookup {
            HerdOrCluster::Herd(herd) => Arc::clone(herd),
            _ => unreachable!(),
        };
        let v1 = runtime_block(herd, &graph);
        assert_eq!(v1, reference, "V1 matches the reference model — graph {graph:?}");

        // V2: the template
        let v2 = v2_rows(&graph, lookup, "flatten").expect("the template runs");
        assert_eq!(v2, reference, "V2 matches the reference model — graph {graph:?}");
    }
}
