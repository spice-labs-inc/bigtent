//! The High Wire walk grammar: `when` gates, per-type walk selectors,
//! and the round-processing loop (filter -> emit -> stop).
//!
//! Prototype under active design discussion; the grammar may change
//! without notice. This module exists so the prototype compiles and
//! runs as part of the regular build.
//!

use anyhow::{Result, bail};
use serde_json::Value;
use std::collections::HashSet;
pub trait ThingSanshoCanWalk {}

pub struct WalkState {
    pub is_terminal: bool,
    pub depth_cnt: usize,
}

pub trait AccountingAndBudget{
    fn should_stop(&self) -> Option<Vec<Value>>;

    // a super cheap test to see if the sink should continue
    // processing Normal messages or has the budget been exceeded?
    fn can_sink_normal(&self) -> bool;
}

pub enum SinkOperation {
    NormalMessage,
    ErrorMessage
}

pub fn walk<
    'a,
    THING: ThingSanshoCanWalk, // for an Item, it returns the materialized Item, for a CBOR byte array, the instance is something cheap thing that contains a slize to the mmap'ed memory for the the CBOR of the Item
    // given an identifer, return a reference to a thing
    ACCOUNTANT: AccountingAndBudget,
    THING_FINDER: Fn(&str, &mut ACCOUNTANT) -> Result<Option<THING>>,
    // whatever is done with the materialized JSON for an Emit
    SINK: Fn(Value, &mut ACCOUNTANT, SinkOperation) -> Result<()>,
    // for a given connection type (e.g., "connected:down" or "*:up"), return
    // the &str of each of the connections. Because the lifetime of the &str in the Vec
    // is the same as the THING, the &str's can be (and should be) zero copy
    CON_FINDER: Fn(&'a THING, &str) -> Vec<&'a str>,
    // filter the conn (note this is per connection type)
    CON_FILTER_FN: Fn(&str) -> bool,
    // Given an item, filter it... return true if the
    // item should be processed
    ITEM_FILTER_FN: Fn(&THING, &WalkState) -> bool,
    // materialize the Item into an array of Value. This allows
    // a single `emit` to emit many rows (e.g. all the pURLs in file_names)
    // and also allows multiple emit statements for a single Item
    ITEM_TO_JSON_FN: Fn(&THING, &WalkState) -> Vec<Value>,
    // Should the walk stop? The first terminates the walk at
    // the end of this set of traversals (continuing to process other
    // items at this level), the second is "stop immediately".
    // If the option is Some, it means stop. The Vec<Value> is sent to
    // the SINK so that a downstream process can understand why the
    // Walk was stopped
    SHOULD_STOP_FN: Fn(&THING, &WalkState) -> (Option<Vec<Value>>, Option<Vec<Value>>),
>(
    roots: Vec<String>,
    lookup: THING_FINDER,
    sink: SINK,
    connection_finder: CON_FINDER,
    connections: Vec<(String, CON_FILTER_FN)>,
    item_filter: ITEM_FILTER_FN,
    emit: ITEM_TO_JSON_FN,
    stop_fn: SHOULD_STOP_FN,
    accountant: &mut ACCOUNTANT,
) -> Result<()> {
    // keep track of depth
    let mut depth = 0usize;

    // the next set of identifiers
    let mut next_round: HashSet<String> = roots.iter().collect();

    // the identifiers we've seen... don't re-walk them
    let mut seen: HashSet<String> = HashSet::new();

    // a variable for "soft" stop
    let mut continue_walk = true;

    // if we're continuing and there are more items, keep traversing
    'outer: while continue_walk && !next_round.is_empty() {
        // stuff to process for this round
        let mut this_round = HashSet::new();

        // update this round and seen
        for v in next_round {
            seen.insert(v.clone());
            this_round.insert(v);
        }

        // the place to store identifiers for the next round
        next_round.clear();

        // the next thing to process
        for id in this_round {
            // get it
            if let Some(thing) = lookup(&id, accountant)? {
                // pre-build stuff for the next round... why?
                // We need to determine if this Item is terminal for
                // some of the state to pass to the functions, so we
                // don't want to actually add to `next_round`
                // until this Item passes
                let mut maybe_next_round: Vec<&str> = vec![];

                for (con_name, con_filter) in &connections {
                    // Get the connections based on the name
                    for maybe_con in connection_finder(&thing, con_name) {
                        // if they have not been seen and they pass the connection filter
                        // for this connection type, add them to the possibilities
                        // note this is a vec because adding is cheap... later, we'll
                        // de-dup with the set
                        if !seen.contains(maybe_con) && con_filter(maybe_con) {
                            maybe_next_round.push(maybe_con);
                        }
                    }
                }

                // create the walk state
                let walk_state = WalkState {
                    depth_cnt: depth,
                    is_terminal: maybe_next_round.is_empty(),
                };

                // do we want to process the item?
                if item_filter(&thing, &walk_state) {
                    // queue up the next round
                    for s in maybe_next_round {
                        next_round.insert(s.to_string());
                    }

                    // emit the stuff for this round
                    for emit_me in emit(&thing, &walk_state) {
                        sink(emit_me, accountant, SinkOperation::NormalMessage)?;
                    }

                    // do stop logic
                    match stop_fn(&thing, &walk_state) {
                        // both rules fire... emit both sets of values and stop
                        // immediately
                        (Some(a), Some(b)) => {
                            for v in a {
                                sink(v, accountant, SinkOperation::ErrorMessage)?;
                            }
                            for v in b {
                                sink(v, accountant, SinkOperation::ErrorMessage)?;
                            }
                            break 'outer;
                        }

                        // stop immediately
                        (_, Some(b)) => {
                            for v in b {
                                sink(v, accountant, SinkOperation::ErrorMessage)?;
                            }
                            break 'outer;
                        }

                        // stop at end of loop
                        (Some(a), _) => {
                            for v in a {
                                sink(v, accountant, SinkOperation::ErrorMessage)?;
                            }

                            continue_walk = false;
                        }
                        _ => {} // continue
                    }
                }
            }
        }
        if let Some(end_reasons) = accountant.should_stop() {
            for r in end_reasons {
                sink(r, accountant, SinkOperation::ErrorMessage)?;
            }

            bail!("Budget Exceeded"); // probably do some more structured error... maybe should_stop generates the return result... ???
        }
        depth += 1;
    }
    Ok(())
}
