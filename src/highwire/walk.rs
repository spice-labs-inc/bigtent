//! The High Wire walk grammar: `when` gates, per-type walk selectors,
//! and the round-processing loop (filter -> emit -> stop).
//!
//! Prototype under active design discussion; the grammar may change
//! without notice. This module exists so the prototype compiles and
//! runs as part of the regular build.
//!

use crate::item::Item;
use crate::rodeo::goat::GoatRodeoCluster;
use crate::rodeo::goat_herd::GoatHerd;
use crate::rodeo::goat_trait::GoatRodeoTrait;
use crate::rodeo::index::GetOffset;
use anyhow::{Result, bail};
use sansho::{Program, SanshoTrait, ZeroCopySanshoResult};
use serde_json::Value;
use std::{collections::HashSet, fmt::Debug, sync::Arc};
use tokio::sync::mpsc::Sender;

pub struct WalkState {
    pub is_terminal: bool,
    pub depth_cnt: usize,
}

pub trait AccountingAndBudget {
    fn should_stop(&self) -> Option<Vec<Value>>;

    // a super cheap test to see if the sink should continue
    // processing Normal messages or has the budget been exceeded?
    fn can_sink_normal(&self) -> bool;

    // Note the AccountingAndBudget implementations will likely be
    // shared in an Arc across threads, so inner mutability (likely and Atomic)
    fn event(&self, event_type: AccountingEventType);
}

pub enum AccountingEventType {
    LoadItem,
    Emit,
}

pub enum SinkOperation {
    NormalMessage,
    ErrorMessage,
}

pub fn walk<
    'a,
    THING: SanshoTrait<'a>,
    ACCOUNTANT: AccountingAndBudget,
    ThingFinder: Fn(&str, &Arc<ACCOUNTANT>) -> Result<Option<THING>> + 'a,
    // whatever is done with the materialized JSON for an Emit
    SINK: FnMut(Value, &Arc<ACCOUNTANT>, SinkOperation) -> Result<()>,
    // for a given connection type (e.g., "connected:down" only exact lookups), return
    // the &str of each of the connections. The strings come out of the document the
    // THING reads, so they borrow for the document's lifetime ('a) — zero copy, and
    // long enough that a filter built once can be handed them (see ConFilter)
    ConFinder: for<'b> Fn(&'b THING, &str) -> Vec<&'b str>,
    // filter the conn (note this is per connection type). The identifier borrows for
    // the document's lifetime, which is what lets the predicate be built once — a
    // predicate over a per-call borrow could not be stored
    ConFilter: Fn(&(&str, usize)) -> bool,
    // Given an item, filter it... return true if the
    // item should be processed
    ItemFilter: Fn(&(&THING, &WalkState)) -> bool,
    // materialize the Item into an array of Value. This allows
    // a single `emit` to emit many rows (e.g. all the pURLs in file_names)
    // and also allows multiple emit statements for a single Item
    ItemToJson: Fn(&(&THING, &WalkState)) -> Vec<Value>,
    // Should the walk stop? The first terminates the walk at
    // the end of this set of traversals (continuing to process other
    // items at this level), the second is "stop immediately".
    // If the option is Some, it means stop. The Vec<Value> is sent to
    // the SINK so that a downstream process can understand why the
    // Walk was stopped
    ShouldStop: Fn(&(&THING, &WalkState)) -> (Option<Vec<Value>>, Option<Vec<Value>>),
>(
    roots: Vec<String>,
    lookup: ThingFinder,
    mut sink: SINK,
    connection_finder: ConFinder,
    connections: Vec<(String, ConFilter)>,
    item_filter: ItemFilter,
    emit: ItemToJson,
    stop_fn: ShouldStop,
    accountant: Arc<ACCOUNTANT>,
) -> Result<()> {
    // keep track of depth
    let mut depth = 0usize;

    // the next set of identifiers
    let mut next_round: HashSet<String> = roots.into_iter().collect();

    // the identifiers we've seen... don't re-walk them
    let mut seen: HashSet<String> = HashSet::new();

    // a variable for "soft" stop
    let mut continue_walk = true;

    // if we're continuing and there are more items, keep traversing
    'outer: while continue_walk && !next_round.is_empty() {
        // stuff to process for this round
        let mut this_round = HashSet::new();

        // update this round and seen
        for v in &next_round {
            seen.insert(v.clone());
            this_round.insert(v.clone());
        }

        // the place to store identifiers for the next round
        next_round.clear();

        // the next thing to process
        for id in this_round {
            let thing2: Option<THING> = lookup(&id, &accountant)?;
            // get it
            if let Some(thing) = thing2 {
                // pre-build stuff for the next round
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
                        if !seen.contains(maybe_con) && con_filter(&(maybe_con, depth)) {
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
                if item_filter(&(&thing, &walk_state)) {
                    // queue up the next round
                    for s in maybe_next_round {
                        next_round.insert(s.to_string());
                    }

                    // emit the stuff for this round
                    for emit_me in emit(&(&thing, &walk_state)) {
                        sink(emit_me, &accountant, SinkOperation::NormalMessage)?;
                    }

                    // do stop logic
                    match stop_fn(&(&thing, &walk_state)) {
                        // both rules fire... emit both sets of values and stop
                        // immediately
                        (Some(a), Some(b)) => {
                            for v in a {
                                sink(v, &accountant, SinkOperation::ErrorMessage)?;
                            }
                            for v in b {
                                sink(v, &accountant, SinkOperation::ErrorMessage)?;
                            }
                            break 'outer;
                        }

                        // stop immediately
                        (_, Some(b)) => {
                            for v in b {
                                sink(v, &accountant, SinkOperation::ErrorMessage)?;
                            }
                            break 'outer;
                        }

                        // stop at end of loop
                        (Some(a), _) => {
                            for v in a {
                                sink(v, &accountant, SinkOperation::ErrorMessage)?;
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
                sink(r, &accountant, SinkOperation::ErrorMessage)?;
            }

            bail!("Budget Exceeded"); // probably do some more structured error... maybe should_stop generates the return result... ???
        }
        depth += 1;
    }
    Ok(())
}

pub fn find_func<'a, A: AccountingAndBudget>(
    cluster: &'a Arc<GoatRodeoCluster>,
) -> impl Fn(&str, &Arc<A>) -> Result<Option<&'a [u8]>> + 'a {
    move |id: &str, accounting: &Arc<A>| -> Result<Option<&'a [u8]>> {
        accounting.event(AccountingEventType::LoadItem);
        let Some(io) = cluster.identifier_to_item_offset(id) else {
            return Ok(None);
        };
        Ok(cluster.bytes_for_file_and_offset(io.loc.get_file_hash(), io.loc.get_offset()))
    }
}

pub fn find_func_for_herd<'a, A: AccountingAndBudget>(
    herd: Arc<GoatHerd>,
) -> impl Fn(&str, &Arc<A>) -> Result<Option<Item>> + 'a {
    move |id: &str, accounting: &Arc<A>| -> Result<Option<Item>> {
        accounting.event(AccountingEventType::LoadItem);
        Ok(herd.item_for_identifier(id))
    }
}

pub fn vec_sink<'a, A: AccountingAndBudget>(
    vec: &'a mut Vec<Value>,
) -> impl FnMut(Value, &Arc<A>, SinkOperation) -> Result<()> {
    move |v: Value, accounting: &Arc<A>, _: SinkOperation| -> Result<()> {
        vec.push(v);
        accounting.event(AccountingEventType::Emit);
        Ok(())
    }
}

pub fn flume_sink<A: AccountingAndBudget>(
    sender: Sender<Value>,
) -> impl FnMut(Value, &Arc<A>, SinkOperation) -> Result<()> {
    move |v: Value, accounting: &Arc<A>, _: SinkOperation| -> Result<()> {
        sender.blocking_send(v)?;
        accounting.event(AccountingEventType::Emit);
        Ok(())
    }
}

pub fn compile_sansho(exp: &str) -> Result<Program> {
    Ok(sansho::compile(&sansho::parse(exp)?)?)
}

pub fn run_sansho<'d, T: SanshoTrait<'d> + ?Sized>(
    input: &'d T,
    program: &Program,
) -> Result<ZeroCopySanshoResult<'d>> {
    Ok(sansho::lookup(input, program)?)
}

pub trait ExpressionBuilder: Debug {
    fn not(&self) -> Result<Option<&Self>>;
    fn and(&self) -> Result<Option<&[Self]>>
    where
        Self: Sized;
    fn or(&self) -> Result<Option<&[Self]>>
    where
        Self: Sized;
    fn as_compare(&self) -> Result<Option<(CompareTypes, &str, &Self)>>;
    fn as_string(&self) -> Option<&str>;
    fn as_int(&self) -> Option<isize>;
    fn as_bool(&self) -> Option<bool>;
}

/// The gate grammar's JSON encoding.
///
/// Every compound node is a single-entry object keyed by its operator —
/// `{"not": …}` / `{"!": …}`, `{"and": […]}` / `{"&&": […]}`,
/// `{"or": […]}` / `{"||": […]}` — and a comparison is
/// `{"<op>": {"<variable>": <value>}}`.
///
/// The probes separate three answers: `Ok(Some(_))` — this is that node;
/// `Ok(None)` — this is not that node; `Err(_)` — this names that
/// operator but its payload is malformed. The error is constructed here
/// because here is where the shape is known.
impl ExpressionBuilder for serde_json::Value {
    fn not(&self) -> Result<Option<&Self>> {
        let Some((operator, operand)) = single_entry(self) else {
            return Ok(None);
        };
        match operator.as_str() {
            "not" | "!" => Ok(Some(operand)),
            _ => Ok(None),
        }
    }

    fn and(&self) -> Result<Option<&[Self]>> {
        let Some((operator, operand)) = single_entry(self) else {
            return Ok(None);
        };
        if operator.as_str() != "and" && operator.as_str() != "&&" {
            return Ok(None);
        }
        match operand {
            serde_json::Value::Array(expressions) => Ok(Some(expressions)),
            other => bail!("`{operator}` takes an array of gate expressions, found {other}"),
        }
    }

    fn or(&self) -> Result<Option<&[Self]>> {
        let Some((operator, operand)) = single_entry(self) else {
            return Ok(None);
        };
        if operator.as_str() != "or" && operator.as_str() != "||" {
            return Ok(None);
        }
        match operand {
            serde_json::Value::Array(expressions) => Ok(Some(expressions)),
            other => bail!("`{operator}` takes an array of gate expressions, found {other}"),
        }
    }

    /// A comparison node: `{"<op>": {"<variable>": <value>}}` — for
    /// example `{"<=": {"depth": 2}}`.
    ///
    /// The operator leads the object (parallel to `{"and": [...]}`,
    /// `{"or": [...]}`, `{"not": {...}}`) and is one of `>`, `>=`,
    /// `=`/`==`, `!=`, `<`, `<=` or their word spellings (`gt`, `gte`,
    /// `eq`, `neq`, `noteq`, `lt`, `lte`). Its single value is the
    /// single-entry object naming the variable the comparison reads and
    /// the value it compares against. The returned reference is that
    /// value, so the comparator builder reads it through `as_int` /
    /// `as_bool` — the variable vocabulary is NOT closed here, the
    /// comparator builder is what knows which variables exist and where
    /// their values come from.
    ///
    /// A node naming a comparison operator that does not carry
    /// `{"<variable>": <value>}` is an error rather than a non-node: the
    /// author meant a comparison, and the message says what is wrong.
    fn as_compare(&self) -> Result<Option<(CompareTypes, &str, &Self)>> {
        let Some((operator, operands)) = single_entry(self) else {
            return Ok(None);
        };
        let Some(compare_type) = comparison_of(operator.as_str()) else {
            return Ok(None);
        };
        let serde_json::Value::Object(operands) = operands else {
            bail!("`{operator}` takes {{\"<variable>\": <value>}}, found {operands}");
        };
        if operands.len() != 1 {
            bail!(
                "`{operator}` reads exactly one variable, found {} in {operands:?}",
                operands.len()
            );
        }
        let (variable, value) = operands.iter().next().expect("exactly one entry");
        Ok(Some((compare_type, variable.as_str(), value)))
    }

    /// An integer leaf: the right-hand side of an integer comparison
    /// (the walk's `depth`). Integral JSON only — a float, even one with
    /// a zero fraction, is not an integer leaf; the comparator builder
    /// reports the type error.
    fn as_int(&self) -> Option<isize> {
        let serde_json::Value::Number(number) = self else {
            return None;
        };
        if let Some(signed) = number.as_i64() {
            return isize::try_from(signed).ok();
        }
        number
            .as_u64()
            .and_then(|unsigned| isize::try_from(unsigned).ok())
    }

    fn as_bool(&self) -> Option<bool> {
        match self {
            serde_json::Value::Bool(v) => Some(*v),
            _ => None,
        }
    }

    fn as_string(&self) -> Option<&str> {
        match self {
            serde_json::Value::String(v) => Some(v),
            _ => None,
        }
    }
}

/// The single entry of a single-entry object — the shape every compound
/// gate node has. `None` for anything else: a non-object, an empty
/// object, or an object with several entries (those are not compound
/// nodes, and the single-entry rule is what keeps the probes
/// unambiguous).
fn single_entry(value: &serde_json::Value) -> Option<(&String, &serde_json::Value)> {
    let serde_json::Value::Object(entries) = value else {
        return None;
    };
    if entries.len() != 1 {
        return None;
    }
    entries.iter().next()
}

/// The comparison an operator name denotes — the symbol forms and their
/// word spellings. `None` for a name that is not a comparison at all.
fn comparison_of(operator: &str) -> Option<CompareTypes> {
    match operator {
        ">" | "gt" => Some(CompareTypes::GT),
        ">=" | "gte" => Some(CompareTypes::GTE),
        "=" | "==" | "eq" => Some(CompareTypes::EQ),
        "!=" | "neq" | "noteq" => Some(CompareTypes::NOTEQ),
        "<" | "lt" => Some(CompareTypes::LT),
        "<=" | "lte" => Some(CompareTypes::LTE),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompareTypes {
    GT,
    GTE,
    EQ,
    NOTEQ,
    LT,
    LTE,
}

pub fn build_con_compare_function_function<'a>()
-> Box<dyn Fn(&str) -> Result<Box<dyn Fn(&(&'a str, usize)) -> bool + 'a>>> {
    Box::new(
        |expr: &str| -> Result<Box<dyn Fn(&(&'a str, usize)) -> bool + 'a>> {
            if expr.starts_with("start:") {
                let cmp = expr[6..].to_string();
                if cmp.len() > 0 {
                    Ok(Box::new(move |conn: &(&'a str, usize)| -> bool {
                        conn.0.starts_with(&cmp)
                    }))
                } else {
                    bail!("'start:' must have the thing that is being compared")
                }
            } else if expr.starts_with("end:") {
                let cmp = expr[4..].to_string();
                if cmp.len() > 0 {
                    Ok(Box::new(move |conn: &(&'a str, usize)| -> bool {
                        conn.0.ends_with(&cmp)
                    }))
                } else {
                    bail!("'end:' must have the thing that is being compared")
                }
            } else if expr.starts_with("regex:") {
                let re_text = &expr[6..];
                if re_text.len() == 0 {
                    bail!("'regex:' must have an actual regular expression")
                } else {
                    let regex = regex::Regex::new(re_text)?;

                    Ok(Box::new(move |conn: &(&'a str, usize)| -> bool {
                        regex.is_match(conn.0)
                    }))
                }
            } else {
                bail!("Invalid expression '{expr}'")
            }
        },
    )
}

pub fn build_expression<
    'd,
    P: 'd,
    EB: ExpressionBuilder,
    FB: Fn(&str) -> Result<Box<dyn Fn(&P) -> bool + 'd>>,
    CB: Fn(CompareTypes, &str, &EB) -> Result<Box<dyn Fn(&P) -> bool + 'd>>,
>(
    expression: &EB,
    func_builder: &FB,
    cmp_builder: &CB,
) -> Result<Box<dyn Fn(&P) -> bool + 'd>> {
    if let Some(not_b) = expression.not()? {
        let sub: Box<dyn for<'a> Fn(&'a P) -> bool> =
            build_expression(not_b, func_builder, cmp_builder)?;
        Ok(Box::new(move |v: &P| -> bool { !sub(v) }))
    } else if let Some(and_b) = expression.and()? {
        let mut subs = vec![];
        for v in and_b {
            subs.push(build_expression(v, func_builder, cmp_builder)?);
        }
        Ok(Box::new(move |v: &P| subs.iter().all(|f| f(v))))
    } else if let Some(or_b) = expression.or()? {
        let mut subs = vec![];
        for v in or_b {
            subs.push(build_expression(v, func_builder, cmp_builder)?);
        }
        Ok(Box::new(move |v: &P| subs.iter().any(|f| f(v))))
    } else if let Some((cmp_type, var_name, vs)) = expression.as_compare()? {
        // the variable vocabulary lives with the comparator builder: it
        // is the thing that knows where a variable's value comes from,
        // so it is the thing that can say which variables exist
        cmp_builder(cmp_type, var_name, vs)
    } else if let Some(str) = expression.as_string() {
        Ok(func_builder(str)?)
    } else {
        bail!("Unable to build an expression from {:?}", expression)
    }
}

/// The connection gate's comparator builder: the comparison variables it
/// can read, and the predicates that test them.
///
/// It knows exactly one variable — `depth`, the traversal depth carried
/// alongside the connection identifier. The vocabulary lives HERE rather
/// than in `build_expression`: the builder is what knows where a
/// variable's value comes from, so it is the thing that can say which
/// variables exist. A walk-state builder over `(&THING, &WalkState)`
/// would know `depth` and `terminal` and leave this one untouched.
pub fn build_depth_cmp<'a, EB: ExpressionBuilder>()
-> Box<dyn Fn(CompareTypes, &str, &EB) -> Result<Box<dyn Fn(&(&'a str, usize)) -> bool + 'a>>> {
    /// The variables this builder can read.
    const KNOWN: &str = "depth";

    Box::new(
        |cmp: CompareTypes,
         var_name: &str,
         expression: &EB|
         -> Result<Box<dyn Fn(&(&'a str, usize)) -> bool + 'a>> {
            if var_name != KNOWN {
                bail!("unknown variable `{var_name}`; known: {KNOWN}");
            }
            let Some(cmp_value) = expression.as_int() else {
                bail!("`{KNOWN}` compares against an integer, not {expression:?}");
            };
            let cmp_value: usize = cmp_value as usize;
            match cmp {
                CompareTypes::EQ => Ok(Box::new(move |data: &(&'a str, usize)| -> bool {
                    data.1 == cmp_value
                })),
                CompareTypes::GT => Ok(Box::new(move |data: &(&'a str, usize)| -> bool {
                    data.1 > cmp_value
                })),
                CompareTypes::GTE => Ok(Box::new(move |data: &(&'a str, usize)| -> bool {
                    data.1 >= cmp_value
                })),
                CompareTypes::LT => Ok(Box::new(move |data: &(&'a str, usize)| -> bool {
                    data.1 < cmp_value
                })),
                CompareTypes::LTE => Ok(Box::new(move |data: &(&'a str, usize)| -> bool {
                    data.1 <= cmp_value
                })),
                CompareTypes::NOTEQ => Ok(Box::new(move |data: &(&'a str, usize)| -> bool {
                    data.1 != cmp_value
                })),
            }
        },
    )
}

/// Find the connections of one type for a thing — the walk's
/// `ConFinder` over the trait surface: `connections` → the named edge
/// type → its target identifiers.
///
/// The returned strings borrow from the DOCUMENT the thing reads, not
/// from any temporary the traversal creates, so they can be carried out
/// of the finder without a copy — that is what the walk's
/// `for<'b> Fn(&'b THING, &str) -> Vec<&'b str>` bound is for.
///
/// Just named edge types: the type is a map key, not an expression, so
/// there is nothing to compile and nothing to cache — the lookup is two
/// key probes and a walk over the target array. An unknown edge type and
/// a thing with no connections both find nothing.
pub fn find_connections<'a, THING: SanshoTrait<'a> + ?Sized>(
    thing: &THING,
    connection_type: &str,
) -> Vec<&'a str> {
    let Some(connections) = SanshoTrait::get_key(thing, "connections") else {
        return Vec::new();
    };
    let Some(targets) = SanshoTrait::get_key(&connections, connection_type) else {
        return Vec::new();
    };
    SanshoTrait::elements(&targets)
        .iter()
        .filter_map(|target| SanshoTrait::as_str_ref(target))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Requirement: the gate grammar's node shapes — every node kind
    // `build_expression` dispatches on is recognized by exactly one probe,
    // under both spellings. What: each compound shape is a single-entry
    // object keyed by its operator, each leaf is a bare JSON value, and a
    // probe answers Ok(None) for every shape but its own. Why:
    // `build_expression` tries the probes in a fixed order, so a probe
    // that claims the wrong shape silently compiles a gate into the wrong
    // predicate.
    //
    // LLM section: one assertion per shape and per spelling (`not`/`!`,
    // `and`/`&&`, `or`/`||`); the leaves answer only for their own JSON
    // type.
    #[test]
    fn json_gate_nodes_recognize_their_own_shapes() {
        let not_node = json!({"not": {"and": []}});
        assert!(not_node.not().expect("not()").is_some(), "the `not` node");
        assert!(
            not_node.and().expect("and()").is_none(),
            "`not` is not an `and`"
        );

        let bang = json!({"!": {"and": []}});
        assert!(bang.not().expect("not()").is_some(), "`!` is `not`");

        let and_node = json!({"and": [true, false]});
        assert_eq!(
            and_node
                .and()
                .expect("and()")
                .map(<[serde_json::Value]>::len),
            Some(2)
        );
        assert!(
            and_node.not().expect("not()").is_none(),
            "`and` is not a `not`"
        );

        let amp = json!({"&&": [true]});
        assert_eq!(
            amp.and().expect("and()").map(<[serde_json::Value]>::len),
            Some(1)
        );

        let or_node = json!({"or": [true]});
        assert_eq!(
            or_node.or().expect("or()").map(<[serde_json::Value]>::len),
            Some(1)
        );

        let pipes = json!({"||": [true]});
        assert_eq!(
            pipes.or().expect("or()").map(<[serde_json::Value]>::len),
            Some(1)
        );

        assert_eq!(json!("start:pkg:").as_string(), Some("start:pkg:"));
        assert_eq!(json!(7).as_int(), Some(7));
        assert_eq!(json!(true).as_bool(), Some(true));

        // the leaves answer only for their own JSON type
        assert!(json!("start:pkg:").as_int().is_none());
        assert!(json!(7).as_string().is_none());
        assert!(json!(7).as_bool().is_none());
    }

    // Requirement: a node naming an operator but carrying the wrong
    // payload is a malformed gate, not some other node kind. What: the
    // probe answers Err naming the operator exactly as it was written.
    // Why: the shape is known here, so this is where the useful message
    // can be constructed; `build_expression` propagates it with `?`
    // instead of falling through to "unrecognized gate node".
    //
    // LLM section: the message names the spelling the author used
    // (`&&`, not `and`).
    #[test]
    fn malformed_compound_nodes_report_their_operator() {
        let bad_and = json!({"and": "oops"});
        let error = bad_and.and().expect_err("`and` must hold an array");
        assert!(error.to_string().contains("`and`"), "{error}");

        let bad_amp = json!({"&&": 5});
        let error = bad_amp.and().expect_err("`&&` must hold an array");
        assert!(error.to_string().contains("`&&`"), "{error}");

        let bad_or = json!({"or": {}});
        let error = bad_or.or().expect_err("`or` must hold an array");
        assert!(error.to_string().contains("`or`"), "{error}");

        let bad_pipes = json!({"||": null});
        let error = bad_pipes.or().expect_err("`||` must hold an array");
        assert!(error.to_string().contains("`||`"), "{error}");
    }

    // Requirement: a comparison node carries three things — the operator,
    // the variable the comparison reads, and the value it compares
    // against. What: `{"<=": {"depth": 2}}` yields the LTE operator, the
    // variable name "depth", and the value `2` by reference, so the
    // comparator builder reads it with `as_int`. Why: the comparator
    // builder is the only place that knows which variables exist and
    // where their values come from, so the node hands the variable name
    // through unresolved.
    //
    // LLM section: the operator leads the object; each symbol and each
    // word spelling names the same comparison.
    #[test]
    fn comparison_node_carries_operator_variable_and_value() {
        let node = json!({"<=": {"depth": 2}});
        let (operator, variable, value) = node
            .as_compare()
            .expect("as_compare()")
            .expect("a comparison node");
        assert_eq!(operator, CompareTypes::LTE, "the operator");
        assert_eq!(variable, "depth", "the variable the comparison reads");
        assert_eq!(value.as_int(), Some(2), "the value it compares against");

        let comparison = |operator: &str| -> serde_json::Value {
            let mut operands = serde_json::Map::new();
            operands.insert("depth".to_string(), json!(1));
            let mut node = serde_json::Map::new();
            node.insert(operator.to_string(), serde_json::Value::Object(operands));
            serde_json::Value::Object(node)
        };
        for (spelling, expected) in [
            (">", CompareTypes::GT),
            ("gt", CompareTypes::GT),
            (">=", CompareTypes::GTE),
            ("gte", CompareTypes::GTE),
            ("=", CompareTypes::EQ),
            ("==", CompareTypes::EQ),
            ("eq", CompareTypes::EQ),
            ("!=", CompareTypes::NOTEQ),
            ("neq", CompareTypes::NOTEQ),
            ("noteq", CompareTypes::NOTEQ),
            ("<", CompareTypes::LT),
            ("lt", CompareTypes::LT),
            ("<=", CompareTypes::LTE),
            ("lte", CompareTypes::LTE),
        ] {
            let node = comparison(spelling);
            let (operator, variable, _) = node
                .as_compare()
                .expect("as_compare()")
                .unwrap_or_else(|| panic!("{spelling} must be a comparison"));
            assert_eq!(variable, "depth");
            assert_eq!(operator, expected, "{spelling} names its comparison");
        }
    }

    // Requirement: a comparison node naming a comparison operator that
    // does not carry `{"<variable>": <value>}` is a malformed gate. What:
    // non-object operands, empty operand objects, and operand objects
    // naming several variables are errors. Why: the author meant a
    // comparison, so silence would be worse than a message that says what
    // is wrong.
    //
    // LLM section: the three malformed shapes are the three ways the
    // `{"<variable>": <value>}` contract can be broken.
    #[test]
    fn malformed_comparisons_report_what_is_wrong() {
        let not_an_object = json!({"<=": 2});
        let error = not_an_object
            .as_compare()
            .expect_err("operands must be an object");
        assert!(error.to_string().contains("`<=`"), "{error}");

        let no_variable = json!({"<=": {}});
        let error = no_variable
            .as_compare()
            .expect_err("exactly one variable is required");
        assert!(
            error.to_string().contains("exactly one variable"),
            "{error}"
        );

        let two_variables = json!({"<=": {"depth": 1, "other": 2}});
        let error = two_variables
            .as_compare()
            .expect_err("exactly one variable is required");
        assert!(
            error.to_string().contains("exactly one variable"),
            "{error}"
        );
    }

    // Requirement: everything that is not a node answers Ok(None) from
    // every probe — that is what lets `build_expression` fall through to
    // its unrecognized-node error. What: multi-entry objects, unknown
    // operators, and non-objects. Why: "not this node" must stay
    // distinguishable from "malformed node" (Err), and the single-entry
    // rule is what keeps the probes unambiguous.
    //
    // LLM section: a two-entry object containing `and` is not an `and`
    // node; an unknown key is not a comparison.
    #[test]
    fn nodes_that_are_not_gate_nodes_answer_none() {
        for node in [
            json!({"and": [], "or": []}),
            json!({"unknown": {"depth": 2}}),
            json!({}),
            json!([]),
            json!("depth"),
            json!(2),
            json!(null),
        ] {
            assert!(node.not().expect("not()").is_none(), "{node}");
            assert!(node.and().expect("and()").is_none(), "{node}");
            assert!(node.or().expect("or()").is_none(), "{node}");
            assert!(node.as_compare().expect("as_compare()").is_none(), "{node}");
        }
    }

    // Requirement: `as_int` is the integer leaf of a comparison (the
    // walk's `depth`). What: integral JSON yields its value; floats —
    // including one with a zero fraction — and integers that do not fit
    // `isize` yield None. Why: depth comparisons are exact integer
    // comparisons, so a float leaf must fail loudly at the comparator
    // builder rather than compare approximately.
    #[test]
    fn int_leaf_is_integral_only() {
        assert_eq!(json!(3).as_int(), Some(3));
        assert_eq!(json!(-3).as_int(), Some(-3));
        assert_eq!(
            json!(3.0).as_int(),
            None,
            "a zero fraction is still a float"
        );
        assert_eq!(json!(3.5).as_int(), None);
        assert_eq!(json!(u64::MAX).as_int(), None, "out of isize range");
        assert_eq!(json!("3").as_int(), None, "a string is not an integer leaf");
        assert_eq!(json!(true).as_int(), None);
        assert_eq!(json!(null).as_int(), None);
    }

    // Requirement: the connection finder is what the walk's `ConFinder`
    // bound accepts — a finder whose strings borrow from the thing it was
    // handed. What: `find_connections` returns the named edge type's
    // targets for a byte-source thing and for a borrowed JSON thing, an
    // empty list for an unknown edge type, and it satisfies the walk's
    // higher-ranked bound when called with a per-item borrow. Why: the
    // walk hands the finder a borrow of the current item and expects the
    // strings to live at least as long; a finder returning temporaries
    // would force the walk to copy every target.
    //
    // LLM section: the same document is read twice — once as CBOR bytes,
    // once as a borrowed JSON value — and both find the same targets.
    #[test]
    fn connection_finder_returns_document_borrows() {
        /// The walk's connection-finder bound, spelled out: any thing,
        /// called with a per-item borrow, returning borrows that live at
        /// least that long.
        fn through_the_walk_bound<'b, THING: SanshoTrait<'b> + ?Sized, FINDER>(
            finder: FINDER,
            thing: &'b THING,
            connection_type: &str,
        ) -> Vec<&'b str>
        where
            FINDER: for<'x> Fn(&'x THING, &str) -> Vec<&'x str>,
        {
            finder(thing, connection_type)
        }

        let document = json!({
            "connections": {
                "alias:from": ["pkg:a", "gitoid:b"],
                "contained:up": ["p"]
            }
        });

        // a borrowed JSON document
        let borrowed: &serde_json::Value = &document;
        assert_eq!(
            find_connections(&borrowed, "alias:from"),
            vec!["pkg:a", "gitoid:b"]
        );

        // the same document as CBOR bytes
        let encoded = serde_cbor::to_vec(&document).unwrap();
        let bytes: &[u8] = &encoded;
        assert_eq!(
            find_connections(&bytes, "alias:from"),
            vec!["pkg:a", "gitoid:b"]
        );
        assert_eq!(find_connections(&bytes, "contained:up"), vec!["p"]);
        assert_eq!(
            find_connections(&bytes, "not-an-edge-type"),
            Vec::<&str>::new()
        );

        // and through the walk's bound, with the borrow the walk hands it
        let found = through_the_walk_bound(
            |thing: &&[u8], connection_type: &str| find_connections(thing, connection_type),
            &bytes,
            "contained:up",
        );
        assert_eq!(found, vec!["p"]);
    }

    // Requirement: the gate compiler is instantiable for the walk's
    // connection gate — a JSON gate compiled once, stored, and used later.
    // What: `build_expression` over the connection leaf builder and the
    // depth comparator builder produces a predicate that accepts and
    // rejects connections, and it lives past the scope that built it.
    // Why: the identifier borrows for the document's lifetime, so the
    // gate's data type is `(&'a str, usize)` — a fixed lifetime, which is
    // what lets a predicate over it be built once and stored; a predicate
    // over a per-call borrow could not be.
    //
    // LLM section: the gate is the walk's connection-filter shape: the
    // identifier leaves come from `build_con_compare_function_function`,
    // the depth comparison from `build_depth_cmp`.
    #[test]
    fn connection_gate_compiles_once_and_can_be_stored() {
        type ConnData<'a> = (&'a str, usize);

        let gate = json!({
            "and": [
                {"or": ["start:pkg:", "start:gitoid:"]},
                {"<=": {"depth": 2}}
            ]
        });

        // built in one scope...
        let stored: Box<dyn Fn(&ConnData<'static>) -> bool> = {
            let func_builder = build_con_compare_function_function::<'static>();
            let cmp_builder = build_depth_cmp::<'static, serde_json::Value>();
            build_expression::<ConnData<'static>, _, _, _>(&gate, &func_builder, &cmp_builder)
                .expect("the gate compiles")
        };

        // ...and used after it
        assert!(stored(&("pkg:thing", 0)), "a pkg: identifier at depth 0");
        assert!(stored(&("gitoid:abc", 2)), "a gitoid at the depth limit");
        assert!(!stored(&("other", 0)), "a different identifier");
        assert!(!stored(&("pkg:thing", 3)), "past the depth limit");
    }

    // Requirement: the variable vocabulary lives with the comparator
    // builder — the thing that knows where a variable's value comes from.
    // What: a comparison against an unknown variable is an error naming
    // the variable and the variables the builder knows; a comparison
    // against `depth` compiles to the depth test; a comparison value that
    // is not an integer leaf is an error. Why: `build_expression` cannot
    // know the vocabulary of every gate context, so one compiler serves
    // several contexts only if each comparator builder owns its own.
    //
    // LLM section: the connection builder knows `depth` only; a
    // walk-state builder over `(&THING, &WalkState)` would know `depth`
    // and `terminal`.
    #[test]
    fn comparator_builder_owns_the_variable_vocabulary() {
        let cmp_builder = build_depth_cmp::<serde_json::Value>();

        let error = cmp_builder(CompareTypes::LTE, "nonsense", &json!(2))
            .err()
            .expect("an unknown variable must be an error");
        let message = error.to_string();
        assert!(message.contains("nonsense"), "{message}");
        assert!(message.contains("depth"), "{message}");

        let depth_test = cmp_builder(CompareTypes::LTE, "depth", &json!(2))
            .expect("`depth` is a variable this builder reads");
        assert!(depth_test(&("anything", 2)), "at the limit");
        assert!(!depth_test(&("anything", 3)), "past the limit");

        let error = cmp_builder(CompareTypes::LTE, "depth", &json!("two"))
            .err()
            .expect("a non-integer comparison must be an error");
        assert!(error.to_string().contains("integer"), "{error}");
    }
}
