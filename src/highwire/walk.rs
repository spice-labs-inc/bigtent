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
    fn event(&mut self, event_type: AccountingEventType);
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
    ThingFinder: Fn(&str, &mut ACCOUNTANT) -> Result<Option<THING>>,
    // whatever is done with the materialized JSON for an Emit
    SINK: FnMut(Value, &mut ACCOUNTANT, SinkOperation) -> Result<()>,
    // for a given connection type (e.g., "connected:down" only exact lookups), return
    // the &str of each of the connections. Because the lifetime of the &str in the Vec
    // is the same as the THING, the &str's can be (and should be) zero copy
    ConFinder: for<'b> Fn(&'b THING, &str) -> Vec<&'b str>,
    // filter the conn (note this is per connection type)
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
    accountant: &mut ACCOUNTANT,
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
            let thing2: Option<THING> = lookup(&id, accountant)?;
            // get it
            if let Some(thing) = thing2 {
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
                        sink(emit_me, accountant, SinkOperation::NormalMessage)?;
                    }

                    // do stop logic
                    match stop_fn(&(&thing, &walk_state)) {
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

pub fn find_func<'a, A: AccountingAndBudget>(
    cluster: &'a Arc<GoatRodeoCluster>,
) -> impl Fn(&str, &mut A) -> Result<Option<&'a [u8]>> + 'a {
    move |id: &str, accounting: &mut A| -> Result<Option<&'a [u8]>> {
        accounting.event(AccountingEventType::LoadItem);
        let Some(io) = cluster.identifier_to_item_offset(id) else {
            return Ok(None);
        };
        Ok(cluster.bytes_for_file_and_offset(io.loc.get_file_hash(), io.loc.get_offset()))
    }
}

pub fn find_func_for_herd<A: AccountingAndBudget>(
    herd: Arc<GoatHerd>,
) -> impl Fn(&str, &mut A) -> Result<Option<Item>> {
    move |id: &str, accounting: &mut A| -> Result<Option<Item>> {
        accounting.event(AccountingEventType::LoadItem);
        Ok(herd.item_for_identifier(id))
    }
}

pub fn vec_sink<'a, A: AccountingAndBudget>(
    vec: &'a mut Vec<Value>,
) -> impl FnMut(Value, &mut A, SinkOperation) -> Result<()> {
    move |v: Value, accounting: &mut A, _: SinkOperation| -> Result<()> {
        vec.push(v);
        accounting.event(AccountingEventType::Emit);
        Ok(())
    }
}

pub fn flume_sink<A: AccountingAndBudget>(
    sender: Sender<Value>,
) -> impl FnMut(Value, &mut A, SinkOperation) -> Result<()> {
    move |v: Value, accounting: &mut A, _: SinkOperation| -> Result<()> {
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
    fn not(&self) -> Option<&Self>;
    fn and(&self) -> Option<&[Self]>
    where
        Self: Sized;
    fn or(&self) -> Option<&[Self]>
    where
        Self: Sized;
    fn as_compare(&self) -> Option<(CompareTypes, &str, &Self)>;
    fn as_string(&self) -> Option<&str>;
    fn as_int(&self) -> Option<isize>;
    fn as_bool(&self) -> Option<bool>;
}

impl ExpressionBuilder for serde_json::Value {
    fn not(&self) -> Option<&Self> {
        match self {
            serde_json::Value::Object(inner) if inner.len() == 1 => inner.get("not"),
            _ => None,
        }
    }

    fn and(&self) -> Option<&[Self]> {
        match self {
            serde_json::Value::Object(inner) if inner.len() == 1 => match inner.get("and") {
                Some(serde_json::Value::Array(arr)) => Some(arr),
                _ => None,
            },
            _ => None,
        }
    }

    fn or(&self) -> Option<&[Self]> {
        match self {
            serde_json::Value::Object(inner) if inner.len() == 1 => match inner.get("or") {
                Some(serde_json::Value::Array(arr)) => Some(arr),
                _ => None,
            },
            _ => None,
        }
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

pub enum CompareTypes {
    GT,
    GTE,
    EQ,
    NOTEQ,
    LT,
    LTE,
}

pub fn build_con_compare_function_function()
-> Box<dyn Fn(&str) -> Result<Box<dyn Fn(&(&str, usize)) -> bool>>> {
    Box::new(
        |expr: &str| -> Result<Box<dyn Fn(&(&str, usize)) -> bool>> {
            if expr.starts_with("start:") {
                let cmp = expr[6..].to_string();
                if cmp.len() > 0 {
                    Ok(Box::new(move |conn: &(&str, usize)| -> bool {
                        conn.0.starts_with(&cmp)
                    }))
                } else {
                    bail!("'start:' must have the thing that is being compared")
                }
            } else if expr.starts_with("end:") {
                let cmp = expr[4..].to_string();
                if cmp.len() > 0 {
                    Ok(Box::new(move |conn: &(&str, usize)| -> bool {
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

                    Ok(Box::new(move |conn: &(&str, usize)| -> bool {
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
    P: 'static,
    EB: ExpressionBuilder,
    FB: Fn(&str) -> Result<Box<dyn Fn(&P) -> bool>>,
    CB: Fn(CompareTypes, &str, &EB) -> Result<Box<dyn Fn(&P) -> bool>>,
>(
    expression: &EB,
    func_builder: &Box<FB>,
    cmp_builder: &Box<CB>,
    valid_vars: &HashSet<&str>,
) -> Result<Box<dyn Fn(&P) -> bool>> {
    if let Some(not_b) = expression.not() {
        let sub: Box<dyn Fn(&P) -> bool> =
            build_expression(not_b, func_builder, cmp_builder, valid_vars)?;
        Ok(Box::new(move |v: &P| -> bool { !sub(v) }))
    } else if let Some(and_b) = expression.and() {
        let mut subs = vec![];
        for v in and_b {
            subs.push(build_expression(v, func_builder, cmp_builder, valid_vars)?);
        }
        Ok(Box::new(move |v: &P| subs.iter().all(|f| f(v))))
    } else if let Some(or_b) = expression.or() {
        let mut subs = vec![];
        for v in or_b {
            subs.push(build_expression(v, func_builder, cmp_builder, valid_vars)?);
        }
        Ok(Box::new(move |v: &P| subs.iter().any(|f| f(v))))
    } else if let Some((cmp_type, var_name, vs)) = expression.as_compare() {
        if !valid_vars.contains(var_name) {
            bail!("No variable '{var_name}' in scope at {:?}", expression);
        }
        Ok(cmp_builder(cmp_type, var_name, vs)?)
    } else if let Some(str) = expression.as_string() {
        Ok(func_builder(str)?)
    } else {
        bail!("Unable to build an expression from {:?}", expression)
    }
}

pub fn build_depth_cmp<EB: ExpressionBuilder>()
-> Box<dyn Fn(CompareTypes, &str, &EB) -> Result<Box<dyn Fn(&(&str, usize)) -> bool>>> {
    Box::new(
        |cmp: CompareTypes,
         var_name: &str,
         expression: &EB|
         -> Result<Box<dyn Fn(&(&str, usize)) -> bool>> {
            if var_name != "depth" {
                bail!("Illegal var name {var_name}");
            }

            if let Some(cmp_value) = expression.as_int() {
                let cmp_value: usize = cmp_value as usize;
                match cmp {
                    CompareTypes::EQ => Ok(Box::new(move |data: &(&str, usize)| -> bool {
                        data.1 == cmp_value
                    })),
                    CompareTypes::GT => Ok(Box::new(move |data: &(&str, usize)| -> bool {
                        data.1 > cmp_value
                    })),
                    CompareTypes::GTE => Ok(Box::new(move |data: &(&str, usize)| -> bool {
                        data.1 >= cmp_value
                    })),
                    CompareTypes::LT => Ok(Box::new(move |data: &(&str, usize)| -> bool {
                        data.1 < cmp_value
                    })),
                    CompareTypes::LTE => Ok(Box::new(move |data: &(&str, usize)| -> bool {
                        data.1 <= cmp_value
                    })),
                    CompareTypes::NOTEQ => Ok(Box::new(move |data: &(&str, usize)| -> bool {
                        data.1 != cmp_value
                    })),
                }
            } else {
                bail!("Must compare to an int, not {:?}", expression);
            }
        },
    )
}
