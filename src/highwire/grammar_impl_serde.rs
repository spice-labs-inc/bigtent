//! The walk grammar, as JSON expresses it.
//!
//! Every trait in [`grammar`](crate::highwire::grammar) implemented for
//! `serde_json::Value`. This file is the JSON READING of the grammar and
//! nothing else: the shapes it recognizes — objects with named keys, arrays,
//! strings, the single-entry object that names a compound node — are facts
//! about JSON, and every one of them lives here rather than on a trait, because
//! a data structure that represents a compound node as a tagged enum has no
//! keys to look up and no single-entry rule to apply.
//!
//! # Where a failure is
//!
//! A reader is handed a node and nothing else, so it cannot know which phase the
//! node belongs to or which entry led to it. What it CAN know is what it was
//! reading, and that is the [`Step`] it reports. Each caller one step up
//! prepends its own, so a failure comes out of the tree located:
//!
//! ```text
//! the query -> phase `classes` -> its emit -> entry 1 -> the gate: `and` takes an array …
//! ```
//!
//! # The document shapes
//!
//! ```text
//! a query      { "phases": [ <walk>, ... ] }        or a walk, itself
//! a walk       { "name"?, "roots", "walk"?, "filter"?, "emit", "stop"?,
//!                "tee"?, "emit_at_end"? }
//! roots        [ "<identifier>", ... ]              or { "<phase>": "<sansho>" }
//! walk         "<connection type>"                  or [ <connection>, ... ]
//! connection   "<connection type>"                  or { "<type>": <gate> }
//! filter       <gate>                               or { "filter": <gate> }
//! emit         <spec>                               or { "emit": <spec>, "if"?, "unnest"? }
//! spec         "<sansho expression>"
//!              | { "expr": "<sansho>", "if"?, "unnest"?, "emit_null"? }
//!              | [ <spec>, ... ]
//! stop         { "hard"?: <rules>, "soft"?: <rules> }
//! rules        <rule>                               or [ <rule>, ... ]
//! rule         <gate>                               or { "if": <gate>, "emit"?: <spec> }
//! gate         "<sansho expression>"
//!              | { "not"|"!"|"and"|"&&"|"or"|"||": <gate or [<gate>]> }
//!              | { "<comparison operator>": { "<variable>": <value> } }
//! ```
//!
//! The wrapper forms are OPTIONAL wherever a level's only content is its body.
//! `{"filter": <gate>}` and `<gate>` mean the same thing, and so do
//! `{"emit": <spec>}` and `<spec>`. A wrapper exists to carry the level's other
//! keys — `if`, `unnest` — and when there are none there is nothing to wrap.
//!
//! # Ownership
//!
//! Every level hands its sub-levels back as `Rc<dyn _>`, and a `serde_json`
//! document does not share its sub-values: a node under a key is a part of the
//! document, not a handle on it. So a sub-level is CLONED into its `Rc`. That is
//! the cost of reading a JSON document through this grammar, and it is paid once
//! per level rather than per item — a data structure that shares its sub-nodes,
//! or holds them behind an `Rc`, would not pay it at all.

use crate::highwire::grammar::{
    Barker, EmitEntry, GrammarError, ItemEmit, ItemFilter, ItemStop, LevelResult, Query, Root,
    RootKind, RootPredicate, SanshoExpression, Stage, StageKind, Step, StopRule, Walk,
    WalkConnection,
};
use crate::highwire::walk::{
    CompareTypes, ConFilter, ExpressionBuilder, WalkBuilder, build_con_compare_function,
    build_expression, build_walk_for_str, depth_cmp,
};
use std::rc::Rc;

/// The single entry of a single-entry object, as JSON represents a compound
/// node: `{"and": …}`, `{"not": …}`, `{">": …}`.
///
/// `None` for anything else — a non-object, an empty object, an object with
/// several entries — because the single-entry rule is what keeps the probes
/// unambiguous: without it, `{"and": […], "or": [… ]}` could be read as either.
fn single_entry(value: &serde_json::Value) -> Option<(&str, &serde_json::Value)> {
    let serde_json::Value::Object(entries) = value else {
        return None;
    };
    if entries.len() != 1 {
        return None;
    }
    entries
        .iter()
        .next()
        .map(|(key, value)| (key.as_str(), value))
}

/// The comparison an operator name denotes — the symbol forms and their word
/// spellings. `None` for a name that is not a comparison at all.
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

/// The gate grammar's JSON encoding.
///
/// Every compound node is a single-entry object keyed by its operator —
/// `{"not": …}` / `{"!": …}`, `{"and": […]}` / `{"&&": […]}`,
/// `{"or": […]}` / `{"||": […]}` — and a comparison is
/// `{"<op>": {"<variable>": <value>}}`.
///
/// The probes separate three answers: `Ok(Some(_))` — this is that node;
/// `Ok(None)` — this is not that node; `Err(_)` — this names that operator but
/// its payload is malformed. The error is constructed here because here is
/// where the shape is known.
impl ExpressionBuilder for serde_json::Value {
    fn not(&self) -> LevelResult<Option<Rc<dyn ExpressionBuilder>>> {
        let Some((operator, operand)) = single_entry(self) else {
            return Ok(None);
        };
        match operator {
            "not" | "!" => Ok(Some(Rc::new(operand.clone()))),
            _ => Ok(None),
        }
    }

    fn and(&self) -> LevelResult<Option<Vec<Rc<dyn ExpressionBuilder>>>> {
        let Some((operator, operand)) = single_entry(self) else {
            return Ok(None);
        };
        if operator != "and" && operator != "&&" {
            return Ok(None);
        }
        match operand {
            serde_json::Value::Array(expressions) => Ok(Some(
                expressions
                    .iter()
                    .map(|v| Rc::new(v.clone()) as Rc<dyn ExpressionBuilder>)
                    .collect(),
            )),
            other => Err(GrammarError::at(
                Step::Gate,
                format!("`{operator}` takes an array of gate expressions, found {other}"),
            )),
        }
    }

    fn or(&self) -> LevelResult<Option<Vec<Rc<dyn ExpressionBuilder>>>> {
        let Some((operator, operand)) = single_entry(self) else {
            return Ok(None);
        };
        if operator != "or" && operator != "||" {
            return Ok(None);
        }
        match operand {
            serde_json::Value::Array(expressions) => Ok(Some(
                expressions
                    .iter()
                    .map(|v| Rc::new(v.clone()) as Rc<dyn ExpressionBuilder>)
                    .collect(),
            )),
            other => Err(GrammarError::at(
                Step::Gate,
                format!("`{operator}` takes an array of gate expressions, found {other}"),
            )),
        }
    }

    /// A comparison node: `{"<op>": {"<variable>": <value>}}` — for example
    /// `{"<=": {"depth": 2}}`.
    ///
    /// The operator leads the object, parallel to `{"and": […]}` and
    /// `{"not": {…}}`. Its single value is the single-entry object naming the
    /// variable the comparison reads and the value it compares against, and the
    /// returned node is that value — so the comparator builder reads it through
    /// `as_int` / `as_bool`. The variable vocabulary is NOT closed here: the
    /// comparator builder is what knows which variables exist and where their
    /// values come from.
    fn as_compare(
        &self,
    ) -> LevelResult<Option<(CompareTypes, &str, Rc<dyn ExpressionBuilder>)>> {
        let Some((operator, operands)) = single_entry(self) else {
            return Ok(None);
        };
        let Some(compare_type) = comparison_of(operator) else {
            return Ok(None);
        };
        let serde_json::Value::Object(operands) = operands else {
            return Err(GrammarError::at(
                Step::Gate,
                format!("`{operator}` takes {{\"<variable>\": <value>}}, found {operands}"),
            ));
        };
        if operands.len() != 1 {
            return Err(GrammarError::at(
                Step::Gate,
                format!(
                    "`{operator}` reads exactly one variable, found {} in {operands:?}",
                    operands.len()
                ),
            ));
        }
        let (variable, value) = operands.iter().next().expect("exactly one entry");
        Ok(Some((
            compare_type,
            variable.as_str(),
            Rc::new(value.clone()),
        )))
    }

    /// An integer leaf: the right-hand side of an integer comparison (the
    /// walk's `depth`). Integral JSON only — a float, even one with a zero
    /// fraction, is not an integer leaf; the comparator builder reports the type
    /// error.
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

/// The walk list, as JSON expresses it: `{"walk": <connection type or list>}`.
///
/// This is the COMPILED reading — it hands back connection predicates rather
/// than syntax — and it predates the grammar's own levels. It is kept because
/// callers use it; [`Walk::connections`] is the syntax-level reading of the same
/// thing.
impl WalkBuilder for serde_json::Value {
    fn walk<'a, 'b>(&self) -> LevelResult<Vec<(String, ConFilter)>> {
        // the builders below work on syntax that has already been read, so they
        // have no route of their own; the walk list is where this failure is
        let built = |error: anyhow::Error| GrammarError::at(Step::Connections, error.to_string());

        match self {
            serde_json::Value::Object(obj) => match obj.get("walk") {
                Some(serde_json::Value::String(s)) => Ok(vec![build_walk_for_str(s).map_err(built)?]),
                Some(serde_json::Value::Array(arr)) => {
                    let mut ret = vec![];
                    for v in arr {
                        match v {
                            serde_json::Value::String(s) => ret.push(build_walk_for_str(s).map_err(built)?),
                            serde_json::Value::Object(obj) if obj.len() == 1 => {
                                for (k, v) in obj {
                                    let e = build_expression(
                                        Rc::new(v.clone()),
                                        &build_con_compare_function,
                                        &depth_cmp,
                                    )
                                    .map_err(built)?;
                                    ret.push((k.to_string(), e));
                                }
                            }
                            it => {
                                return Err(GrammarError::at(
                                    Step::Connections,
                                    format!("Subwalk failed for {it:?}"),
                                ));
                            }
                        }
                    }
                    Ok(ret)
                }

                it => Err(GrammarError::at(
                    Step::Connections,
                    format!("Walk doesn't have a type we understand {it:?}"),
                )),
            },
            _ => Err(GrammarError::at(
                Step::Connections,
                "Must be testing an object",
            )),
        }
    }
}

/// The query level, as JSON expresses it.
///
/// A document with `phases` is a chain; a document without is one walk, and it
/// is itself. That is the whole difference between the simple and the complex
/// form, and it is settled here so that nothing downstream has to know which one
/// it was given.
impl Query for serde_json::Value {
    fn phases(&self) -> LevelResult<Vec<Rc<dyn Stage>>> {
        match self.get("phases") {
            Some(serde_json::Value::Array(phases)) => Ok(phases
                .iter()
                .map(|phase| Rc::new(phase.clone()) as Rc<dyn Stage>)
                .collect()),
            Some(other) => Err(GrammarError::at(
                Step::Query,
                format!("`phases` is an array of walks, found {other}"),
            )),
            None => Ok(vec![Rc::new(self.clone())]),
        }
    }
}

/// A stage, as JSON expresses it.
///
/// The selector is the `barker` key: a document that names a program is a
/// program, anything else is a walk. That keeps every existing document valid
/// unchanged — a walk is still a walk — and gives a program the one shape it
/// needs, `{"barker": "<source>", "inputs"?: [...]}`.
impl Stage for serde_json::Value {
    fn kind(&self) -> LevelResult<StageKind> {
        if self.get("barker").is_some() {
            Ok(StageKind::Barker)
        } else {
            Ok(StageKind::Walk)
        }
    }

    fn name(&self) -> LevelResult<Option<String>> {
        Walk::name(self)
    }

    fn walk(&self) -> LevelResult<Rc<dyn Walk>> {
        if self.get("barker").is_some() {
            return Err(GrammarError::at(
                Step::Query,
                "this stage names a program; it is not a walk",
            ));
        }
        Ok(Rc::new(self.clone()))
    }

    fn barker(&self) -> LevelResult<Rc<dyn Barker>> {
        if self.get("barker").is_none() {
            return Err(GrammarError::at(
                Step::Query,
                "this stage is a walk; it names no program",
            ));
        }
        Ok(Rc::new(self.clone()))
    }
}

/// A program, as JSON expresses it: `{"barker": "<source>", "inputs"?: [...]}`.
impl Barker for serde_json::Value {
    fn inputs(&self) -> LevelResult<Vec<String>> {
        match self.get("inputs") {
            Some(serde_json::Value::Array(inputs)) => {
                let mut names = Vec::with_capacity(inputs.len());
                for input in inputs {
                    let Some(name) = input.as_str() else {
                        return Err(GrammarError::at(
                            Step::Query,
                            format!("every input is the name of a stage, found {input}"),
                        ));
                    };
                    names.push(name.to_string());
                }
                Ok(names)
            }
            Some(other) => Err(GrammarError::at(
                Step::Query,
                format!("`inputs` is a list of stage names, found {other}"),
            )),
            None => Ok(Vec::new()),
        }
    }

    fn tee(&self) -> LevelResult<bool> {
        match self.get("tee") {
            Some(serde_json::Value::Bool(tee)) => Ok(*tee),
            Some(other) => Err(GrammarError::at(
                Step::Tee,
                format!("`tee` is a boolean, found {other}"),
            )),
            None => Ok(false),
        }
    }

    fn unnest(&self) -> LevelResult<bool> {
        match self.get("unnest") {
            Some(serde_json::Value::Bool(unnest)) => Ok(*unnest),
            Some(other) => Err(GrammarError::at(
                Step::Emit,
                format!("`unnest` is a boolean, found {other}"),
            )),
            None => Ok(false),
        }
    }

    fn dedup(&self) -> LevelResult<bool> {
        match self.get("dedup") {
            Some(serde_json::Value::Bool(dedup)) => Ok(*dedup),
            Some(other) => Err(GrammarError::at(
                Step::Emit,
                format!("`dedup` is a boolean, found {other}"),
            )),
            None => Ok(false),
        }
    }

    fn program(&self) -> LevelResult<&str> {
        match self.get("barker") {
            Some(serde_json::Value::String(source)) => Ok(source.as_str()),
            Some(other) => Err(GrammarError::at(
                Step::Query,
                format!("a program is written as a string, found {other}"),
            )),
            None => Err(GrammarError::at(
                Step::Query,
                "this stage is a walk; it names no program",
            )),
        }
    }
}

/// A walk, as JSON expresses it.
///
/// The name is optional and only a chain needs it: it is how a phase downstream
/// refers to the phase it draws on.
impl Walk for serde_json::Value {
    fn name(&self) -> LevelResult<Option<String>> {
        match self.get("name") {
            Some(serde_json::Value::String(name)) => Ok(Some(name.clone())),
            Some(other) => Err(GrammarError::at(
                Step::Name,
                format!("a phase's name is a string, found {other}"),
            )),
            None => Ok(None),
        }
    }

    fn root(&self) -> LevelResult<Option<Rc<dyn Root>>> {
        // `roots` is one key with three forms: a list of identifiers, an object
        // naming the stages this walk derives them from, or `{"from-input": true}`
        // for the caller's input stream. The absence means the walk has no roots,
        // which only the compiler can judge.
        Ok(self.get("roots").map(|roots| Rc::new(roots.clone()) as Rc<dyn Root>))
    }

    fn connections(&self) -> LevelResult<Vec<Rc<dyn WalkConnection>>> {
        match self.get("walk") {
            Some(serde_json::Value::String(connection_type)) => Ok(vec![Rc::new(
                serde_json::Value::String(connection_type.clone()),
            )]),
            Some(serde_json::Value::Array(connections)) => Ok(connections
                .iter()
                .map(|connection| Rc::new(connection.clone()) as Rc<dyn WalkConnection>)
                .collect()),
            Some(other) => Err(GrammarError::at(
                Step::Connections,
                format!("`walk` is a connection type or a list of them, found {other}"),
            )),
            None => Ok(Vec::new()),
        }
    }

    fn filter(&self) -> LevelResult<Option<Rc<dyn ItemFilter>>> {
        Ok(self
            .get("filter")
            .map(|filter| Rc::new(filter.clone()) as Rc<dyn ItemFilter>))
    }

    fn emit(&self) -> LevelResult<Rc<dyn ItemEmit>> {
        let Some(emit) = self.get("emit") else {
            return Err(GrammarError::at(
                Step::Emit,
                "a walk emits something, and there is no `emit` here",
            ));
        };
        Ok(Rc::new(emit.clone()))
    }

    fn stop(&self) -> LevelResult<Option<Rc<dyn ItemStop>>> {
        Ok(self
            .get("stop")
            .map(|stop| Rc::new(stop.clone()) as Rc<dyn ItemStop>))
    }

    fn tee(&self) -> LevelResult<bool> {
        match self.get("tee") {
            Some(serde_json::Value::Bool(tee)) => Ok(*tee),
            Some(other) => Err(GrammarError::at(
                Step::Tee,
                format!("`tee` is a boolean, found {other}"),
            )),
            None => Ok(false),
        }
    }

    fn dedup(&self) -> LevelResult<bool> {
        match self.get("dedup") {
            Some(serde_json::Value::Bool(dedup)) => Ok(*dedup),
            Some(other) => Err(GrammarError::at(
                Step::Emit,
                format!("`dedup` is a boolean, found {other}"),
            )),
            None => Ok(false),
        }
    }

    fn emit_at_end(&self) -> LevelResult<Option<Rc<dyn ItemEmit>>> {
        Ok(self
            .get("emit_at_end")
            .map(|emit| Rc::new(emit.clone()) as Rc<dyn ItemEmit>))
    }
}

/// A connection, as JSON expresses it: a bare connection type, or a
/// single-entry object naming one and giving it a gate.
impl WalkConnection for serde_json::Value {
    fn connection_type(&self) -> LevelResult<String> {
        match self {
            serde_json::Value::String(connection_type) => Ok(connection_type.clone()),
            serde_json::Value::Object(_) => {
                let Some((connection_type, _)) = single_entry(self) else {
                    return Err(GrammarError::at(
                        Step::Connections,
                        format!(
                            "a gated connection is exactly {{\"<connection type>\": <gate>}}, \
                             found {self}"
                        ),
                    ));
                };
                Ok(connection_type.to_string())
            }
            other => Err(GrammarError::at(
                Step::Connections,
                format!(
                    "a connection is a connection type or a single-entry object naming one, \
                     found {other}"
                ),
            )),
        }
    }

    fn gate(&self) -> LevelResult<Option<Rc<dyn ExpressionBuilder>>> {
        match self {
            serde_json::Value::String(_) => Ok(None),
            serde_json::Value::Object(_) => {
                let Some((_, gate)) = single_entry(self) else {
                    return Err(GrammarError::at(
                        Step::ConnectionGate,
                        format!(
                            "a gated connection is exactly {{\"<connection type>\": <gate>}}, \
                             found {self}"
                        ),
                    ));
                };
                Ok(Some(Rc::new(gate.clone())))
            }
            other => Err(GrammarError::at(
                Step::ConnectionGate,
                format!(
                    "a connection is a connection type or a single-entry object naming one, \
                     found {other}"
                ),
            )),
        }
    }
}

/// A root, as JSON expresses it: a list of identifiers, or an object naming the
/// phases it draws them from.
///
/// The two are told apart by their JSON type, which is the whole of the
/// selector: a list is enumerated, an object is predicates, and anything else is
/// neither.
impl Root for serde_json::Value {
    fn kind(&self) -> LevelResult<RootKind> {
        match self {
            serde_json::Value::Array(_) => Ok(RootKind::Enumerated),
            // `{"from-input": true}` is the caller's input stream; any other object
            // names the stages this walk derives its roots from. The from-input form
            // is from-input ALONE — a mixed object would leave its extra keys
            // silently unread, so it is refused here where the mistake is visible.
            serde_json::Value::Object(entries) => {
                if entries.get("from-input") == Some(&serde_json::Value::Bool(true)) {
                    if entries.len() > 1 {
                        return Err(GrammarError::at(
                            Step::Root,
                            format!(
                                "`{{\"from-input\": true}}` is the whole root; the extra \
                                 keys are unread: {entries:?}"
                            ),
                        ));
                    }
                    Ok(RootKind::FromInput)
                } else {
                    Ok(RootKind::Predicates)
                }
            }
            other => Err(GrammarError::at(
                Step::Root,
                format!(
                    "a root is a list of identifiers or an object naming the phases it draws \
                     on, found {other}"
                ),
            )),
        }
    }

    fn identifiers(&self) -> LevelResult<Vec<String>> {
        let serde_json::Value::Array(items) = self else {
            return Err(GrammarError::at(
                Step::Root,
                "this root derives its identifiers; it does not name them",
            ));
        };
        let mut identifiers = Vec::with_capacity(items.len());
        for item in items {
            let Some(identifier) = item.as_str() else {
                return Err(GrammarError::at(
                    Step::Root,
                    format!("every root is a string identifier, found {item}"),
                ));
            };
            identifiers.push(identifier.to_string());
        }
        Ok(identifiers)
    }

    fn predicates(&self) -> LevelResult<Vec<Rc<dyn RootPredicate>>> {
        let serde_json::Value::Object(entries) = self else {
            return Err(GrammarError::at(
                Step::Root,
                "this root names its identifiers; it has no predicates",
            ));
        };
        Ok(entries
            .iter()
            .map(|(source, expression)| {
                // each predicate is its own single-entry object, because a
                // predicate reads ONE phase's rows
                let predicate = serde_json::Value::Object(
                    [(source.clone(), expression.clone())].into_iter().collect(),
                );
                Rc::new(predicate) as Rc<dyn RootPredicate>
            })
            .collect())
    }
}

/// A predicate, as JSON expresses it: a single-entry object naming the phase it
/// reads and the expression it applies to that phase's rows.
impl RootPredicate for serde_json::Value {
    fn source(&self) -> LevelResult<String> {
        let Some((source, _)) = single_entry(self) else {
            return Err(GrammarError::at(
                Step::Root,
                format!(
                    "a predicate is exactly {{\"<phase>\": \"<expression>\"}}, found {self}"
                ),
            ));
        };
        Ok(source.to_string())
    }

    fn expression(&self) -> LevelResult<Rc<dyn SanshoExpression>> {
        let Some((_, expression)) = single_entry(self) else {
            return Err(GrammarError::at(
                Step::Root,
                format!(
                    "a predicate is exactly {{\"<phase>\": \"<expression>\"}}, found {self}"
                ),
            ));
        };
        Ok(Rc::new(expression.clone()))
    }
}

/// A sansho expression, as JSON expresses it: a string.
impl SanshoExpression for serde_json::Value {
    fn source(&self) -> LevelResult<&str> {
        let serde_json::Value::String(expression) = self else {
            return Err(GrammarError::at(
                Step::Expression,
                format!("a sansho expression is written as a string, found {self}"),
            ));
        };
        Ok(expression.as_str())
    }
}

/// The item filter level, as JSON expresses it.
///
/// The wrapper is optional: `{"filter": <gate>}` names the level, and a bare
/// gate is the filter itself. Both readings are the same gate.
impl ItemFilter for serde_json::Value {
    fn gate(&self) -> LevelResult<Rc<dyn ExpressionBuilder>> {
        if let Some((name, gate)) = single_entry(self)
            && name == "filter"
        {
            return Ok(Rc::new(gate.clone()));
        }
        Ok(Rc::new(self.clone()))
    }
}

/// The emit level, as JSON expresses it.
///
/// A value with an `emit` key is the WRAPPER: its `emit` is the spec, its `if`
/// gates the whole emit, and its `unnest` is the default the entries inherit. A
/// value without one IS the spec, which is why a bare `"identifier"` is a
/// complete emit.
impl ItemEmit for serde_json::Value {
    fn gate(&self) -> LevelResult<Option<Rc<dyn ExpressionBuilder>>> {
        if self.get("emit").is_none() {
            return Ok(None);
        }
        Ok(self
            .get("if")
            .map(|gate| Rc::new(gate.clone()) as Rc<dyn ExpressionBuilder>))
    }

    fn unnest(&self) -> LevelResult<bool> {
        if self.get("emit").is_none() {
            // a bare spec: the entries carry their own row rules
            return Ok(false);
        }
        match self.get("unnest") {
            Some(serde_json::Value::Bool(unnest)) => Ok(*unnest),
            Some(other) => Err(GrammarError::at(
                Step::Emit,
                format!("`unnest` is a boolean, found {other}"),
            )),
            None => Ok(false),
        }
    }

    fn entries(&self) -> LevelResult<Vec<Rc<dyn EmitEntry>>> {
        match self.get("emit") {
            Some(spec) => Ok(spec_entries(spec)),
            None => Ok(spec_entries(self)),
        }
    }
}

/// The entries an emit spec names.
///
/// An array IS the list of entries; anything else — a string, an object — is one
/// entry. This is the same rule at the level and inside a stop rule's report,
/// which is why it is one function: a report is a spec, not a level, and reading
/// it as a level would wrap its array in an entry of its own.
fn spec_entries(spec: &serde_json::Value) -> Vec<Rc<dyn EmitEntry>> {
    match spec {
        serde_json::Value::Array(entries) => entries
            .iter()
            .map(|entry| Rc::new(entry.clone()) as Rc<dyn EmitEntry>)
            .collect(),
        entry => vec![Rc::new(entry.clone())],
    }
}

/// An emit entry, as JSON expresses it: a sansho expression, or an object
/// carrying one under `expr` along with its own gate and row rules.
impl EmitEntry for serde_json::Value {
    fn expression(&self) -> LevelResult<&str> {
        match self {
            serde_json::Value::String(expression) => Ok(expression.as_str()),
            serde_json::Value::Object(_) => {
                let Some(serde_json::Value::String(expression)) = self.get("expr") else {
                    return Err(GrammarError::at(
                        Step::Expression,
                        format!("an emit entry names its expression under `expr`, found {self}"),
                    ));
                };
                Ok(expression.as_str())
            }
            other => Err(GrammarError::at(
                Step::Expression,
                format!("an emit entry is a string or an object with `expr`, found {other}"),
            )),
        }
    }

    fn gate(&self) -> LevelResult<Option<Rc<dyn ExpressionBuilder>>> {
        Ok(self
            .get("if")
            .map(|gate| Rc::new(gate.clone()) as Rc<dyn ExpressionBuilder>))
    }

    fn unnest(&self) -> LevelResult<Option<bool>> {
        match self.get("unnest") {
            Some(serde_json::Value::Bool(unnest)) => Ok(Some(*unnest)),
            Some(other) => Err(GrammarError::at(
                Step::Emit,
                format!("`unnest` is a boolean, found {other}"),
            )),
            None => Ok(None),
        }
    }

    fn emit_null(&self) -> LevelResult<Option<bool>> {
        match self.get("emit_null") {
            Some(serde_json::Value::Bool(emit_null)) => Ok(Some(*emit_null)),
            Some(other) => Err(GrammarError::at(
                Step::Emit,
                format!("`emit_null` is a boolean, found {other}"),
            )),
            None => Ok(None),
        }
    }
}

/// The stop level, as JSON expresses it: an object holding the two rule lists.
impl ItemStop for serde_json::Value {
    fn hard(&self) -> LevelResult<Vec<Rc<dyn StopRule>>> {
        stop_rules(self, "hard")
    }

    fn soft(&self) -> LevelResult<Vec<Rc<dyn StopRule>>> {
        stop_rules(self, "soft")
    }
}

/// One rule list. Absent is empty, and an empty list never fires.
fn stop_rules(stop: &serde_json::Value, key: &str) -> LevelResult<Vec<Rc<dyn StopRule>>> {
    let Some(list) = stop.get(key) else {
        return Ok(Vec::new());
    };
    match list {
        serde_json::Value::Array(rules) => Ok(rules
            .iter()
            .map(|rule| Rc::new(rule.clone()) as Rc<dyn StopRule>)
            .collect()),
        rule => Ok(vec![Rc::new(rule.clone())]),
    }
}

/// A stop rule, as JSON expresses it: a gate, or an object carrying one under
/// `if` along with what it reports under `emit`.
///
/// A rule with no gate is a failure, not a rule that always fires: a rule that
/// always fires would end every walk immediately, which is never what was meant.
impl StopRule for serde_json::Value {
    fn gate(&self) -> LevelResult<Rc<dyn ExpressionBuilder>> {
        match self {
            serde_json::Value::String(_) => Ok(Rc::new(self.clone())),
            serde_json::Value::Object(_) => {
                let Some(gate) = self.get("if") else {
                    return Err(GrammarError::at(
                        Step::RuleGate,
                        format!("a stop rule names the gate that fires it under `if`, found {self}"),
                    ));
                };
                Ok(Rc::new(gate.clone()))
            }
            other => Err(GrammarError::at(
                Step::RuleGate,
                format!("a stop rule is a gate or an object with `if`, found {other}"),
            )),
        }
    }

    fn report(&self) -> LevelResult<Option<Rc<dyn ItemEmit>>> {
        Ok(self
            .get("emit")
            .map(|emit| Rc::new(emit.clone()) as Rc<dyn ItemEmit>))
    }
}
