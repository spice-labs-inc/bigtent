//! The `highwire` namespace: building and running a query from inside a script.
//!
//! The principle is **structure is a literal, cross-references are values**.
//!
//! The levels that are trees — `filter`, `emit`, `stop`, a connection's gate —
//! are written as they are written in a query document, because a document's
//! shape is already Starlark's. The levels that refer to another stage are built
//! by value instead, so a stage is never named by a string and "no stage is named
//! that" cannot happen.
//!
//! What a builder returns is a **stage value**: an opaque handle carrying the
//! stage's document. [`highwire::run`] collects them, renders the document, and
//! hands it to the same compiler the JSON reader feeds — so a query built here and
//! one parsed from a document are the same query, and there is no second grammar.
//!
//! `run` STARTS EAGERLY: it compiles the document and starts the graph's stages
//! before returning, and the rows are then pulled one at a time from the running
//! graph — so a nested query feeds its parent rather than being materialised.
//! A script that builds a query and never reads it still pays for the work that
//! was started.

use crate::barker::{Context, Rows};
use starlark::any::ProvidesStaticType;
use starlark::environment::GlobalsBuilder;
use starlark::starlark_module;
use starlark::values::{NoSerialize, StarlarkValue, Trace, Tracer, Value, ValueLike, starlark_value};

/// A stage, as a script holds it: the document it renders to.
///
/// Two things, because a stage needs both: the document that becomes one entry of
/// `phases`, and the name other stages refer to it by. The name is carried here
/// rather than read back out of the document so that a reference by value cannot
/// disagree with what the document says.
#[derive(Debug, ProvidesStaticType, NoSerialize)]
pub struct Stage {
    pub(crate) name: String,
    pub(crate) document: serde_json::Value,
}

// The document holds no Starlark values, so there is nothing for the collector to
// trace and nothing the heap allocates.
impl allocative::Allocative for Stage {
    fn visit<'a, 'b: 'a>(&self, _visitor: &'a mut allocative::Visitor<'b>) {}
}
unsafe impl<'v> Trace<'v> for Stage {
    fn trace(&mut self, _tracer: &Tracer<'v>) {}
}
starlark::starlark_simple_value!(Stage);

#[starlark_value(type = "stage")]
impl<'v> StarlarkValue<'v> for Stage {}

impl std::fmt::Display for Stage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stage({})", self.name)
    }
}

/// A root that derives its identifiers from another stage's rows.
#[derive(Debug, ProvidesStaticType, NoSerialize)]
pub struct FromStage {
    name: String,
    expression: String,
}

impl allocative::Allocative for FromStage {
    fn visit<'a, 'b: 'a>(&self, _visitor: &'a mut allocative::Visitor<'b>) {}
}
unsafe impl<'v> Trace<'v> for FromStage {
    fn trace(&mut self, _tracer: &Tracer<'v>) {}
}
starlark::starlark_simple_value!(FromStage);

#[starlark_value(type = "from_stage")]
impl<'v> StarlarkValue<'v> for FromStage {}

impl std::fmt::Display for FromStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "from_stage({})", self.name)
    }
}

/// A connection type and the gate that admits an edge of it.
#[derive(Debug, ProvidesStaticType, NoSerialize)]
pub struct Connection {
    connection_type: String,
    gate: Option<String>,
}

impl allocative::Allocative for Connection {
    fn visit<'a, 'b: 'a>(&self, _visitor: &'a mut allocative::Visitor<'b>) {}
}
unsafe impl<'v> Trace<'v> for Connection {
    fn trace(&mut self, _tracer: &Tracer<'v>) {}
}
starlark::starlark_simple_value!(Connection);

#[starlark_value(type = "connection")]
impl<'v> StarlarkValue<'v> for Connection {}

impl std::fmt::Display for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.connection_type)
    }
}

/// A Starlark string as owned text.
fn text(value: Value, what: &str) -> anyhow::Result<String> {
    value
        .unpack_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("`{what}` is a string, found `{}`", value.get_type()))
}

/// An optional Starlark string.
fn optional_text(value: Option<Value>, what: &str) -> anyhow::Result<Option<String>> {
    match value {
        None => Ok(None),
        Some(value) if value.is_none() => Ok(None),
        Some(value) => Ok(Some(text(value, what)?)),
    }
}

/// A Starlark JSON-ish value as `serde_json`.
fn json(value: Value) -> anyhow::Result<serde_json::Value> {
    value
        .to_json_value()
        .map_err(|e| anyhow::anyhow!("{e}"))
}

/// The document a list of roots renders to.
///
/// A list of strings is enumerated; a list of `from_stage` values is derived. The
/// two cannot be mixed, and that is checked here rather than by the compiler
/// later, because the mistake is in the script.
fn roots_document(roots: Value) -> anyhow::Result<serde_json::Value> {
    let Some(list) = starlark::values::list::ListRef::from_value(roots) else {
        anyhow::bail!("`roots` is a list, found `{}`", roots.get_type());
    };
    if list.is_empty() {
        anyhow::bail!("`roots` names at least one root");
    }

    let mut identifiers = Vec::new();
    let mut derived = serde_json::Map::new();
    for root in list.iter() {
        if let Some(from) = root.downcast_ref::<FromStage>() {
            derived.insert(from.name.clone(), from.expression.clone().into());
            continue;
        }
        identifiers.push(text(root, "root")?);
    }

    match (identifiers.is_empty(), derived.is_empty()) {
        (true, false) => Ok(serde_json::Value::Object(derived)),
        (false, true) => Ok(serde_json::Value::Array(
            identifiers.into_iter().map(Into::into).collect(),
        )),
        (true, true) => anyhow::bail!("`roots` names at least one root"),
        (false, false) => anyhow::bail!(
            "a walk's roots are either identifiers or derived from stages, not both"
        ),
    }
}

/// The document a list of connections renders to.
fn connections_document(traverse: Option<Value>) -> anyhow::Result<serde_json::Value> {
    let Some(traverse) = traverse else {
        return Ok(serde_json::Value::Array(Vec::new()));
    };
    if traverse.is_none() {
        return Ok(serde_json::Value::Array(Vec::new()));
    }
    let Some(list) = starlark::values::list::ListRef::from_value(traverse) else {
        anyhow::bail!("`traverse` is a list, found `{}`", traverse.get_type());
    };

    let mut out = Vec::new();
    for entry in list.iter() {
        if let Some(connection) = entry.downcast_ref::<Connection>() {
            out.push(match &connection.gate {
                None => serde_json::Value::String(connection.connection_type.clone()),
                Some(gate) => serde_json::Value::Object(
                    [(
                        connection.connection_type.clone(),
                        serde_json::Value::String(gate.clone()),
                    )]
                    .into_iter()
                    .collect(),
                ),
            });
            continue;
        }
        out.push(json(entry)?);
    }
    Ok(serde_json::Value::Array(out))
}

/// A stage value's name and document, or an error naming what arrived.
fn stage_of(value: Value, what: &str) -> anyhow::Result<(String, serde_json::Value)> {
    match value.downcast_ref::<Stage>() {
        Some(stage) => Ok((stage.name.clone(), stage.document.clone())),
        None => anyhow::bail!("`{what}` is a stage, found `{}`", value.get_type()),
    }
}

#[starlark_module]
pub fn highwire(builder: &mut GlobalsBuilder) {
    /// A walk stage: a traversal and the levels applied to the items it reaches.
    ///
    /// `roots` is a list of identifiers, or of `from_stage` values. `traverse` is
    /// a list of connection types, plain strings or `connection` values, and
    /// defaults to none. `emit` is required: a stage that emits nothing produces
    /// nothing.
    #[expect(clippy::too_many_arguments)]
    fn walk(
        name: Value,
        roots: Value,
        emit: Value,
        traverse: Option<Value>,
        filter: Option<Value>,
        stop: Option<Value>,
        tee: Option<Value>,
        dedup: Option<Value>,
    ) -> anyhow::Result<Stage> {
        let name = text(name, "name")?;

        let mut document = serde_json::Map::new();
        document.insert("name".into(), name.clone().into());
        document.insert("roots".into(), roots_document(roots)?);
        document.insert("emit".into(), json(emit)?);
        document.insert("walk".into(), connections_document(traverse)?);
        if let Some(filter) = optional_json(filter)? {
            document.insert("filter".into(), filter);
        }
        if let Some(stop) = optional_json(stop)? {
            document.insert("stop".into(), stop);
        }
        document.insert("tee".into(), bool_of(tee)?.into());
        document.insert("dedup".into(), bool_of(dedup)?.into());

        Ok(Stage {
            name,
            document: serde_json::Value::Object(document),
        })
    }

    /// A barker stage: Starlark source, and the stages whose rows it reads.
    ///
    /// `source` is the script as text. `inputs` is a list of stage values, so a
    /// stage is referred to by value and cannot be mistyped.
    fn barker(
        name: Value,
        source: Value,
        inputs: Option<Value>,
        tee: Option<Value>,
        unnest: Option<Value>,
        dedup: Option<Value>,
    ) -> anyhow::Result<Stage> {
        let name = text(name, "name")?;
        let source = text(source, "source")?;

        let mut named = Vec::new();
        if let Some(inputs) = inputs
            && !inputs.is_none()
        {
            let Some(list) = starlark::values::list::ListRef::from_value(inputs) else {
                anyhow::bail!("`inputs` is a list, found `{}`", inputs.get_type());
            };
            for input in list.iter() {
                named.push(stage_of(input, "input")?.0);
            }
        }

        let document = serde_json::json!({
            "name": name,
            "barker": source,
            "inputs": named,
            "tee": bool_of(tee)?,
            "unnest": bool_of(unnest)?,
            "dedup": bool_of(dedup)?,
        });

        Ok(Stage {
            name,
            document,
        })
    }

    /// A root that derives its identifiers from another stage's rows.
    ///
    /// `expression` is applied to each of that stage's rows, and the identifiers
    /// it yields are this walk's starting set.
    fn from_stage(stage: Value, expression: Value) -> anyhow::Result<FromStage> {
        let (name, _) = stage_of(stage, "stage")?;
        Ok(FromStage {
            name,
            expression: text(expression, "expression")?,
        })
    }

    /// A connection type and, optionally, the gate that admits an edge of it.
    fn connection(connection_type: Value, gate: Option<Value>) -> anyhow::Result<Connection> {
        Ok(Connection {
            connection_type: text(connection_type, "connection type")?,
            gate: optional_text(gate, "gate")?,
        })
    }

    /// Run a query built from these stages, returning its rows.
    ///
    /// THE WORK STARTS HERE, not on the first pull: the document is compiled and
    /// the graph's stages started before this returns, and the rows are then read
    /// one at a time from the running graph — a nested query feeds its parent
    /// rather than being materialised. A caller that builds a query and never
    /// reads it has still started the work.
    fn run(
        stages: Value,
        eval: &mut starlark::eval::Evaluator,
    ) -> anyhow::Result<Rows> {
        let Some(list) = starlark::values::list::ListRef::from_value(stages) else {
            anyhow::bail!("`run` takes a list of stages, found `{}`", stages.get_type());
        };

        let mut phases = Vec::new();
        let mut names = std::collections::HashSet::new();
        for entry in list.iter() {
            let (name, document) = stage_of(entry, "stage")?;
            if !names.insert(name.clone()) {
                anyhow::bail!("two stages are named `{name}`; a name identifies one stage");
            }
            phases.push(document);
        }
        if phases.is_empty() {
            anyhow::bail!("`run` needs at least one stage");
        }

        let context = eval
            .extra
            .and_then(|extra| extra.downcast_ref::<Context>())
            .ok_or_else(|| anyhow::anyhow!("a query needs a run context"))?;

        crate::highwire::compiler::run_nested(
            serde_json::Value::Object([("phases".to_string(), phases.into())].into_iter().collect()),
            context.lookup.clone(),
            context.accountant.clone(),
        )
    }
}

/// An optional Starlark value as `serde_json`, with `None` and a JSON null both
/// meaning "absent".
fn optional_json(value: Option<Value>) -> anyhow::Result<Option<serde_json::Value>> {
    match value {
        None => Ok(None),
        Some(value) if value.is_none() => Ok(None),
        Some(value) => Ok(Some(json(value)?)),
    }
}

/// An optional Starlark boolean, defaulting to false.
fn bool_of(value: Option<Value>) -> anyhow::Result<bool> {
    match value {
        None => Ok(false),
        Some(value) if value.is_none() => Ok(false),
        Some(value) => value
            .unpack_bool()
            .ok_or_else(|| anyhow::anyhow!("a boolean is required, found `{}`", value.get_type())),
    }
}
