//! Barker: a program over a stream.
//!
//! A Barker program is [Starlark](https://github.com/bazelbuild/starlark) — the
//! hermetic Python dialect — and Barker is an ELEMENT OF A PIPELINE rather than
//! a layer above one:
//!
//! ```text
//! rows in ──▶ a Starlark program ──▶ rows out
//! ```
//!
//! Rows go in, the program runs, rows come out. Both ends are streams: the
//! program reads its input by iterating it, and every `emit` hands a row on as
//! it is produced rather than at the end.
//!
//! # The functions a program can call
//!
//! Big Tent provides `emit`. Everything else is the embedder's: a caller passes
//! its own set of natives, and they compose with Big Tent's and with the
//! standard library. That is the whole plugin surface — a function, not a
//! framework:
//!
//! ```
//! use bigtent::barker;
//! use starlark::environment::GlobalsBuilder;
//! use starlark::starlark_module;
//!
//! #[starlark_module]
//! fn my_natives(builder: &mut GlobalsBuilder) {
//!     /// Double a number.
//!     fn double(x: i32) -> anyhow::Result<i32> {
//!         Ok(x * 2)
//!     }
//! }
//!
//! let program = barker::parse("emit(double(21))\n")?;
//! let (sender, input) = flume::unbounded();
//! drop(sender); // the stream is empty and closed
//! let rows = barker::run_with(program, input, None, Some(my_natives))?;
//! let out: Vec<serde_json::Value> = rows.collect::<Result<Vec<_>, _>>()?;
//! assert_eq!(out, vec![serde_json::json!(42)]);
//! # Ok::<(), anyhow::Error>(())
//! ```
//!
//! # Threading, and why there is a thread here
//!
//! Starlark's evaluator is arena-bound and is not `Send`, so it cannot be built
//! on one thread and run on another. It is therefore built on the thread that
//! runs it, and the rows travel back over a channel. That is what makes the
//! output a stream: the caller reads a row while the program is still running,
//! instead of waiting for it to finish.

pub mod collections;
pub mod dates;
pub mod highwire;
pub mod digest;
pub mod math;
pub mod patterns;
pub mod values;

use anyhow::Result;
use flume::{Receiver, Sender};
use starlark::any::ProvidesStaticType;
use starlark::environment::{GlobalsBuilder, Module};
use starlark::eval::Evaluator;
use starlark::starlark_module;
use starlark::syntax::{AstModule, Dialect};
use starlark::values::none::NoneType;
use starlark::values::NoSerialize;
use starlark::values::{Heap, StarlarkValue, Trace, Tracer, Value, starlark_value};

/// A set of natives an embedder adds to a program's globals.
///
/// A function pointer rather than a trait object: the globals builder is the
/// interface, and a function that fills it in is the smallest thing that can.
pub type Natives = fn(&mut GlobalsBuilder);

/// What a native needs to reach: where rows go, and where a nested query walks.
///
/// It lives in the evaluator's `extra`, which is how a native reaches
/// per-evaluation state without a global and without capturing anything.
/// `pub(crate)` because the highwire natives read it too.
#[derive(Debug, ProvidesStaticType)]
pub(crate) struct Context {
    /// The row stream `emit` writes to.
    pub(crate) out: Sender<Result<serde_json::Value>>,

    /// The cluster a nested query walks, when this run was given one.
    pub(crate) lookup: Option<crate::highwire::walk::HerdOrCluster>,
}

/// The natives Big Tent provides to every program.
#[starlark_module]
fn big_tent_natives(builder: &mut GlobalsBuilder) {
    /// Hand one row on.
    ///
    /// The value is converted to JSON here, at the boundary, and sent
    /// immediately — so a program that emits and then keeps working has already
    /// delivered the row.
    fn emit(x: Value, eval: &mut Evaluator) -> anyhow::Result<NoneType> {
        let context = eval
            .extra
            .expect("the evaluator always carries a context")
            .downcast_ref::<Context>()
            .expect("the evaluator's extra is a context");
        context
            .out
            .send(Ok(x.to_json_value()?))
            .map_err(|_| anyhow::anyhow!("the row stream was closed"))?;
        Ok(NoneType)
    }
}

/// The input, as a value the program can iterate.
///
/// It is a PULL iterator, not an allocated list: `iter_next` takes the next row
/// from the source on demand, so a program that reads three rows of a million
/// only ever has three in memory. The index Starlark passes is ignored, which is
/// what distinguishes a stateful source from a container like `range` that
/// computes its element from the position.
///
/// It cannot be frozen — a source of rows is not a value that can be shared
/// immutably — so a program that binds it into a module global is refused rather
/// than given something that would have to be materialized to honour the binding.
#[derive(Debug, ProvidesStaticType, NoSerialize)]
struct StreamingInput {
    rows: flume::Receiver<serde_json::Value>,
}

/// A channel holds no allocation the heap tracks, so the accounting is empty.
impl allocative::Allocative for StreamingInput {
    fn visit<'a, 'b: 'a>(&self, _visitor: &'a mut allocative::Visitor<'b>) {}
}

impl std::fmt::Display for StreamingInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("input")
    }
}

/// The rows hold no Starlark values — they are JSON, converted as they are
/// pulled — so there is nothing for the garbage collector to trace.
unsafe impl<'v> Trace<'v> for StreamingInput {
    fn trace(&mut self, _tracer: &Tracer<'v>) {}
}

// The macro is the source of this type's `AllocValue`, `AllocFrozenValue`,
// `ProvidesStaticType` and frozen/non-frozen aliases, which is what `heap.alloc`
// needs. A doc comment cannot attach to a macro invocation, hence a plain
// comment.
starlark::starlark_simple_value!(StreamingInput);

#[starlark_value(type = "input")]
impl<'v> StarlarkValue<'v> for StreamingInput {
    /// The value is its own iterator, the way `range` is.
    unsafe fn iterate(&self, me: Value<'v>, _heap: Heap<'v>) -> starlark::Result<Value<'v>> {
        Ok(me)
    }

    /// Pull one row. `None` ends the iteration — the producer dropped its
    /// sender, which is what "the stream is finished" means on a channel.
    unsafe fn iter_next(&self, _index: usize, heap: Heap<'v>) -> Option<Value<'v>> {
        self.rows.recv().ok().map(|row| heap.alloc(row))
    }

    unsafe fn iter_stop(&self) {}
}

/// The rows a program produced, as they arrive.
///
/// Iterating this pulls rows while the program is still running. The stream
/// ends when the program does; an error — the program failing, a row that
/// cannot be JSON — arrives as an item, in the order it happened.
#[derive(Debug, ProvidesStaticType, NoSerialize)]
pub struct Rows(Receiver<Result<serde_json::Value>>);

impl allocative::Allocative for Rows {
    fn visit<'a, 'b: 'a>(&self, _visitor: &'a mut allocative::Visitor<'b>) {}
}
unsafe impl<'v> Trace<'v> for Rows {
    fn trace(&mut self, _tracer: &Tracer<'v>) {}
}
starlark::starlark_simple_value!(Rows);

#[starlark_value(type = "rows")]
impl<'v> StarlarkValue<'v> for Rows {
    /// A rows value is its own iterator, like `range`.
    unsafe fn iterate(
        &self,
        me: Value<'v>,
        _heap: starlark::values::Heap<'v>,
    ) -> starlark::Result<Value<'v>> {
        Ok(me)
    }

    /// Pull one row. `None` ends the stream, which is what a closed channel is.
    ///
    /// An error item is not the end of the stream and must not become silence: a
    /// nested query that failed would otherwise look like one that emitted
    /// nothing, which is the difference between a broken query and an empty one.
    /// So it panics, which becomes the enclosing program's error row.
    unsafe fn iter_next(
        &self,
        _index: usize,
        heap: starlark::values::Heap<'v>,
    ) -> Option<Value<'v>> {
        match self.0.recv().ok()? {
            Ok(row) => Some(heap.alloc(row)),
            Err(error) => panic!("the nested query failed: {error}"),
        }
    }

    unsafe fn iter_stop(&self) {}
}

impl std::fmt::Display for Rows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("rows")
    }
}

impl Rows {
    /// A rows value over a stream someone else produced.
    pub(crate) fn from_receiver(receiver: Receiver<Result<serde_json::Value>>) -> Self {
        Rows(receiver)
    }
}

impl Iterator for Rows {
    type Item = Result<serde_json::Value>;

    fn next(&mut self) -> Option<Self::Item> {
        // the channel closes when the program's thread ends, which is what ends
        // the stream: a dropped sender is the end, not an error
        self.0.recv().ok()
    }
}

/// A program that has been parsed and not run.
///
/// Parsing is its own phase and it happens on the caller's thread, before
/// anything is forked: a program that does not parse never reaches a thread and
/// never produces a stream. A `Program` is what [`run`] accepts, so there is no
/// way to execute source that was not parsed first.
pub struct Program {
    ast: AstModule,
}

/// Parse a program. Nothing is executed and nothing is allocated for a run.
///
/// Extended, because a program's work is at the top level: the standard dialect
/// forbids a `for` outside a `def`, which would make every program a function
/// definition and a call. Extended also enables `load`, and that is left
/// unusable on purpose — no loader is configured, so a program cannot reach the
/// filesystem through it.
pub fn parse(program: &str) -> Result<Program> {
    let ast = AstModule::parse("barker.star", program.to_owned(), &Dialect::Extended)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(Program { ast })
}

/// Run a parsed program over a stream of rows, with Big Tent's natives.
pub fn run(program: Program, input: Receiver<serde_json::Value>) -> Result<Rows> {
    run_with(program, input, None, None)
}

/// Run a parsed program over a stream of rows, with Big Tent's natives and the
/// embedder's.
///
/// Execution happens on its own thread, and its rows come back through a channel
/// — so this returns as soon as the program has started, and the caller can read
/// rows while it runs. The program is consumed: a parsed program is run once.
pub fn run_with(
    program: Program,
    input: Receiver<serde_json::Value>,
    lookup: Option<crate::highwire::walk::HerdOrCluster>,
    natives: Option<Natives>,
) -> Result<Rows> {
    let Program { ast } = program;
    let (sender, receiver) = flume::unbounded();

    std::thread::spawn(move || {
        // the evaluator is built here because it is not Send: it cannot be made
        // on one thread and run on another
        let outcome = Module::with_temp_heap(|module| {
            // standard library, then Big Tent's natives and namespaces, then the
            // embedder's — `with` consumes the builder, so the layers are composed
            // in order
            let builder = GlobalsBuilder::standard()
                .with(big_tent_natives)
                // the namespaces: each is a module of functions, so a script
                // writes `math.sqrt(x)` and `dates.now_ms()`
                .with_namespace("math", math::math)
                .with_namespace("stats", math::stats)
                .with_namespace("collections", collections::collections)
                .with_namespace("patterns", patterns::patterns)
                .with_namespace("digest", digest::digest)
                .with_namespace("dates", dates::dates)
                .with_namespace("highwire", highwire::highwire);
            let builder = match natives {
                Some(natives) => builder.with(natives),
                None => builder,
            };
            let globals = builder.build();

            // the context a native needs: where its rows go, and where a nested
            // query would walk. Declared BEFORE the evaluator so it outlives it,
            // because `extra` holds a borrow.
            let context = Context {
                out: sender.clone(),
                lookup: lookup.clone(),
            };
            let mut eval = Evaluator::new(&module);
            eval.extra = Some(&context);

            // the input is a global the program iterates, pulled a row at a time
            module.set(
                "input",
                module.heap().alloc(StreamingInput { rows: input }),
            );

            eval.eval_module(ast, &globals)?;
            starlark::Result::Ok(())
        });

        if let Err(error) = outcome {
            // A program that failed says so as a ROW, not as the end of the
            // stream. A pipeline carries rows; an error that lived outside them
            // would either end the run or be invisible to whatever is downstream,
            // and neither is what a failure inside one stage should do. The
            // shape is fixed so a consumer can recognise it: a single key naming
            // the failure. A FAILED send means the consumer is gone — the
            // teardown has begun — and is swallowed only because this thread
            // is finishing anyway; there is nothing left to report to.
            // a FAILED send means the consumer is gone — the teardown has
            // begun — and is swallowed only because this thread is finishing
            // anyway; there is nothing left to report to
            let _ = sender.send(Ok(serde_json::json!({
                "error": format!("{error}"),
            })));
        }
    });

    Ok(Rows(receiver))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A closed stream of the given rows: the sender is dropped before the
    /// program starts, so iteration ends after the last row.
    fn stream(rows: &[serde_json::Value]) -> Receiver<serde_json::Value> {
        let (sender, receiver) = flume::unbounded();
        for row in rows {
            sender.send(row.clone()).expect("the stream accepts rows");
        }
        drop(sender);
        receiver
    }

    /// Requirement: a program takes rows in and produces rows out.
    ///
    /// What: a program that emits each input row unchanged, over three inputs of
    /// different JSON types.
    ///
    /// Why: this is the whole shape of the element — rows in, a program, rows
    /// out. If the input were not readable by the program, or the emitted rows
    /// were not JSON, there would be no element to put in a pipeline.
    ///
    /// LLM section: `input` is a stream bound as a global the program iterates;
    /// `emit` is Big Tent's native. The values are compared as JSON, so a row
    /// that arrives in the wrong shape fails here.
    #[test]
    fn a_program_takes_rows_in_and_puts_rows_out() -> Result<()> {
        let rows = run(
            parse("for x in input:\n    emit(x)\n")?,
            
            stream(&[json!(1), json!(2), json!("three")]),
        )?;

        let out: Vec<serde_json::Value> = rows.collect::<Result<Vec<_>>>()?;
        assert_eq!(out, vec![json!(1), json!(2), json!("three")]);
        Ok(())
    }

    /// Requirement: the program is a program, not a mapping — it may transform,
    /// filter and multiply the rows it is given.
    ///
    /// What: a program that emits a transformed row for each input and an extra
    /// row at the end.
    ///
    /// Why: an element that could only pass rows through would not be worth
    /// embedding a language for. The count changing is the point: three rows in,
    /// four rows out.
    ///
    /// LLM section: Starlark is used for its control flow, which is what makes
    /// this an element rather than a conversion.
    #[test]
    fn a_program_may_transform_and_multiply_rows() -> Result<()> {
        let rows = run(
            parse("for x in input:\n    emit(x * 2)\nemit(99)\n")?,
            
            stream(&[json!(1), json!(2), json!(3)]),
        )?;

        let out: Vec<serde_json::Value> = rows.collect::<Result<Vec<_>>>()?;
        assert_eq!(out, vec![json!(2), json!(4), json!(6), json!(99)]);
        Ok(())
    }

    /// Requirement: ten integers stream in and ten doubled integers stream out,
    /// with execution on its own thread and both ends on the caller's.
    ///
    /// What: a program that emits `"started"` before it touches its input, then
    /// doubles each row it reads. The first row is read *before* any input is
    /// sent; then ten rows are sent; then the sender closes; then the rest are
    /// collected.
    ///
    /// Why: this is the contract — production and collection on the calling
    /// thread, execution on another — and the assertion order is what proves it.
    /// Reading `"started"` before sending anything can only happen if the
    /// program is running concurrently: were it on this thread, `run` would be
    /// blocked waiting for input and would not have returned. And the ten
    /// doubled rows can only come back if the program pulled each row off the
    /// channel as it arrived, rather than being handed a finished list.
    ///
    /// LLM section: the input channel is unbounded, so production never blocks;
    /// the output channel is unbounded, so the program never blocks on the
    /// consumer. Neither side can deadlock the other, which is why the ordering
    /// below is safe as well as meaningful.
    #[test]
    fn ten_ints_stream_in_and_double_out() -> Result<()> {
        let (input, receiver) = flume::unbounded();

        // forked: the program is now running on its own thread, blocked on the
        // input channel
        let mut rows = run(
            parse("emit(\"started\")\nfor x in input:\n    emit(x * 2)\n")?,
            receiver,
        )?;

        // the program is alive before the caller has produced anything
        let first = rows.next().expect("the program emitted before reading input")?;
        assert_eq!(first, json!("started"), "execution had already begun");

        // production, on the calling thread
        for n in 1..=10 {
            input.send(json!(n)).expect("the stream accepts rows");
        }
        drop(input);

        // collection, on the calling thread
        let out: Vec<serde_json::Value> = rows.collect::<Result<Vec<_>>>()?;
        assert_eq!(
            out,
            vec![
                json!(2),
                json!(4),
                json!(6),
                json!(8),
                json!(10),
                json!(12),
                json!(14),
                json!(16),
                json!(18),
                json!(20)
            ],
            "ten rows in, ten doubled rows out, in the order they were sent"
        );
        Ok(())
    }

    /// Requirement: a program that fails says so, rather than ending quietly.
    ///
    /// What: a program with a runtime error after one emitted row.
    ///
    /// Why: the stream ends when the program ends, so a failure that did not
    /// travel on the stream would be indistinguishable from a program that
    /// simply finished — and a pipeline would carry on with half the rows.
    ///
    /// LLM section: the row emitted before the failure is still delivered; the
    /// failure follows it as a row of its own, under the single key `error`. The
    /// failure is a RUNTIME one on purpose — an undefined name is caught before
    /// the module runs at all, so nothing would have been emitted and there would
    /// be nothing to deliver first. That distinction is pinned by
    /// `a_program_with_a_static_error_emits_nothing`.
    #[test]
    fn a_failing_program_reports_its_failure_as_a_row() -> Result<()> {
        let rows = run(parse("emit(1)\nfail(\"boom\")\n")?, stream(&[]))?;

        let out: Vec<serde_json::Value> = rows.collect::<Result<Vec<_>>>()?;
        assert_eq!(out.len(), 2, "one row, then the failure: {out:?}");
        assert_eq!(out[0], json!(1));
        let failure = out[1].as_object().expect("the failure is a row");
        assert_eq!(failure.len(), 1, "one key names it: {failure:?}");
        assert!(
            failure["error"].as_str().is_some_and(|m| m.contains("boom")),
            "the row carries the message: {failure:?}"
        );
        Ok(())
    }

    /// Requirement: a program that cannot be compiled emits nothing at all.
    ///
    /// What: a program that emits a row and then names an undefined variable.
    ///
    /// Why: this is the difference between a program that fails part-way and one
    /// that never runs. Starlark checks a module's globals before executing it,
    /// so an undefined name means the whole module is refused — and a caller
    /// that assumed "the rows before the failure are still there" would be wrong
    /// about a program that produced none.
    ///
    /// LLM section: one row, the error — no row precedes it.
    #[test]
    fn a_program_with_a_static_error_emits_nothing() -> Result<()> {
        let rows = run(parse("emit(1)\nundefined_name\n")?, stream(&[]))?;
        let out: Vec<serde_json::Value> = rows.collect::<Result<Vec<_>>>()?;
        assert_eq!(out.len(), 1, "the error, and nothing else: {out:?}");
        assert!(
            out[0].get("error").is_some(),
            "the one row is the failure: {out:?}"
        );
        Ok(())
    }

    /// Requirement: a program cannot reach the filesystem through `load`.
    ///
    /// What: a program that tries to `load` another Starlark file.
    ///
    /// Why: the dialect a program is parsed with enables `load`, and `load` is
    /// file access. It is left unusable by not configuring a loader — so this
    /// pins the hermetic boundary rather than trusting it: if a loader were ever
    /// wired in, this test would start reading a file it should not.
    ///
    /// LLM section: the failure arrives as an error row, like any other failure
    /// the program meets.
    #[test]
    fn a_program_cannot_load_another_file() -> Result<()> {
        let rows = run(parse("load(\"other.star\", \"x\")\n")?, stream(&[]))?;
        let out: Vec<serde_json::Value> = rows.collect::<Result<Vec<_>>>()?;
        assert_eq!(out.len(), 1, "one row: {out:?}");
        assert!(
            out[0].get("error").is_some(),
            "`load` cannot resolve without a loader: {out:?}"
        );
        Ok(())
    }

    /// Requirement: a program that does not parse is refused before anything
    /// runs.
    ///
    /// What: `run` with a syntax error.
    ///
    /// Why: parsing happens on the calling thread before the program is forked,
    /// so a syntax error is returned to the caller and there is no stream at
    /// all. That is deliberately different from a program that parses and then
    /// fails while running: the first is a mistake in the script the caller has
    /// in hand, the second is a failure the pipeline must carry as a row, and
    /// conflating them would make a typo indistinguishable from a walk that
    /// broke.
    ///
    /// LLM section: no `Rows` is produced, so nothing here is an item on a
    /// stream — the assertion is on the returned error.
    #[test]
    fn a_program_that_does_not_parse_is_refused_before_it_runs() -> Result<()> {
        let error = parse("def def def\n")
            .err()
            .expect("a syntax error is the caller's to handle");
        assert!(
            error.to_string().contains("Parse error"),
            "the failure says it is a parse error: {error}"
        );
        Ok(())
    }

    /// Requirement: an embedder's natives are available to the program, alongside
    /// Big Tent's.
    ///
    /// What: a native defined by the caller, called from the program.
    ///
    /// Why: this is the plugin surface in its entirety — a program may call
    /// functions Big Tent defines and functions the embedding application
    /// defines, with no difference between them from inside the script. If the
    /// embedder could not add natives, Barker could only ever do what Big Tent
    /// happened to provide.
    ///
    /// LLM section: the native is an ordinary `#[starlark_module]` function, so
    /// this also pins that the composition order works: standard library, Big
    /// Tent's natives, then the embedder's.
    #[test]
    fn an_embedders_natives_are_callable_from_the_program() -> Result<()> {
        #[starlark_module]
        fn embedder_natives(builder: &mut GlobalsBuilder) {
            /// Double a number.
            fn double(x: i32) -> anyhow::Result<i32> {
                Ok(x * 2)
            }
        }

        let rows = run_with(
            parse("emit(double(21))\n")?,
            stream(&[]),
            None,
            Some(embedder_natives),
        )?;
        let out: Vec<serde_json::Value> = rows.collect::<Result<Vec<_>>>()?;
        assert_eq!(out, vec![json!(42)]);
        Ok(())
    }
}

#[cfg(test)]
mod function_tests {
    use super::*;
    use serde_json::json;

    /// A stream that is already closed and empty.
    fn empty() -> Receiver<serde_json::Value> {
        let (sender, receiver) = flume::unbounded();
        drop(sender);
        receiver
    }

    /// Run a program and collect the rows it emits, failing on a parse error.
    fn rows_of(script: &str) -> Vec<serde_json::Value> {
        run(parse(script).expect("the script parses"), empty())
            .expect("the program runs")
            .collect::<Result<Vec<_>>>()
            .expect("every row converts")
    }

    /// Run a program over a stream of rows and collect what it emits.
    fn rows_over(values: &[serde_json::Value], script: &str) -> Vec<serde_json::Value> {
        rows_over_with(values, script, None)
    }

    /// The same, with a cluster a nested query can walk.
    fn rows_over_with(
        values: &[serde_json::Value],
        script: &str,
        lookup: Option<crate::highwire::walk::HerdOrCluster>,
    ) -> Vec<serde_json::Value> {
        let (sender, receiver) = flume::unbounded();
        for value in values {
            sender.send(value.clone()).expect("the stream accepts rows");
        }
        drop(sender);
        run_with(parse(script).expect("the script parses"), receiver, lookup, None)
            .expect("the program runs")
            .collect::<Result<Vec<_>>>()
            .expect("every row converts")
    }

    /// A cluster holding the named identifiers, so a nested query has a graph.
    fn test_cluster(identifiers: &[&str]) -> crate::highwire::walk::HerdOrCluster {
        use crate::item::Item;
        use crate::rodeo::goat_herd::GoatHerd;
        use crate::rodeo::member::member_synth;
        use crate::rodeo::robo_goat::RoboticGoat;

        let items: Vec<Item> = identifiers
            .iter()
            .map(|identifier| Item {
                identifier: identifier.to_string(),
                connections: Default::default(),
                body_mime_type: None,
                body: None,
            })
            .collect();
        let member = member_synth(RoboticGoat::new("test", items, serde_json::Value::Null));
        crate::highwire::walk::HerdOrCluster::Herd(std::sync::Arc::new(GoatHerd::new(vec![member])))
    }

    /// The single number a one-row program emitted, as an `f64`.
    fn one_number(script: &str) -> f64 {
        let rows = rows_of(script);
        assert_eq!(rows.len(), 1, "one row expected: {rows:?}");
        rows[0]
            .as_f64()
            .or_else(|| rows[0].as_i64().map(|n| n as f64))
            .unwrap_or_else(|| panic!("a number expected, found {}", rows[0]))
    }

    /// Requirement: every function in `math` is reachable and computes.
    ///
    /// What: one assertion per function, over values whose answer is known
    /// exactly or to within a tolerance.
    ///
    /// Why: the module is a list of thirty wrappers, and a wrapper that is
    /// registered but wired to the wrong `f64` method returns a plausible number
    /// rather than an error. The only way to know `acos` is not `asin` is to
    /// assert a value only one of them produces.
    ///
    /// LLM section: the assertions are chosen so that the correct answer and the
    /// neighbouring function's answer differ — `acos(0)` is pi/2 where `asin(0)`
    /// is 0, `ceil` and `floor` and `round` differ on 1.5, and `trunc` differs
    /// from `floor` on a negative.
    #[test]
    fn every_math_function_computes() {
        let close = |a: f64, b: f64| (a - b).abs() < 1e-12;

        assert!(close(one_number("emit(math.e)"), std::f64::consts::E));
        assert!(close(one_number("emit(math.pi)"), std::f64::consts::PI));
        assert_eq!(one_number("emit(math.ceil(1.2))"), 2.0);
        assert_eq!(one_number("emit(math.floor(1.8))"), 1.0);
        assert_eq!(one_number("emit(math.round(1.5))"), 2.0);
        assert_eq!(one_number("emit(math.trunc(-1.5))"), -1.0);
        assert_eq!(one_number("emit(math.floor(-1.5))"), -2.0, "trunc is not floor");
        assert_eq!(one_number("emit(math.pow(2, 10))"), 1024.0);
        assert_eq!(one_number("emit(math.sqrt(81))"), 9.0);
        assert!(close(one_number("emit(math.exp(0))"), 1.0));
        assert!(close(one_number("emit(math.log(100, 10))"), 2.0));
        assert!(close(one_number("emit(math.log(math.e))"), 1.0), "natural by default");
        assert_eq!(one_number("emit(math.fabs(-3))"), 3.0);
        assert_eq!(one_number("emit(math.mod(7, 3))"), 1.0);
        assert_eq!(one_number("emit(math.copysign(3, -1))"), -3.0);
        assert!(close(one_number("emit(math.acos(0))"), std::f64::consts::FRAC_PI_2));
        assert!(close(one_number("emit(math.asin(0))"), 0.0), "asin(0) is not acos(0)");
        assert!(close(one_number("emit(math.atan(0))"), 0.0));
        assert!(close(one_number("emit(math.atan2(1, 1))"), std::f64::consts::FRAC_PI_4));
        assert!(close(one_number("emit(math.cos(0))"), 1.0));
        assert!(close(one_number("emit(math.sin(0))"), 0.0), "sin(0) is not cos(0)");
        assert!(close(one_number("emit(math.tan(0))"), 0.0));
        assert!(close(one_number("emit(math.hypot(3, 4))"), 5.0));
        assert!(close(one_number("emit(math.acosh(1))"), 0.0));
        assert!(close(one_number("emit(math.asinh(0))"), 0.0));
        assert!(close(one_number("emit(math.atanh(0))"), 0.0));
        assert!(close(one_number("emit(math.cosh(0))"), 1.0));
        assert!(close(one_number("emit(math.sinh(0))"), 0.0), "sinh(0) is not cosh(0)");
        assert!(close(one_number("emit(math.tanh(0))"), 0.0));
        assert!(close(one_number("emit(math.radians(180))"), std::f64::consts::PI));
        assert!(close(one_number("emit(math.degrees(math.pi))"), 180.0));
    }

    /// Requirement: a stream is summarised in ONE PASS, without collecting it.
    ///
    /// What: eight rows arrive over a stream; the program keeps three running
    /// totals — count, sum, sum of squares — and derives the mean and both
    /// deviations from them at the end. No list is built.
    ///
    /// Why: this is the shape the pipeline exists for. A stream is one-shot and
    /// has no `len()`, so a summary that needed the values twice could not be
    /// computed at all; and a program that collected them first would hold the
    /// whole stream in memory, which is the thing a stream avoids. The identities
    /// used are the sum ones — variance is `Σx²/n - mean²` — so the assertions
    /// fail if any accumulator is wrong, not just if the final division is.
    ///
    /// LLM section: the rows are the textbook set 2, 4, 4, 4, 5, 5, 7, 9, whose
    /// mean is 5, population deviation 2, and sample deviation sqrt(32/7). The
    /// population and sample forms differ only in the denominator, so a swap
    /// shows.
    #[test]
    fn a_stream_is_summarised_in_one_pass() {
        let rows = rows_over(
            &[2, 4, 4, 4, 5, 5, 7, 9].map(|n| json!(n)),
            "n = 0\n\
             total = 0.0\n\
             squares = 0.0\n\
             for row in input:\n\
             \x20   n = n + 1\n\
             \x20   total = total + row\n\
             \x20   squares = squares + row * row\n\
             mean = total / n\n\
             population = squares / n - mean * mean\n\
             sample = (squares - total * total / n) / (n - 1)\n\
             emit({\"n\": n, \"mean\": mean, \"total_squared\": squares,\n\
                   \"population_stdev\": math.sqrt(population),\n\
                   \"sample_stdev\": math.sqrt(sample)})\n",
        );

        assert_eq!(rows.len(), 1, "one summary row: {rows:?}");
        let summary = rows[0].as_object().expect("the summary is an object");
        let number = |key: &str| {
            summary
                .get(key)
                .and_then(|v| v.as_f64())
                .unwrap_or_else(|| panic!("`{key}` is a number: {summary:?}"))
        };
        let close = |a: f64, b: f64| (a - b).abs() < 1e-12;

        assert_eq!(number("n"), 8.0, "every row was read exactly once");
        assert!(close(number("mean"), 5.0), "mean: {summary:?}");
        assert!(
            close(number("total_squared"), 232.0),
            "4 + 16 + 16 + 16 + 25 + 25 + 49 + 81 = 232: {summary:?}"
        );
        assert!(
            close(number("population_stdev"), 2.0),
            "the population deviation: {summary:?}"
        );
        assert!(
            close(number("sample_stdev"), (32.0f64 / 7.0).sqrt()),
            "the sample deviation, which is a different number: {summary:?}"
        );
    }

    /// Requirement: `tally` counts occurrences and returns them as a dict.
    ///
    /// What: a list with repeats, counted.
    ///
    /// Why: a tally is how a stream becomes a distribution, and a mis-keyed count
    /// changes every number derived from it.
    ///
    /// LLM section: strings and numbers key by their own rendered form.
    #[test]
    fn tally_counts() {
        let rows = rows_of("emit(stats.tally([\"a\", \"b\", \"a\", \"a\"]))");
        assert_eq!(rows[0]["a"], json!(3), "{:?}", rows[0]);
        assert_eq!(rows[0]["b"], json!(1), "{:?}", rows[0]);
    }

    /// Requirement: the collection functions shape a list.
    ///
    /// What: `unique`, `flatten`, `chunks`, `group_by`, `any_of`, `all_of`,
    /// `count_if` and `find_first`.
    ///
    /// Why: these are what turn a stream into groups before arithmetic; each is
    /// one line of logic and one line of wrapper, and a wrapper that is not
    /// registered is invisible until a script calls it.
    ///
    /// LLM section: the predicates are lambdas, which pins that a Starlark
    /// function can be passed to a native and called back.
    #[test]
    fn the_collections_shape() {
        assert_eq!(
            rows_of("emit(collections.unique([1, 2, 1, 3, 2]))")[0],
            json!([1, 2, 3])
        );
        assert_eq!(
            rows_of("emit(collections.flatten([[1, 2], 3, [4]]))")[0],
            json!([1, 2, 3, 4])
        );
        assert_eq!(
            rows_of("emit(collections.chunks([1, 2, 3, 4, 5], 2))")[0],
            json!([[1, 2], [3, 4], [5]])
        );
        assert_eq!(
            rows_of("emit(collections.group_by([1, 2, 3, 4], lambda x: x % 2))")[0],
            json!({"0": [2, 4], "1": [1, 3]})
        );
        assert_eq!(
            rows_of("emit(collections.any_of([1, 2, 3], lambda x: x > 2))")[0],
            json!(true)
        );
        assert_eq!(
            rows_of("emit(collections.all_of([1, 2, 3], lambda x: x > 2))")[0],
            json!(false)
        );
        assert_eq!(
            rows_of("emit(collections.count_if([1, 2, 3], lambda x: x > 1))")[0],
            json!(2)
        );
        assert_eq!(
            rows_of("emit(collections.find_first([1, 2, 3], lambda x: x > 1))")[0],
            json!(2)
        );
    }

    /// Requirement: the pattern functions match, replace and encode.
    ///
    /// What: `matches`, `find_all`, `replace_re`, `hex_encode`, `hex_decode`,
    /// `base64url_encode`.
    ///
    /// Why: Starlark has no pattern matching at all, so these are the only way to
    /// filter by identifier shape; a bad pattern must be an error rather than a
    /// panic, which is why one is included.
    ///
    /// LLM section: a gitoid prefix and a jar suffix are the two shapes the corpus
    /// actually contains.
    #[test]
    fn the_patterns_match() {
        assert_eq!(
            rows_of("emit(patterns.matches(\"gitoid:blob:sha256:ab\", \"^gitoid:\"))")[0],
            json!(true)
        );
        assert_eq!(
            rows_of("emit(patterns.matches(\"repo/a.jar\", \"\\\\.jar$\"))")[0],
            json!(true)
        );
        assert_eq!(
            rows_of("emit(patterns.find_all(\"a1b22c333\", \"[0-9]+\"))")[0],
            json!(["1", "22", "333"])
        );
        assert_eq!(
            rows_of("emit(patterns.replace_re(\"a1b22\", \"[0-9]+\", \"#\"))")[0],
            json!("a#b#")
        );
        assert_eq!(rows_of("emit(patterns.hex_encode(\"hi\"))")[0], json!("6869"));
        assert_eq!(rows_of("emit(patterns.hex_decode(\"6869\"))")[0], json!("hi"));
        assert_eq!(rows_of("emit(patterns.base64url_encode(\"hi\"))")[0], json!("aGk="));

        let bad = rows_of("emit(patterns.matches(\"x\", \"[\"))");
        assert!(
            bad[0].get("error").is_some(),
            "a bad pattern is a row, not a panic: {bad:?}"
        );
    }

    /// Requirement: the digest functions produce the corpus's identifier forms.
    ///
    /// What: the bare digests, and the gitoid and Software Heritage forms of the
    /// same content.
    ///
    /// Why: a gitoid is the digest of `"blob <len>\0"` followed by the content,
    /// NOT the digest of the content. A script that hashed the content alone would
    /// produce a digest matching nothing in the corpus, and both would look like a
    /// 64-hex string. The empty content is the case that makes the framing
    /// visible, because its bare digest is the well-known
    /// `e3b0c442…` while its gitoid is different.
    ///
    /// LLM section: `sha256("")` is the empty-string digest; `gitoid_sha256("")`
    /// is the git blob id of an empty file. Distinct values, same content.
    #[test]
    fn the_digests_produce_the_identifier_forms() {
        let empty_sha256 = rows_of("emit(digest.sha256(\"\"))")[0]
            .as_str()
            .expect("hex")
            .to_string();
        assert_eq!(
            empty_sha256,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "the bare digest of no content"
        );

        let gitoid = rows_of("emit(digest.gitoid_sha256(\"\"))")[0]
            .as_str()
            .expect("an identifier")
            .to_string();
        assert!(
            gitoid.starts_with("gitoid:blob:sha256:"),
            "the corpus's form: {gitoid}"
        );
        assert_ne!(
            gitoid.trim_start_matches("gitoid:blob:sha256:"),
            empty_sha256,
            "the framing changes the digest, which is the whole point"
        );

        // "hello\n" is the canonical test vector for both hash functions
        assert_eq!(
            rows_of("emit(digest.sha1(\"hello\\n\"))")[0],
            json!("f572d396fae9206628714fb2ce00f72e94f2258f")
        );
        assert_eq!(
            rows_of("emit(digest.sha256(\"hello\\n\"))")[0],
            json!("5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03")
        );
        assert_eq!(
            rows_of("emit(digest.md5(\"hello\\n\"))")[0],
            json!("b1946ac92492d2347c6235b4d2611184")
        );
        let gitoid_sha1 = rows_of("emit(digest.gitoid_sha1(\"hello\\n\"))")[0]
            .as_str()
            .expect("an identifier")
            .to_string();
        assert_eq!(
            gitoid_sha1,
            "gitoid:blob:sha1:ce013625030ba8dba906f756967f9e9ca394464a"
        );

        // A SWHID for content is the GIT BLOB digest under its own prefix. So the
        // hash is identical and only the prefix differs — an implementation that
        // used the bare digest would produce an identifier that resolves to
        // nothing.
        let swh_sha1 = rows_of("emit(digest.swh_sha1(\"hello\\n\"))")[0]
            .as_str()
            .expect("an identifier")
            .to_string();
        assert_eq!(swh_sha1, format!("swh:1:cnt:{}", &gitoid_sha1["gitoid:blob:sha1:".len()..]));

        let gitoid_sha256 = rows_of("emit(digest.gitoid_sha256(\"hello\\n\"))")[0]
            .as_str()
            .expect("an identifier")
            .to_string();
        let swh_sha256 = rows_of("emit(digest.swh_sha256(\"hello\\n\"))")[0]
            .as_str()
            .expect("an identifier")
            .to_string();
        assert_eq!(
            swh_sha256,
            format!("swh:1:cnt:{}", &gitoid_sha256["gitoid:blob:sha256:".len()..])
        );
        assert_ne!(
            &swh_sha256["swh:1:cnt:".len()..],
            empty_sha256,
            "a SWHID is not the bare digest"
        );

        assert_eq!(
            rows_of("emit(digest.sha512(\"hello\\n\"))")[0]
                .as_str()
                .expect("hex")
                .len(),
            128,
            "sha512 is routed through the same crate"
        );

        // BLAKE3 is the one digest the coordinates crate does not define, so its
        // vector is the crate's own: the empty input.
        assert_eq!(
            rows_of("emit(digest.blake3(\"\"))")[0],
            json!("af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262")
        );
    }

    /// Requirement: a date is a canonical string, so comparing two dates as
    /// strings is comparing them in time.
    ///
    /// What: `parse_date` canonicalising an offset, and the string comparison
    /// that follows from it.
    ///
    /// Why: this is the property the representation was chosen for. If the
    /// canonical form were not fixed-width UTC, `"2026-10-16T12:00:00Z" <
    /// "2026-10-16T13:00:00+02:00"` would compare the text rather than the
    /// instants, and 13:00+02:00 is an hour EARLIER than 12:00Z.
    ///
    /// LLM section: 2026-10-16T13:00:00+02:00 is 11:00Z, so it sorts before
    /// 12:00Z — the opposite of its text order.
    #[test]
    fn dates_are_canonical_and_compare_chronologically() {
        assert_eq!(
            rows_of("emit(dates.parse_date(\"2026-10-16T13:00:00+02:00\"))")[0],
            json!("2026-10-16T11:00:00.000Z"),
            "canonical means UTC"
        );
        let rows = rows_of(
            "a = dates.parse_date(\"2026-10-16T13:00:00+02:00\")\n\
             b = dates.parse_date(\"2026-10-16T12:00:00Z\")\n\
             emit(a < b)",
        );
        assert_eq!(rows[0], json!(true), "11:00Z is before 12:00Z");
    }

    /// Requirement: components, arithmetic, ranges and formatting all work on the
    /// canonical string.
    ///
    /// What: every accessor, both shifts, the between and in-range functions, and
    /// a format.
    ///
    /// Why: each is a one-line wrapper, and a wrapper wired to the wrong
    /// component returns a plausible number.
    ///
    /// LLM section: 2026-10-16 is a Friday.
    #[test]
    fn the_dates_compute() {
        let stamp = "2026-10-16T12:34:56.789Z";
        let one = |script: String| rows_of(&script)[0].clone();

        assert_eq!(one(format!("emit(dates.year(\"{stamp}\"))")), json!(2026));
        assert_eq!(one(format!("emit(dates.month(\"{stamp}\"))")), json!(10));
        assert_eq!(one(format!("emit(dates.day(\"{stamp}\"))")), json!(16));
        assert_eq!(one(format!("emit(dates.hour(\"{stamp}\"))")), json!(12));
        assert_eq!(one(format!("emit(dates.minute(\"{stamp}\"))")), json!(34));
        assert_eq!(one(format!("emit(dates.second(\"{stamp}\"))")), json!(56));
        assert_eq!(one(format!("emit(dates.millisecond(\"{stamp}\"))")), json!(789));
        assert_eq!(
            one(format!("emit(dates.weekday(\"{stamp}\"))")),
            json!(4),
            "2026-10-16 is a Friday, and Monday is 0"
        );

        assert_eq!(
            one(format!("emit(dates.add_days(\"{stamp}\", 1))")),
            json!("2026-10-17T12:34:56.789Z")
        );
        assert_eq!(
            one(format!("emit(dates.add_milliseconds(\"{stamp}\", -789))")),
            json!("2026-10-16T12:34:56.000Z")
        );
        assert_eq!(
            one(
                "emit(dates.milliseconds_between(\"2026-10-16T12:00:00.000Z\", \
                 \"2026-10-16T12:00:01.500Z\"))"
                    .to_string()
            ),
            json!(1500)
        );
        assert_eq!(
            one(
                "emit(dates.in_range(\"2026-10-16T12:00:00.000Z\", \
                 \"2026-10-16T11:00:00.000Z\", \"2026-10-16T13:00:00.000Z\"))"
                    .to_string()
            ),
            json!(true)
        );
        assert_eq!(
            one(format!("emit(dates.format_date(\"{stamp}\", \"%Y-%m-%d\"))")),
            json!("2026-10-16")
        );
        assert_eq!(
            one("emit(dates.is_date(\"2026-10-16\"))".to_string()),
            json!(false),
            "a date alone is not an instant"
        );
        assert_eq!(
            one(format!("emit(dates.is_date(\"{stamp}\"))")),
            json!(true)
        );

        // round trip through milliseconds
        assert_eq!(
            one(format!(
                "emit(dates.from_millis(dates.to_millis(\"{stamp}\")))"
            )),
            json!(stamp)
        );

        // now_ms and now agree about the instant, to within a second
        assert_eq!(
            one("emit(dates.milliseconds_between(dates.now(), dates.from_millis(dates.now_ms())) < 1000)"
                .to_string()),
            json!(true)
        );
    }
    /// Requirement: a stage built by the DSL renders to the document the JSON
    /// reader accepts, and a query built from stages runs.
    ///
    /// What: a two-stage query — a walk, then a barker reading it — built entirely
    /// with `highwire.walk`, `highwire.from_stage` and `highwire.barker`, and run
    /// with `highwire.run`.
    ///
    /// Why: the DSL has no grammar of its own; it renders the same document the
    /// reader parses. If it produced anything else, a query written in a script
    /// and the same query written as JSON would be different queries, and only one
    /// of them would be tested.
    ///
    /// LLM section: the cluster holds `a`, so the walk reaches one item; the barker
    /// doubles it and the emitted row is `aa`.
    #[test]
    fn a_dsl_built_query_runs() {
        let rows = rows_over_with(
            &[],
            "jars = highwire.walk(\n\
             \x20   name = \"jars\",\n\
             \x20   roots = [\"a\"],\n\
             \x20   emit = \"identifier\",\n\
             )\n\
             shout = highwire.barker(\n\
             \x20   name = \"shout\",\n\
             \x20   source = \"for row in input:\\n    emit(row + row)\\n\",\n\
             \x20   inputs = [jars],\n\
             )\n\
             for row in highwire.run([jars, shout]):\n\
             \x20   emit(row)\n",
            Some(test_cluster(&["a"])),
        );

        assert_eq!(rows, vec![json!("aa")], "the nested query emitted its row");
    }

    /// Requirement: a stage is referred to by VALUE, so a mistyped name cannot
    /// happen.
    ///
    /// What: a barker whose `inputs` names a stage value, and a walk whose roots
    /// derive from another stage value.
    ///
    /// Why: this is the design's whole justification over writing the document by
    /// hand. A name typed as a string can be wrong, and the only defence is the
    /// compiler's "no stage is named that"; a value cannot be wrong, so that error
    /// is unrepresentable rather than caught.
    ///
    /// LLM section: the walk below derives its roots from the barker above it,
    /// across two stages, with no string naming either.
    #[test]
    fn stages_are_referred_to_by_value() {
        let rows = rows_over_with(
            &[],
            "first = highwire.barker(\n\
             \x20   name = \"first\",\n\
             \x20   source = \"emit(\\\"y\\\")\\n\",\n\
             \x20   tee = True,\n\
             )\n\
             second = highwire.walk(\n\
             \x20   name = \"second\",\n\
             \x20   roots = [highwire.from_stage(first, \"@\")],\n\
             \x20   emit = \"identifier\",\n\
             )\n\
             for row in highwire.run([first, second]):\n\
             \x20   emit(row)\n",
            Some(test_cluster(&["x"])),
        );

        // `first` emits "y"; `second` derives its roots from that row and walks to
        // "y" — which the herd does NOT hold, because the herd holds "x". So the
        // traversal finds nothing, and the only row is `first`'s tee copy: `first`
        // has a consumer, so it is not terminal, and its tee is what makes its row
        // visible at all.
        //
        // That is the assertion this test needs: it proves the reference between the
        // stages carried the value, and that a root which names nothing in the corpus
        // produces no rows rather than a phantom.
        assert_eq!(
            rows,
            vec![json!("y")],
            "the teed first, and nothing from the second: {rows:?}"
        );
    }

    /// Requirement: a query the DSL builds is refused where a hand-written one
    /// would be, and the refusal names the script's mistake.
    ///
    /// What: three malformed builds — no `emit`, a mixed root list, a duplicate
    /// stage name.
    ///
    /// Why: the DSL checks at construction, where the message can name the
    /// argument, rather than at compile time where the message names a position in
    /// a document the script never wrote. Each of these is a mistake a script
    /// author makes and none of them should reach the compiler.
    ///
    /// LLM section: each failure arrives as the enclosing program's error row, so
    /// the assertions look for `error`.
    #[test]
    fn the_dsl_refuses_malformed_stages() {
        let cases = [
            // no emit
            "highwire.walk(name = \"w\", roots = [\"a\"])\n",
            // identifiers and derived roots together
            "s = highwire.barker(name = \"s\", source = \"emit(1)\\n\")\n\
             highwire.walk(name = \"w\", roots = [\"a\", highwire.from_stage(s, \"@\")], emit = \"@\")\n",
            // one name twice
            "a = highwire.walk(name = \"same\", roots = [\"a\"], emit = \"@\")\n\
             b = highwire.walk(name = \"same\", roots = [\"b\"], emit = \"@\")\n\
             highwire.run([a, b])\n",
        ];

        for case in cases {
            let rows = rows_over_with(&[], case, Some(test_cluster(&["a", "b"])));
            assert!(
                rows.iter().any(|row| row.get("error").is_some()),
                "the mistake is reported as a row: {case}\n-> {rows:?}"
            );
        }
    }

    /// Requirement: nothing happens until the returned rows are iterated.
    ///
    /// What: a query built and run but never iterated, and then the same query
    /// iterated.
    ///
    /// Why: laziness is the decision the design rests on. A `run` that compiled and
    /// started the graph eagerly would make a script pay for a query it never
    /// reads, and would start threads that then had to be torn down.
    ///
    /// LLM section: the first program emits a marker after building and running
    /// without reading, so a completed run proves the build itself did not raise;
    /// the second reads and emits the row.
    #[test]
    fn a_nested_query_does_nothing_until_it_is_read() {
        let unread = rows_over_with(
            &[],
            "jars = highwire.walk(name = \"jars\", roots = [\"a\"], emit = \"identifier\")\n\
             rows = highwire.run([jars])\n\
             emit(\"built\")\n",
            Some(test_cluster(&["a"])),
        );
        assert_eq!(unread, vec![json!("built")], "the build did not raise");

        let read = rows_over_with(
            &[],
            "jars = highwire.walk(name = \"jars\", roots = [\"a\"], emit = \"identifier\")\n\
             for row in highwire.run([jars]):\n\
             \x20   emit(row)\n",
            Some(test_cluster(&["a"])),
        );
        assert_eq!(read, vec![json!("a")], "reading it produced the row");
    }

    /// Requirement: a barker that is a fan-in point can also run a query of its
    /// own, and merge that query's rows into what it emits.
    ///
    /// What: a diamond — two stages each deriving their roots from one seed — whose
    /// join is a barker. While iterating its two producers it also runs a nested
    /// query and counts the rows that query returns, then emits both counts.
    ///
    /// Why: this is the only place where two graphs meet on one thread. The join is
    /// iterating `input`, which is fed by two producers, at the same time as it
    /// iterates the stream `highwire.run` gave it, which is fed by a second graph's
    /// stages on other threads. Nothing else in the suite composes those: the
    /// diamond tests are JSON documents and never nest, and the nested-query tests
    /// have linear graphs and never fan in. A wiring mistake here would hang rather
    /// than produce a wrong number, so the failure mode is a stall — which is why
    /// the assertion is on both counts arriving rather than on one.
    ///
    /// LLM section: the seed holds `a` and `b`, so each producer emits two rows and
    /// the join sees four; the nested query walks `a` and `b` again and yields two
    /// rows. The emitted row is therefore `{"joined": 4, "inner": 2}`.
    #[test]
    fn a_join_that_runs_a_nested_query_merges_its_rows() {
        let rows = rows_over_with(
            &[],
            "seed = highwire.walk(name = \"seed\", roots = [\"a\", \"b\"], emit = \"identifier\")\n\
             left = highwire.walk(\n\
             \x20   name = \"left\",\n\
             \x20   roots = [highwire.from_stage(seed, \"@\")],\n\
             \x20   emit = \"{side: 'left', id: identifier}\",\n\
             )\n\
             right = highwire.walk(\n\
             \x20   name = \"right\",\n\
             \x20   roots = [highwire.from_stage(seed, \"@\")],\n\
             \x20   emit = \"{side: 'right', id: identifier}\",\n\
             )\n\
             join_source = \"\"\"\n\
             seen = 0\n\
             for row in input:\n\
             \x20   seen = seen + 1\n\
             inner = highwire.walk(name = 'inner', roots = ['a', 'b'], emit = 'identifier')\n\
             count = 0\n\
             for extra in highwire.run([inner]):\n\
             \x20   count = count + 1\n\
             emit({'joined': seen, 'inner': count})\n\
             \"\"\"\n\
             join = highwire.barker(name = \"join\", source = join_source, inputs = [left, right])\n\
             for row in highwire.run([seed, left, right, join]):\n\
             \x20   emit(row)\n",
            Some(test_cluster(&["a", "b"])),
        );

        let summary = rows
            .iter()
            .find(|row| row.get("joined").is_some())
            .unwrap_or_else(|| panic!("the join emitted a row: {rows:?}"));
        assert_eq!(summary["joined"], json!(4), "both producers arrived: {rows:?}");
        assert_eq!(
            summary["inner"],
            json!(2),
            "the nested query ran and its rows reached the join: {rows:?}"
        );
    }

}
