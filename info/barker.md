# Barker — Starlark in the pipeline

Barker is [Starlark](https://github.com/facebook/starlark-rust) — the hermetic
Python dialect used by build systems — embedded in Big Tent as a stage of a
query. This page is about embedding and running Barker; the query-writing side is
[Writing a Query](writing_queries.md).

## What Barker is

A program that reads a **stream of rows** and emits a **stream of rows**:

```
rows in ──▶ a Starlark program ──▶ rows out
```

It is an element in a pipeline — a query stage between two walks — and it can
start pipelines of its own, by building and running a query with the `highwire`
namespace from inside the script.

The language is Starlark's restricted subset: **no recursion, no `while`, no
`import`, no classes, no I/O**. A program reaches the world only through the
natives Big Tent and the embedding application provide.

## Running one

Three phases, separate:

```rust
use bigtent::barker;

// 1. parse — on the caller's thread; a syntax error is returned, never streamed
let program = barker::parse("for row in input:\n    emit(row)\n")?;

// 2+3. run — forked onto its own thread; rows stream back
let rows = barker::run(program, input_receiver)?;

for row in rows {
    println!("{}", row?);
}
```

- **`parse(&str) -> Program`** — parses and nothing else. A malformed script is
  the caller's error to handle, not a row on a stream.
- **`run(program, input) -> Rows`** — forks the program onto its own thread and
  returns immediately. `input` is a `flume::Receiver<serde_json::Value>`; `Rows`
  is an iterator over the emitted rows, read while the program is still running.
- **`run_with(program, input, lookup, natives)`** — the same, with a cluster (so
  the script can run a nested query) and the embedder's own natives.

Streaming, both ends: the program *pulls* each input row from the channel when it
iterates — nothing is collected first — and every `emit` hands the row on as it is
produced. A program that fails emits a row naming the failure
(`{"error": "…"}`) rather than ending silently.

## The evaluator's threading constraint

The Starlark evaluator is arena-bound and **not `Send`**: it cannot be built on
one thread and run on another. So a program is parsed anywhere and *run* on the
thread that built its evaluator, with the rows travelling over channels — which is
why `run` forks, and why `Rows` is a channel reader rather than a `Vec`.

The input value the script iterates is a custom Starlark value wrapping the
receiver: each `iter_next` pulls one row and converts it. It cannot be frozen, so
a script that binds `input` into a module global is refused rather than silently
materialised.

## What a program can call

| namespace | contents |
|---|---|
| *(builtins)* | `emit(row)` — hand a row on; `input` — the row stream |
| `math` | rounding, powers, roots, trigonometry, `e`, `pi` |
| `stats` | `mean`, `median`, `stdev`, `percentile`, `tally`, float guards |
| `collections` | `unique`, `flatten`, `chunks`, `group_by`, `count_if`, … |
| `patterns` | regex `matches`/`find_all`/`replace_re`, hex and base64url |
| `digest` | `sha1`/`sha256`/`sha512`/`md5`, `gitoid_*`, `swh_*` |
| `dates` | RFC 3339 dates, epoch-millisecond durations, ranges, formatting |
| `highwire` | build and run a query from inside the script |

The full function tables are in [Writing a Query](writing_queries.md).

## Adding natives

An embedding application's functions compose with Big Tent's:

```rust
use starlark::environment::GlobalsBuilder;
use starlark::starlark_module;

#[starlark_module]
fn my_natives(builder: &mut GlobalsBuilder) {
    /// Double a number.
    fn double(x: i32) -> anyhow::Result<i32> {
        Ok(x * 2)
    }
}

let rows = barker::run_with(
    barker::parse("emit(double(21))\n")?,
    input_receiver,
    None,                    // no cluster: the script cannot run a nested query
    Some(my_natives),        // the embedder's functions, beside Big Tent's
)?;
```

Composition order: the Starlark standard library, then Big Tent's natives and
namespaces, then the embedder's. There is no other plugin surface — a native is
an ordinary Rust function.

## Boundedness

A Barker program is Turing-complete within its limits, and **no determinism
promise is made** — the deterministic tiers are the walk engine and the JMESPath
expressions. What bounds a program is the language's restrictions plus whatever
resource limits the embedding application enforces; resource accounting is not yet
wired into the run loop.

`load` cannot reach the filesystem: the dialect enables it and no loader is
configured, so it is an error rather than a file read.
