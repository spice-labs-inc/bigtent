# Writing a query

A query is a program that reads a graph and produces rows. This page is about
writing one; the parts of the system it runs on are described in
[architecture](architecture.md).

## The shape of a query

A query documents one or more **stages**. A stage is either a **walk** — a
traversal of the graph — or a **program** — Starlark source that reads rows and
emits rows. The two have the same shape at their edges: rows or constants in,
rows out.

```json
{
  "phases": [
    {"name": "jars", "roots": ["pkg:maven/…"], "walk": ["alias:to"],
     "emit": {"expr": "{id: identifier}", "if": "…"}},
    {"name": "classes", "roots": {"jars": "id"}, "walk": ["contained:down"],
     "emit": {"expr": "{id: identifier, jars: length(connections.\"contained:up\")}"}},
    {"name": "summary",
     "barker": "for row in input:\n    emit(row[\"jars\"])\n",
     "inputs": ["classes"]}
  ]
}
```

A single-stage query is a walk document without the `phases` wrapper.

## Stages are named, and a name is how one stage feeds another

A walk says where it starts with `roots`:

- a **list** of identifiers is enumerated — the walk starts from exactly those;
- an **object** names upstream stages and gives, for each, the expression that
  pulls identifiers out of that stage's rows: `{"jars": "id"}` reads the `id`
  field of each row `jars` emitted.

A program says where it reads with `inputs`, a list of stage names. Its rows are
the upstream stages' rows, unchanged — a program reads the row itself, where a
walk reads only the identifier projected out of it.

**The expression is applied to the row, not to the item.** A stage that emits a
bare string — `"emit": "identifier"` — produces rows that *are* the identifier, so
a downstream stage reads them with `"@"`. A stage that emits an object reads them
with a field name. Getting this wrong does not fail: the projection yields
nothing, the downstream stage starts from an empty root set, and the query emits
no rows.

## `tee`, `unnest` and `dedup`

By default a stage's rows go to the stages downstream of it and nowhere else: the
middle of a pipeline is usually not wanted as output as well as consumed. `"tee":
true` on a stage sends its rows to the caller as well. A stage that nothing consumes
is the run's output whether it is teed or not — the declaration order says nothing
about which stage that is.

`"unnest": true` makes an array a row evaluates to produce one row per element, one
level only.

`"dedup": true` emits a repeated row once. A row this stage has already emitted is
dropped, compared by a 16-byte BLAKE3 digest of the row's canonical form — object
keys sorted — so two rows with equal fields dedupe whatever order they were built
in.

**`dedup` is cheapest on identifiers.** A string row is hashed over its own bytes,
with no copy — a string's rendering is the string — so a stream of identifiers costs
one digest per row. A row that is an object is rendered canonically first, which
allocates. Both are exact; the difference is what it costs.

## Writing a program

A program is Starlark. It sees two things:

- **`input`** — the rows of its `inputs`, as a stream. Iterate it once; it is a
  stream, not a list, so it has no `len()` and cannot be re-read.
- **`emit(row)`** — hand one row on. `emit` may be called any number of times,
  including zero, and after any amount of work.

```python
counts = []
for row in input:
    counts.append(row["jars"])

n = len(counts)
total = 0
for c in counts:
    total = total + c

emit({"n": n, "total": total, "mean": total / n})
```

A program that fails emits a row naming the failure rather than ending quietly:

```json
{"error": "error: Variable `nope` not found …"}
```

That row is not a string, so a downstream walk does not mistake it for an
identifier — which is the general rule for what crosses a stage edge.

### `unnest`

A program that emits a list emits it as **one row** by default. `"unnest": true`
on the program stage makes each element its own row, one level only — the same
meaning the flag has on a walk's emit.

## The functions a program can call

Big Tent's builtins compose with the standard library and with any natives the
embedding application adds. They are grouped into namespaces.

### `math`

| function | |
|---|---|
| `ceil(x)`, `floor(x)`, `round(x)`, `trunc(x)` | rounding, all returning floats |
| `pow(x, y)`, `sqrt(x)`, `exp(x)`, `log(x, base?)` | powers and logarithms |
| `fabs(x)`, `mod(x, y)`, `copysign(x, y)` | magnitude and remainder |
| `acos`, `asin`, `atan`, `atan2`, `cos`, `sin`, `tan` | trigonometry, in radians |
| `acosh`, `asinh`, `atanh`, `cosh`, `sinh`, `tanh` | hyperbolic |
| `hypot(x, y)`, `radians(x)`, `degrees(x)` | |
| `e`, `pi` | the constants |

`trunc` is not `floor` for negative numbers: `trunc(-1.5)` is -1 and
`floor(-1.5)` is -2.

### `stats`

| function | |
|---|---|
| `mean(xs)`, `median(xs)` | |
| `variance(xs)`, `stdev(xs)` | population — divides by n |
| `sample_variance(xs)`, `sample_stdev(xs)` | the estimator — divides by n-1 |
| `percentile(xs, p)` | `p` from 0 to 100, interpolating between ranks |
| `tally(xs)` | a dict of value to count |
| `clamp(x, low, high)`, `sign(x)`, `round_to(x, digits)` | |
| `is_nan(x)`, `is_infinite(x)`, `is_finite(x)`, `approx_eq(a, b, tol)` | |

`round_to` is what a report wants: `round_to(2.3333333333333335, 2)` is `2.33`.

`is_finite` matters because JSON has no NaN and no infinity — a program that
computes one cannot emit it, and testing first is how a row stays representable.
Such a value *reaches* `stats.is_nan`, because the conversion a native applies to
its arguments reads the float directly rather than through JSON.

### `collections`

`unique(xs)`, `flatten(xs)` (one level), `chunks(xs, n)`, `group_by(xs, key)`,
`any_of(xs, pred)`, `all_of(xs, pred)`, `count_if(xs, pred)`,
`find_first(xs, pred)`.

The last four take a function: `count_if([1, 2, 3], lambda x: x > 1)` is `2`.

### `patterns`

`matches(s, pattern)`, `find_all(s, pattern)`, `replace_re(s, pattern, repl)` —
Rust `regex` syntax — plus `hex_encode(s)`, `hex_decode(s)` and
`base64url_encode(s)`.

### `digest`

| function | produces |
|---|---|
| `sha1(s)`, `sha256(s)`, `sha512(s)`, `md5(s)` | the bare digest of the bytes, as hex |
| `blake3(s)` | the bare BLAKE3 digest — the one hash the `coordinates` crate does not define |
| `gitoid_sha1(s)`, `gitoid_sha256(s)` | `gitoid:blob:sha1:<hex>` — the digest of `"blob <len>\0"` + the content |
| `swh_sha1(s)`, `swh_sha256(s)` | `swh:1:cnt:<hex>` — the **same** digest as the gitoid, under Software Heritage's prefix |

A gitoid is **not** the digest of the content. A script that hashes the content
where a gitoid was meant gets a hex string that matches nothing in the corpus, and
both are 64 hex digits. The empty content makes the difference visible:
`sha256("")` is `e3b0c442…` while `gitoid_sha256("")` is a different digest
entirely, because of the header.

A SWHID for content is the **git blob digest** — the same hash as the gitoid —
which is easy to get wrong, because Software Heritage calls the quantity
`sha1_git` rather than "the git blob id". For `"hello\n"`:

```
gitoid:blob:sha1:ce013625030ba8dba906f756967f9e9ca394464a
swh:1:cnt:      ce013625030ba8dba906f756967f9e9ca394464a
```

Same hash, different prefix. The bare `sha1("hello\n")` is `f572d396…`, which is
*not* the SWHID.

Every hash comes from [coordinates](https://github.com/spice-labs-inc/coordinates) except
`blake3`, which that crate does not calculate.

### `dates`

A date is a **canonical RFC 3339 string** — fixed width, UTC, `Z` — and a duration
is **milliseconds as an integer**.

| function | |
|---|---|
| `now_ms()`, `now()` | the current instant, as epoch milliseconds and as canonical text |
| `parse_date(s)`, `is_date(s)` | canonicalise, and test without raising |
| `to_millis(s)`, `from_millis(ms)` | between the two representations |
| `year(s)`, `month(s)`, `day(s)`, `hour(s)`, `minute(s)`, `second(s)`, `millisecond(s)`, `weekday(s)` | components; `weekday` is 0 for Monday |
| `add_milliseconds(s, ms)`, `add_days(s, n)` | |
| `milliseconds_between(a, b)`, `in_range(s, start, end)` | |
| `format_date(s, pattern)` | `strftime`, for `%Y-%m-%d` |

Because the form is canonical, **comparing two dates as strings compares them in
time** — `"2026-10-16T11:00:00.000Z" < "2026-10-16T12:00:00.000Z"` is the same
answer as comparing the instants. That is why a date needs no value type to be
sorted or tested in a filter.

Measuring duration reduces to integer arithmetic:

```python
t0 = now_ms()
... work ...
emit({"elapsed_ms": now_ms() - t0})
```

### Not available

`load` cannot reach the filesystem: the dialect enables it and no loader is
configured, so it is an error rather than a file read.

## Serving a query over HTTP

The V2 surface serves the named queries at `/v2/query`:

```
GET  /v2/query?identifier=…&template=flatten
POST /v2/query  {"roots": ["…"], "template": "flatten"}
POST /v2/query?template=flatten  {"roots": ["…"]}
POST /v2/query  {"roots": ["…"], "query": { <a full query document> }}
GET  /v2/query?identifier=…          (no template: the default)
```

On both verbs the template's name resolves against the server's registry —
the same registry for GET and POST — so a template your application added
with `with_template()` runs on both, and one removed with `without_template()`
runs on neither. On POST the body's `template` field wins when the query
parameter is also given.

A registry is **empty by default** — the built-ins are available to add, not
forced upon the server. `TemplateRegistry::new()` starts with nothing; add the
built-ins with `with_builtins()`, your own with `with_template()`, and take any
of them away with `without_template()` — a built-in included. Every one takes
its roots from the input stream, so the text never names an identifier.

The output is **JSON lines**: one row per line, a stop's report marked
`{"stop": …}`, a failure `{"error": …}`. The response streams as the pipeline
produces, and a client that stops reading backpressures the run — every channel
in the pipeline is bounded.

An embedding application defines its own templates and designates the default at
construction:

```rust
let templates = TemplateRegistry::new()
    .with_builtins()                       // or not — the server decides
    .with_template("sizes", r#"{"roots": {"from-input": true}, …}"#)
    .without_template("north")             // take a built-in away
    .with_default("sizes")?;
let state = V2State::with_templates(holder, cache, templates)?;
```

A caller's template **replaces** a built-in of the same name — the name is the
identifier of the query. An unknown default is refused at construction, so a
misconfigured server fails to start rather than failing every request that
omitted a template. An inline `query` document is compiled per request and NOT
cached, because a cache keyed on caller-controlled text is a memory leak the
caller controls.

There is no authentication: these endpoints run arbitrary traversal and
arbitrary Starlark against the cluster, and exposing them is a trust decision
for whoever serves them.

## Running a query from inside a script

A program can build and run a query of its own, with the `highwire` namespace. The
principle is **structure is a literal, cross-references are values**: the levels
that are trees are written as they are in a document, and the levels that refer to
another stage are built by value, so a stage is never named by a string.

```python
jars = highwire.walk(
    name = "jars",
    roots = ["pkg:maven/uk.co.real-logic/artio-core@0.118"],
    traverse = ["alias:to"],
    emit = "{id: identifier}",
)

classes = highwire.walk(
    name = "classes",
    roots = [highwire.from_stage(jars, "id")],
    traverse = [highwire.connection("contained:down", gate = "start:BOOT-INF")],
    emit = "{id: identifier, jars: length(connections.\"contained:up\")}",
)

for row in highwire.run([jars, classes]):
    emit(row["jars"])
```

| function | |
|---|---|
| `highwire.walk(name, roots, emit, traverse=[], filter=None, stop=None, tee=False)` | a walk stage |
| `highwire.barker(name, source, inputs=[], tee=False, unnest=False)` | a barker stage; `source` is Starlark text |
| `highwire.from_stage(stage, expression)` | a root derived from another stage's rows |
| `highwire.connection(type, gate=None)` | a connection to traverse, with its gate |
| `highwire.run(stages)` | run them and return the rows |

A stage value is what `run` collects and what `from_stage` and `inputs` take. The
builders render the same document the JSON reader parses, so a query built in a
script and the same query written as JSON are the same query.

**Everything is lazy.** `run` returns a rows value and does nothing until it is
iterated: the document is compiled, and the graph's stages started, on the first
pull. A script that builds a query and never reads it pays nothing.

Three checks happen at construction rather than at compile time, because the
mistake is in the script: a missing `emit`, roots that mix identifiers with
`from_stage` values, and two stages with one name.

## Testing a query

Compile it first: a malformed document is refused with a located error naming the
stage and the level, such as

```
at the query -> phase `classes` -> its root: a root draws on `jars`, and no stage is named that
```

A program's syntax is checked at compile time too, so a typo in the script is a
compile failure rather than a row.
