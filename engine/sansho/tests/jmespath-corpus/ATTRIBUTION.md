# Attribution and license status of the vendored corpus

The files under `compliance/` are the JMESPath compliance corpus,
vendored from the upstream repository
[github.com/jmespath/jmespath.test](https://github.com/jmespath/jmespath.test)
at commit `53abcc37901891cf4308fcd910eab287416c4609` (fetched
2026-09-11; see `PIN.txt`).

## License status of the corpus

The upstream corpus repository declares **no license**: it carries no
LICENSE or COPYING file, its files contain no copyright notices, and
GitHub reports no license for it. The corpus is used here as conformance
test data **by courtesy of the upstream project** — it is the shared
conformance suite maintained for JMESPath implementations generally, and
it is treated accordingly:

- this directory is test data for Sansho's conformance harness;
- the corpus is **not redistributed as product**: when the `sansho`
  crate is published, `tests/` is excluded from the published package,
  so the corpus ships in this repository's test suite only;
- the pin (`PIN.txt`) records exactly what was vendored and when, so
  the provenance of every corpus case is auditable.

## Reference: the JMESPath implementation's license

For contrast and reference: the Rust JMESPath implementation, the
[`jmespath`](https://crates.io/crates/jmespath) crate on crates.io
(jmespath.rs), is **MIT-licensed**. Sansho's own parser is written from
the JMESPath specification published at
[jmespath.org](https://jmespath.org) and copies no code from any
implementation, so no attribution is carried in Sansho's source files.
Were code ever copied from an MIT-licensed source, the MIT copyright and
permission notice would be preserved in the copied files, as the license
requires.
