# ADR 0003: Version 4 Compatibility and Mixed-Version Merge

* Status: Proposed
* Deciders: project owner

## Human-readable record

### Context

The version 4 format changes the in-memory and on-disk item shape, so
existing version 3 clusters must remain readable and mergeable.

### Decision

A cluster has exactly one version; version 3 and version 4 clusters may
coexist in one herd. Readers accept version 3 and version 4; writers write
version 4 only. Mixed-version merges are supported, and their output is
always version 4.

The output history names the original input clusters: it contains the
verbatim histories of the inputs and the normal merge marker, and internal
processing artifacts never appear in it. When an input cluster was
converted as part of the merge, the history carries one marker recording
that conversion and the BigTent commit that performed it.

### Consequences

Positive: legacy clusters remain readable and mergeable; provenance stays
attributable — a reader sees the original clusters' histories plus, where
conversion occurred, an explicit marker naming the BigTent commit that
performed it. Negative: mixed-version merges do more work than
same-version merges.

## LLM summary

One version per cluster; readers accept v3 and v4; writers write v4 only;
mixed-version merges are supported and always output v4. Output history =
original input histories verbatim + the merge marker naming original
clusters; when an input was converted, one marker records the conversion
and the BigTent commit that performed it; internal processing artifacts
never appear in history.