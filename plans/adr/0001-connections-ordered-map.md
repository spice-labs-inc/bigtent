# ADR 0001: Item Connections as an Ordered Map of Edge Type to Target Set

* Status: Proposed
* Deciders: project owner

## Human-readable record

### Context

`Item.connections` is a `BTreeSet<(String, String)>` of (edge type, target
identifier) pairs, so finding all connections of one type means walking the
set. `.grd` files are content-addressed, so iteration order must be stable.

### Decision

`Item.connections` becomes `BTreeMap<String, BTreeSet<String>>`: edge type
to set of target identifiers. The legacy `BTreeSet<(String, String)>` folds
into it directly: for every `(edge type, target)` pair, the target is
inserted into the set for that edge type. Serialization emits the map with
keys and target sets in sorted order. Deserialization accepts both the
legacy pair array and the new map, applying the same fold to legacy input.
Merge unions target sets per edge type. The legacy view remains public as
`ItemV3`, with `Item::to_v3()`, `From<&Item> for ItemV3`, and
`From<ItemV3> for Item`.

### Consequences

Positive: per-type lookup is one map lookup and enumeration is proportional
to that type's targets; existing legacy JSON stays readable; one in-memory
representation. Neutral: identifiers and traversal behavior are unchanged,
and the file-naming algorithm is unchanged.

## LLM summary

connections = `BTreeMap<String, BTreeSet<String>>`; legacy pairs fold in by
inserting each target under its edge type; serialization is the sorted map;
deserialization accepts legacy pairs and the new map; merge unions per edge
type; `ItemV3` emits the legacy shape.