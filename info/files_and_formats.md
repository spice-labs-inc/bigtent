# Big Tent's Files and Formats

Big Tent is a Graph Database that is based on a high performance Key/Value store.

There are four basic file types in Big Tent:

* Clusters -- pointers to Index and Data files denoted by the `.grc` suffix
* Indexes -- Ordered collections of hashed primary keys, file names, and offsets denoted by the `.gri` suffix
* Data -- files that contain records, denoted by the `.grd` suffix
* `purls.txt` -- the file that contains the [Package URLs](https://github.com/package-url/purl-spec)

## Version support

Big Tent reads cluster versions 3 and 4 and writes version 4 only. Version
4 changes the `Item` connection shape (below), the index key algorithm
(the constant `BLAKE3[0..16]/Long/Long`), and the data envelope version.
As demonstrated by `test_v3_fixture_clusters_load_and_resolve` and
`test_writer_emits_version_4_envelopes`.

## Hashes and File

Cluster, Index, and Data files are named based on the SHA256 of the file.

The SHA256 is computed for the file and the most significant 8 bytes of the hash
are converted to hexadecimal and used as part of the filename.

For Index and Data files, the names are the hex of the most significant
8 bytes with `.gri` or `.grd` suffix. Examples `3d58cbbc64cc612b.gri` and
`38c4ea9bc6b082f2.grd`.

The Cluster file is prefixed by the year, month, day, hour, minute, and
second it was created, the hex of the 8 most significant bytes of the hash,
and the `.grc` suffix: `2024_10_10_17_31_49_13c5aa7216a2bbe6.grc`

Having the timestamp as part of the Cluster file's name allows for
easy identification of the most recent Cluster file in a given directory.

The data structures in the Cluster, Index, and Data files are in
[CBOR](https://cbor.io/) format as CBOR's field ordering is stable
where JSON is not.

## The Cluster File

The Cluster file contains references to all the Index and Data
files for a given Cluster of graphs.

The first 4 bytes of the Cluster file is a "magic number": `ClusterFileMagicNumber: u32 = 0xba4a4a; // Banana`
which rough translation of a Banana pepper. It is stored in [Big Endian](https://en.wikipedia.org/wiki/Endianness) format.

The next two bytes are the length (in Big Endian format) of the Cluster Envelope.

The Cluster Envelope is stored in CBOR format.

The Cluster Envelope is described by this data structure:

```rust
pub struct ClusterFileEnvelope {
  pub version: u32,
  pub magic: u32,
  pub data_files: Vec<u64>,
  pub index_files: Vec<u64>,
  pub info: BTreeMap<String, String>,
  pub encoding: Option<String>,
}
```

The `version` is 4 for current clusters. Readers accept versions 3 and 4
and reject other versions (`test_cluster_envelope_rejects_unknown_versions`,
`test_envelope_version_cross_check`).

The `magic` is equal to `ClusterFileMagicNumber`.

The `data_files` field contains a vector of the most significant 8 bytes of the SHA256 of the Data files. Thus,
the files can be located by the filename based on the Rust string format `{:016x}.grd`.

The `index_files` field contains a vector of the most significant 8 bytes of the SHA256 of the Index files. Thus,
the files can be located by the filename based on the Rust string format `{:016x}.gri`. Ideally, the order of the Index
files in the Cluster file is ascending based on the order of the index entries such that the indexes can be
read into memory in order and no sorting is required.

The `info` field provides a place to store metadata about the Cluster. Currently, that metadata is not consulted
during the operation of Big Tent.

The `encoding` field is the cluster's index key algorithm declaration: for
version 4 clusters it is present and equals `"BLAKE3[0..16]/Long/Long"` —
the first 16 bytes (128 bits) of the BLAKE3 digest over the identifier's
UTF-8 bytes, followed by the two big-endian u64 fields of the index entry
(`test_writer_emits_version_4_envelopes`). Version 3 clusters have no
declaration; readers fall back to the first `.gri` file's hash description
(`test_v3_fixture_clusters_load_and_resolve`). Readers use the algorithm
the files declare, and an unknown declaration fails cluster load
(`test_reader_follows_declared_algorithm`).

## The Index File

The Index file contains a set of the following tuples: MD5 hash of data record primary key, 8 most significant bytes
of file containing the data record, and offset within that file of the data record.

We chose to use MD5 of the primary key because MD5 hashes are fast, the is no concern about using a cryptographic
hash, and MD5 hashes consume 16 bytes.

The format of the Index file is as follows:

The first 4 bytes of the Index file is a "magic number": `IndexFileMagicNumber: u32 = 0x54154170; // Shishitō`. It is
stored in Big Endian format.

The next two bytes are the length of the Index file's envelop (Big Endian) followed by the Index Envelope
stored in CBOR format.

The Index Envelope structure:

```rust
pub struct IndexEnvelope {
    pub version: u32,
    pub magic: u32,
    pub size: u32,
    pub data_files: BTreeSet<u64>,
    pub encoding: String,
    pub info: BTreeMap<String, String>,
}
```

The `version` is unchanged across the format change (the current writer
emits 1); it is the `encoding` string that names the key algorithm.

The `magic` is equal to `IndexFileMagicNumber`.

`size` is the number of Index entries in the file.

`data_files` is the set of files referenced by indexes in this file. Note that this should
be a subset of the `data_files` entry in the Cluster Envelope.

`encoding` is `"BLAKE3[0..16]/Long/Long"` for version 4 clusters and
`"MD5/Long/Long"` for version 3 clusters. Readers use the algorithm the
files declare (`test_reader_follows_declared_algorithm`).

`info` provides a place to store metadata about the Index. Currently, that metadata is not consulted
during the operation of Big Tent.

Then there are `size` records: the 16-byte index key (the declared
algorithm's digest of the identifier — BLAKE3 truncated to 128 bits for
version 4, MD5 for version 3), 8 most significant bytes of the SHA of the
data file, and the offset (u64) of the record within the data file.

## The Data File

The Data File contains the records in Big Tent.

The format of the Data File is as follows:

The first 4 bytes of the file is a magic number: `DataFileMagicNumber: u32 = 0x00be1100; // Bell`
stored in Big Endian format.

A two byte envelope length (Big Endian) followed by the Data File envelope:

```rust
pub struct DataFileEnvelope {
  pub version: u32,
  pub magic: u32,
  pub previous: u64,
  pub depends_on: BTreeSet<u64>,
  pub built_from_merge: bool,
  pub info: BTreeMap<String, String>,
}
```

`version` == 2 for version 4 clusters; version 3 clusters carry data
envelope version 1 (`test_envelope_version_cross_check`).

`magic` == `DataFileMagicNumber`

`previous` and `depends_on` are inert in version 4: the writer always
emits `previous: 0` and an empty `depends_on`, and BigTent neither
maintains nor consults the data-file chain
(`test_writer_emits_version_4_envelopes`).

`built_from_merge` was this Data File built by merging other Data Files together or was it created
"fresh" by a tool like [Goat Rodeo](https://github.com/spice-labs-inc/goatrodeo)

`info` provides a place to store metadata about the Index. Currently, that metadata is not consulted
during the operation of Big Tent.

The balance of the Data File is a series of length fields as u32 Big Endian and `Item` records stored in CBOR format:

```rust
pub struct Item {
    pub identifier: String,
    pub connections: Connections, // ordered map of edge type to target set
    pub body: Option<Value>,
    pub body_mime_type: Option<String>
}
```

`identifier` is the primary key of the record.

`connections`: an ordered map (`BTreeMap<String, BTreeSet<String>>`) of
edge type to the set of target identifiers, so "all connections of type
X" is a single map lookup. Serialization emits sorted keys and sorted
target sets. Deserialization also accepts the version 3 legacy shape —
an array of `(edge type, target)` pairs — folding each target under its
edge type, with duplicates deduplicated; a missing field reads as an
empty map; wrong-arity pairs, non-string elements, and nested arrays are
rejected with an error naming the entry. As demonstrated by
`test_item_v4_cbor_round_trip`, `test_item_legacy_pairs_cbor_deserialize`,
`test_item_missing_connections_field_is_empty_map`,
`test_legacy_connections_malformed_rejected`, and
`test_item_serialize_canonical_deterministic`.

`body`: The optional JSON/CBOR body for this item. Typically it's something like `ItemMetaData`

`body_mime_type`: The mime type for the body. 

The `ItemMetaData` structure contains:

```rust
pub struct ItemMetaData {
    pub file_names: BTreeSet<String>,
    pub file_type: BTreeSet<String>,
    pub file_sub_type: BTreeSet<String>,
    pub file_size: Option<i64>,
    pub extra: BTreeMap<String, BTreeSet<String>>,
}
```

`ItemMetaData` contains data that led to the creation of the graph vertex.

`file_names` contains the names of the files that were hashed to create the metadata record.

`file_type` the types of the files identified by the tool that generated the hash (likely Goat Rodeo)

`file_sub_type` the subtype of the file.

`file_size`: the size of the file the resulted in the creation of this `Item`. Note that this is a mistake... `file_size` should be in the `ItemMetaData` and this will be corrected in a future version of Big Tent.

`extra` a set of additional information about the file that represents this vertex.

Note the use of `BTreeSet`s This is to ensure that `ItemMetaData` records can be merged together losslessly and that
the ordering of the keys and other information is preserved.


## HTTP Item Wire Shapes

The HTTP API emits `Item` objects in two wire shapes. The **map shape**
(version 4) is the default: `connections` is a JSON object mapping edge
type to an array of target identifiers. Passing `?item_format=v3` on any
item-emitting endpoint selects the **legacy pair shape**: `connections`
is a JSON array of two-element `[edge_type, target]` arrays, shaped like
the version 3 output. As demonstrated by `test_item_default_shape_is_map`
and `test_item_format_v3_shape_is_legacy_pairs`.

* Accepted values: absent, `v4`, `v3`. Anything else — including wrong
  case, empty, and conflicting duplicate parameters — is rejected with
  400 and a static message naming the accepted values
  (`test_item_format_invalid_rejected`).
* Identical duplicate parameters are accepted
  (`test_item_format_invalid_rejected`).
* Endpoints that emit items honor the parameter: `/item/{gitoid}`,
  `/item`, `POST /bulk`, all `/aa` forms, and the full-item `/north`
  forms (`test_item_format_applies_to_bulk`,
  `test_item_format_applies_to_aa_endpoints`,
  `test_item_format_applies_to_north_full_items`).
* Endpoints that emit identifier strings (`/flatten`, `/flatten_source`)
  and metadata endpoints (`/purls`, `/node_count`, `/health`) are
  unaffected; passing the parameter there is accepted and has no effect
  (`test_flatten_returns_identifiers_regardless_of_item_format`,
  `test_identifier_streams_unaffected_by_item_format`).
* Error responses are static strings; no parser output, filesystem
  paths, or internal type names are echoed
  (`test_item_format_invalid_rejected`).
* Both shapes are documented in `/openapi.json` from a running server —
  the only specification (no static spec file is checked in);
  `test_openapi_schema_contains_both_shapes`.
* The two shapes are two views of the same edges — semantically equal
  (`prop_default_and_v3_responses_are_semantically_equal`).
