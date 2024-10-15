# Big Tent's Files and Formats

Big Tent is a Graph Database that is based on a high performance Key/Value store.

There are five basic file types in Big Tent:

* Clusters -- pointers to Index and Data files denoted by the `.grc` suffix
* Indexes -- Ordered collections of hashed primary keys, file names, and offsets denoted by the `.gri` suffix
* Data -- files that contain records, denoted by the `.grd` suffix
* `purls.txt` -- the file that contains the [Package URLs](https://github.com/package-url/purl-spec)
* `config.toml` -- the configuration file that points to the Cluster files

## Hashes and File

Cluster, Index, and Data files are named based on the SHA256 of the file.

The SHA256 is computed for the file and the most significant 8 bytes of the hash
are converted to hexidecimal and used as part of the filename.

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
}
```

The `version` should be 1.

The `magic` is equal to `ClusterFileMagicNumber`.

The `data_files` field contains a vector of the most significant 8 bytes of the SHA256 of the Data files. Thus,
the files can be located by the filename based on the Rust string format `{:016x}.grd`.

The `index_files` field contains a vector of the most significant 8 bytes of the SHA256 of the Index files. Thus,
the files can be located by the filename based on the Rust string format `{:016x}.gri`. Ideally, the order of the Index
files in the Cluster file is ascending based on the order of the index entries such that the indexes can be
read into memory in order and no sorting is required.

The `info` field provides a place to store metadata about the Cluster. Currently, that metadata is not consulted
during the operation of Big Tent.

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
    pub data_files: HashSet<u64>,
    pub encoding: String,
    pub info: BTreeMap<String, String>,
}
```

The `version` should be 1.

The `magic` is equal to `IndexFileMagicNumber`.

`size` is the number of Index entries in the file.

`data_files` is the set of files referenced by indexes in this file. Note that this should
be a subset of the `data_files` entry in the Cluster Envelope.

`encoding` is currently `"MD5/Long/Long"`. If the hash or other formats change, the encoding may vary.

`info` provides a place to store metadata about the Index. Currently, that metadata is not consulted
during the operation of Big Tent.

Then there are `size` records: MD5 hash (16 bytes), 8 most significant bytes of the SHA of the data file,
and the offset (u64) of the record within the data file.

## The Data File

The Data File contains the records in Big Tent.

The format of the Data File is as follows:

The first 4 bytes of the file is a magic number: `DataFileMagicNumber: u32 = 0x00be1100; // Bell`
stored in Big Endian format.

A two byte envelope lentgh (Big Endian) followed by the Data File envelope:

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

`version` == 1

`magic` == `DataFileMagicNumber`

`previous` contains the most significant 8 bytes of the hash of the previous data file in
a particular set of data files. This allows for the potential recostruction of Index and Cluster
files from a set of data files.

`depends_on` when Big Tent databases are merged, there is a history kept of the previous data records
that are merged into a new data record. The `depends_on` field lists all the other Data Files that
resulted in the records merged into this Data File.

`built_from_merge` was this Data File built by merging other Data Files together or was it created
"fresh" by a tool like [Goat Rodeo](https://github.com/spice-labs-inc/goatrodeo)

`info` provides a place to store metadata about the Index. Currently, that metadata is not consulted
during the operation of Big Tent.

The balance of the Data File is a series of length fields as u32 Big Endian and `Item` records stored in CBOR format:

```rust
pub struct Item {
    pub identifier: String,
    pub reference: LocationReference,
    pub connections: BTreeSet<Edge>,
    pub metadata: Option<ItemMetaData>,
    pub merged_from: BTreeSet<LocationReference>,
    pub file_size: i64,
}
```

`identifier` is the primary key of the record.

`reference` is a tuple of `u64` containing the 8 most significant bytes of the the SHA256 of the file (note that this will be 0 on disk and is populated by
  Big Tent when serving the record) and the offset of the record in the file (technically, the offset to the length field).

`connections`: an ordered list of a tuple of `EdgeType` and `String` where `EdgeType` is an enumeration of `AliasTo`, `AliasFrom`, `Contains`, `ContainedBy`,
  `BuildsTo`, and `BuiltFrom`. It's ordered to ensure reproducibility.

`merged_from`: an ordered list of the `Items` that were merged into this `Item`.

`file_size`: the size of the file the resulted in the creation of this `Item`. Note that this is a mistake... `file_size` should be in the `ItemMetaData` and this will be corrected in a future version of Big Tent.

`metadata`: The `ItemMetaData` referenced by this `Item`.

The `ItemMetaData` structure contains:

```rust
pub struct ItemMetaData {
    pub file_names: BTreeSet<String>,
    pub file_type: BTreeSet<String>,
    pub file_sub_type: BTreeSet<String>,
    pub extra: BTreeMap<String, BTreeSet<String>>,
}
```

`ItemMetaData` contains data that led to the creation of the graph vertex.

`file_names` contains the names of the files that were hashed to create the metadata record.

`file_type` the types of the files identified by the the tool that generated the hash (likely Goat Rodeo)

`file_sub_type` the subtype of the file.

`extra` a set of additional information about the file that represents this vertex.

Note the use of `BTreeSet`s This is to ensure that `ItemMetaData` records can be merged together losslessly and that
the ordering of the keys and other information is preserved.

