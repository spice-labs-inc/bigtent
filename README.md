# Big Tent

It takes a lot to serve millions of GitOIDs.

It takes keeping the GitOID index in memory.

It takes a tad bit of being clever.

And that's Big Tent.

## Using BigTent

Big Tent is both a rust crate, usable to read ADGs from files, and a server with an API for accessing that data.

## Running as a server

Create a `config.toml` file pointing to the OmniBOR Corpus:

```toml
# A string path to the root of the data storage
root_dir = "/path/to/data/storage"
# a string path to the cluster directory (parent of the cluster_path= `.grc` file) within the root data directory.
cluster_path = "/path/to/data/storage/clustername"
```

Then run the service:

```shell
cargo run -- -c config.toml -t 33 --host localhost --host 127.0.0.1
```

If you change the `config.toml` file, send a `SIGHUP` (`kill -HUP <big_tent_process>`).

To create an OmniBOR Corpus, use [Goat Rodeo](https://github.com/spice-labs-inc/goatrodeo).

## Design docs

In the [info](info/README.md) directory.

## Stuff

[Matrix Discussion](https://matrix.to/#/#spice-labs:matrix.org)

License: Apache 2.0
