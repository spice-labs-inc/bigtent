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


## Endpoints

The following are Big Tent's endpoints:

* `POST /omnibor/bulk` -- POST an array of Strings to get the bulk `Item`s.
* `GET /omnibor/{*gitoid}` or `GET /omnibor/?identifier=gitoid"` -- get a single `Item`. Note
   in the URL form, the identifier (`{*gitoid}`) is _not_ URL encoded. This allows for
   copy/pasting of Package URLs without any escaping.
* `GET /omnibor/aa/{*gitoid}` or `GET /omnibor/aa/?identifier=gitoid"` -- get a single `Item`. If the
  `Item` contains an `alias:to` in its connection, follow the alias.
   In the URL form, the identifier (`{*gitoid}`) is _not_ URL encoded. This allows for
   copy/pasting of Package URLs without any escaping.
* `GET /omnibor/north/{*gitoid}` or `GET /omnibor/north/?identifier=gitoid"` -- get all the `Items` connected to
  the found item via `build:up`, `alias:to`, and `contained:up`. This can be a large list.
  In the URL form, the identifier (`{*gitoid}`) is _not_ URL encoded. This allows for
  copy/pasting of Package URLs without any escaping.
* `POST /omnibor/north` -- The POST body contains a JSON array of Strings that are the identifiers of the
  root `Item`s.  Get all the `Items` connected to
  the found/root items via `build:up`, `alias:to`, and `contained:up`. This can be a large list.
  In the URL form, the identifier (`{*gitoid}`) is _not_ URL encoded. This allows for
  copy/pasting of Package URLs without any escaping.
* `GET /omnibor/north_purls/{*gitoid}` or `GET /omnibor/north_purls/?identifier=gitoid"` -- get all the `Items` connected to
  the found item via `build:up`, `alias:to`, and `contained:up`. Return only the `alias:from` connections
  In the URL form, the identifier (`{*gitoid}`) is _not_ URL encoded. This allows for
  copy/pasting of Package URLs without any escaping.
* `GET /purls` -- return all the Package URLs in the Artifact Dependency Graph.
* `GET /node_count` -- return the count of all the nodes in the Artifact Dependency Graph.

## Design docs

In the [info](info/README.md) directory.

## Stuff

[Matrix Discussion](https://matrix.to/#/#spice-labs:matrix.org)

License: Apache 2.0
