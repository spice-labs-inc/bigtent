# Big Tent

It takes a lot to serve millions of GitOIDs.

It takes keeping the GitOID index in memory.

It takes a tad bit of being clever.

And that's Big Tent.

To run: `cargo run -- -c config.toml -t 33 --host localhost --host 127.0.0.1`

In the `config.toml` file, point to the OmniBOR Corpus:

```
oc_file='~/info_repo_ae.txt'
```

If you change the `config.toml` file, send a `SIGHUP` (`kill -HUP <big_tent_process>`).

To create an OmniBOR Corpus, use [Goat Rodeo](https://github.com/spice-labs-inc/goatrodeo).

## Design docs

In the [info](info/README.md) directory.


## Stuff

[Matrix Discussion](https://matrix.to/#/#spice-labs:matrix.org)


License: Apache 2.0
