"""Generate the walk fixture: a V4 cluster, its purls list, and a second JSON set.

The graph is the topology the specification describes, at a size a reader can hold
in their head:

  pkg:maven/... --alias:to--> jar --contained:down--> class

Cases it deliberately contains (see the test's documentation):

  - a maven purl aliased to one jar, and another purl aliased to the same jar, so
    a jar reached twice is visited once
  - `alias:from` on the jar, the reverse edge
  - a class in TWO jars (the distribution's core case) and a class in ONE jar
  - a nested jar (BOOT-INF/lib/...), so containment goes two levels
  - a deb purl with NO body, the one non-maven case
  - a `tag:from` edge on a source item, an edge type the walk's code never names

The .grc/.gri/.grd are produced by the real writer, so they are a V4 cluster and
exercise the encoder, the index and the decoder as well as the walk.
"""
import hashlib
import json
import os
import struct

OUT = "/data/test_data/walk_v4"
CLUSTER_NAME = "walk"

# ---------------------------------------------------------------- the items


def ident(kind, text):
    """The corpus's identifier forms."""
    digest = hashlib.sha256(text.encode()).hexdigest()
    if kind == "jar":
        return f"gitoid:blob:sha256:{digest}"
    if kind == "class":
        return f"gitoid:blob:sha256:{digest}"
    if kind == "tag":
        return f"tag:sha256:{hashlib.sha1(text.encode()).hexdigest()}"
    raise ValueError(kind)


PURL_A = "pkg:maven/uk.co.real-logic/artio-core@0.118"
PURL_B = "pkg:maven/uk.co.real-logic/artio-codecs@0.118"
PURL_C = "pkg:deb/debian/tk8.6@8.6.14-1build1?arch=amd64"

JAR_A = ident("jar", "artio-core-0.118.jar")
NESTED_JAR = ident("jar", "nested-javassist-3.21.0-GA.jar")
JAR_B = ident("jar", "artio-codecs-0.118.jar")

# a class in two jars, and one in only one
SHARED = ident("class", "artio/EncodingException.class")
ONLY_IN_A = ident("class", "artio/core/CoreOnly.class")
INSIDE_NESTED = ident("class", "javassist/expr/FieldAccess.class")

TAG = ident("tag", "artio")

JAVA_ARCHIVE = ["application/java-archive"]
JAVA_VM = ["application/java-vm"]


def item(identifier, connections, mime=None, file_names=None, size=None):
    body = None
    if mime is not None:
        body = {
            "extra": {},
            "file_names": file_names or [identifier],
            "file_size": size if size is not None else 0,
            "mime_type": mime,
        }
    # `body_mime_type` is ONE string; the list of types lives inside the body,
    # alongside the file names and size
    return {
        "identifier": identifier,
        "connections": connections,
        "body_mime_type": mime[0] if mime else None,
        "body": body,
    }


items = [
    # the two maven purls, each naming a jar, and the deb purl with no body
    item(PURL_A, {"alias:to": [JAR_A]}, [], [PURL_A]),
    item(PURL_B, {"alias:to": [JAR_A]}, [], [PURL_B]),
    item(PURL_C, {}),
    # the jar: alias:from back to both purls, contained:down to its classes and to
    # the nested jar
    item(
        JAR_A,
        {
            "alias:from": [PURL_A, PURL_B],
            "contained:down": [SHARED, ONLY_IN_A, NESTED_JAR],
            "contained:up": [],
        },
        JAVA_ARCHIVE,
        ["repo_ea/artio-core-0.118.jar"],
        513213,
    ),
    # the second jar, so SHARED has arity 2 and ONLY_IN_A has arity 1
    item(
        JAR_B,
        {"contained:down": [SHARED], "contained:up": []},
        JAVA_ARCHIVE,
        ["repo_ea/artio-codecs-0.118.jar"],
        412000,
    ),
    # a jar nested inside JAR_A: contained:up names its container, so containment
    # is two levels deep
    item(
        NESTED_JAR,
        {"contained:down": [INSIDE_NESTED], "contained:up": [JAR_A]},
        JAVA_ARCHIVE,
        ["BOOT-INF/lib/javassist-3.21.0-GA.jar"],
        741_000,
    ),
    # the classes
    item(SHARED, {"contained:up": [JAR_A, JAR_B]}, JAVA_VM, ["artio/EncodingException.class"], 1_234),
    item(ONLY_IN_A, {"contained:up": [JAR_A]}, JAVA_VM, ["artio/core/CoreOnly.class"], 2_345),
    item(
        INSIDE_NESTED,
        {"contained:up": [NESTED_JAR]},
        JAVA_VM,
        ["javassist/expr/FieldAccess.class"],
        3_456,
    ),
    # a source item carrying a tag:from edge, which no walk in these tests follows
    # but the corpus has and a query could
    item(TAG, {"tag:from": [JAR_A]}, []),
]

# ---------------------------------------------------------------- purls.txt

purls = [PURL_A, PURL_B, PURL_C]
os.makedirs(OUT, exist_ok=True)
with open(os.path.join(OUT, "purls.txt"), "w") as f:
    f.write("\n".join(purls) + "\n")

# the item list the writer reads, so the cluster and the JSON stay derived from
# one description
with open("/data/workspace/walk_fixture_items.json", "w") as f:
    json.dump(items, f, indent=2)

print(f"{len(items)} items, {len(purls)} purls")
print("shared class is in", len(items[6]["connections"]["contained:up"]), "jars")
