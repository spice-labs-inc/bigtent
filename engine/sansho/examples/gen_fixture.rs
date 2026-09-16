//! One-time fixture generator: writes the golden CBOR bytes of a
//! representative item (the SPEC-0001 Appendix-A shape: file_names with
//! merge-disambiguated gitoid entries, an array-valued mime_type,
//! file_size, extra, connections). Run once:
//! `cargo run -p sansho --example gen_fixture`

fn main() {
    let item = serde_json::json!({
        "identifier": "gitoid:blob:sha1:30e65ad24f4b4d799e52cfd70fcbebc0490b7343",
        "connections": [
            ["alias:from", "gitoid:blob:sha1:30e65ad24f4b4d799e52cfd70fcbebc0490b7343"],
            ["alias:from", "md5:4d921bf0ce238d1a96bdf65000fde565"],
            ["alias:from", "sha1:e9f38efe19210f3b63a72699a69eb261176d251e"]
        ],
        "body_mime_type": "application/vnd.cc.goatrodeo",
        "body": {
            "extra": {},
            "file_names": [
                "com/groupbyinc/flux/common/apache/logging/log4j/core/lookup/JndiLookup.java",
                "gitoid:blob:sha256:005a5131bc1c950b2b4d1081a95bd21f52e45c308d9633465fbc240f8433c9e6!$org/apache/logging/log4j/core/lookup/JndiLookup.java",
                "gitoid:blob:sha256:007015fe7b249408bff2b406245d6213d211f27c1286e1ecaac05a63462186bc!$org/apache/logging/log4j/core/lookup/JndiLookup.java",
                "logging-log4j2-log4j-2.10.0/log4j-core/src/main/java/org/apache/logging/log4j/core/lookup/JndiLookup.java",
                "org/apache/logging/log4j/core/lookup/JndiLookup.java"
            ],
            "file_size": 3050,
            "mime_type": ["text/x-java-source"]
        }
    });
    let bytes = serde_cbor::to_vec(&item).expect("fixture encoding");
    let out = std::path::Path::new("tests/fixtures/item_a.cbor");
    std::fs::create_dir_all(out.parent().unwrap()).unwrap();
    std::fs::write(out, &bytes).unwrap();
    std::fs::write(
        std::path::Path::new("tests/fixtures/item_a.json"),
        serde_json::to_string_pretty(&item).unwrap(),
    )
    .unwrap();
    println!("wrote {} bytes to {}", bytes.len(), out.display());
}
