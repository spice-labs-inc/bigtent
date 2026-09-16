//! The spike: probe minicbor's API for everything the cursor backend
//! needs. This test is the Phase 3 spike's record — it proves the
//! decoder supports single-pass consumption with position control,
//! zero-copy string access, skip, container iteration, and indefinite-
//! length detection BEFORE the backend is built on top of it.

#[test]
fn minicbor_supports_the_cursor_surface() {
    use minicbor::Decoder;

    // a map with two string keys and nested values
    let bytes: Vec<u8> = {
        let mut enc = minicbor::Encoder::new(Vec::new());
        enc.map(2).unwrap();
        enc.str("alpha").unwrap();
        enc.array(2).unwrap();
        enc.u64(1).unwrap();
        enc.u64(2).unwrap();
        enc.str("beta").unwrap();
        enc.str("hello").unwrap();
        let buf = enc.into_writer(); // the Encoder's underlying Vec
        buf
    };

    let mut decoder = Decoder::new(&bytes);
    // the root is a map with 2 entries
    assert_eq!(decoder.map().unwrap(), Some(2));

    // entry 1: the key borrows from the input
    let key = decoder.str().unwrap();
    assert_eq!(key, "alpha");
    // the value's position is HERE — recordable without decoding it
    let value_position = decoder.position();
    // skip the nested array entirely
    decoder.skip().unwrap();
    // entry 2
    let key2 = decoder.str().unwrap();
    assert_eq!(key2, "beta");
    let value2_position = decoder.position();
    let s = decoder.str().unwrap();
    assert_eq!(s, "hello");

    // a node = (bytes, position): re-entering at a position works
    let mut sub = Decoder::new(&bytes);
    sub.set_position(value_position);
    assert_eq!(sub.array().unwrap(), Some(2));
    assert_eq!(sub.u64().unwrap(), 1);
    assert_eq!(sub.u64().unwrap(), 2);

    // the end position after decoding an item: position() tracks it
    let mut sub2 = Decoder::new(&bytes);
    sub2.set_position(value2_position);
    let _ = sub2.str().unwrap();
    assert_eq!(sub2.position(), bytes.len());

    // indefinite-length detection: the array() call returns None
    let indefinite: Vec<u8> = {
        let mut enc = minicbor::Encoder::new(Vec::new());
        enc.begin_array().unwrap();
        enc.u8(1).unwrap();
        enc.end().unwrap();
        let buf = enc.into_writer();
        buf
    };
    let mut indef_decoder = Decoder::new(&indefinite);
    assert_eq!(indef_decoder.array().unwrap(), None, "indefinite array must be detectable");

    // datatype inspection without consuming
    let mut peek = Decoder::new(&bytes);
    let _ = peek.map().unwrap();
    assert!(matches!(peek.datatype().unwrap(), minicbor::data::Type::String));
}
