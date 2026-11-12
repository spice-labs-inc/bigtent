#[test]
fn probe_int_key_rendering() {
    use bigtent::barker::{parse, run_with};
    for script in [
        "emit(stats.tally([42]))\n",
        "emit(collections.group_by([42], lambda x: 42))\n",
        "emit(stats.tally([\"42\"]))\n",
    ] {
        let program = parse(script).unwrap();
        let (s, i) = flume::unbounded();
        s.send(serde_json::json!(1)).unwrap();
        drop(s);
        let rows: Vec<_> = run_with(program, i, None, None, None).unwrap().filter_map(|r| r.ok()).collect();
        println!("{script:?} -> {rows:?}");
    }
}
