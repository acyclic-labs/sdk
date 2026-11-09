//! Replay original argument inputs through the shared parser and fuzz oracle.
#[path = "../local_hotpath_options.rs"]
mod oracle;

#[test]
fn original_parser_admits_and_refuses_the_retained_argument_corpus() {
    let corpus = acyclic_test_support::crate_path!("corpus/local_hotpath_options");
    for entry in std::fs::read_dir(corpus).expect("argument corpus") {
        let entry = entry.expect("corpus entry");
        let name = entry.file_name();
        let name = name.to_str().expect("corpus name");
        let input = std::fs::read(entry.path()).expect("original argument bytes");
        let admitted = oracle::check(&input);
        if name.starts_with("accepted-") || name.starts_with("refused-") {
            assert_eq!(admitted, name.starts_with("accepted-"), "{name}");
        }
    }
}
