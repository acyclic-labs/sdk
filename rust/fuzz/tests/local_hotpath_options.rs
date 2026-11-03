#[path = "../local_hotpath_options.rs"]
mod local_hotpath_options;

#[test]
fn original_parser_admits_and_refuses_the_retained_argument_corpus() {
    let corpus =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus/local_hotpath_options");
    for entry in std::fs::read_dir(corpus).expect("argument corpus") {
        let entry = entry.expect("corpus entry");
        let name = entry.file_name();
        let name = name.to_str().expect("corpus name");
        let input = std::fs::read(entry.path()).expect("original argument bytes");
        local_hotpath_options::check(&input);
        assert_eq!(
            local_hotpath_options::admitted(&input),
            name.starts_with("accepted-"),
            "{name}"
        );
    }
}
