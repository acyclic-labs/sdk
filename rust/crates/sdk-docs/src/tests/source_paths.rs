use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // All coordinates survive reuse; filesystem work depends on unique paths,
    // never public-item multiplicity. Each projection checks paths at both ends.
    #[test]
    fn source_paths_preserve_coordinates_and_revalidate_mutations(
        spans in prop::collection::vec((0u8..2, 0usize..1000, 0usize..1000), 3..40),
    ) {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        for name in ["a.rs", "b.rs"] { fs::write(root.join(name), "pub struct Api;").unwrap(); }
        let generated = HashMap::new();
        let mut paths = HashMap::new();
        SOURCE_PATH_CHECKS.set(0);
        for (file, line, column) in &spans {
            let span = rustdoc_types::Span {
                filename: PathBuf::from(if *file == 0 { "a.rs" } else { "b.rs" }),
                begin: (*line, *column), end: (*line + 1, *column + 1),
            };
            let actual = cached_source_span(&root, &root, &generated, &mut paths, &span).unwrap();
            prop_assert_eq!(actual.path, span.filename.to_str().unwrap());
            prop_assert_eq!((actual.begin_line, actual.begin_column), span.begin);
            prop_assert_eq!((actual.end_line, actual.end_column), span.end);
        }
        prop_assert_eq!(SOURCE_PATH_CHECKS.get(), paths.len());
        validate_source_paths(&root, &root, &generated, &paths).unwrap();
        prop_assert_eq!(SOURCE_PATH_CHECKS.get(), 2 * paths.len());
        let filename = paths.keys().next().unwrap().clone();
        fs::remove_file(root.join(&filename)).unwrap();
        // A broken cache that only returns its saved value accepts this deletion.
        let span = rustdoc_types::Span { filename, begin: (0, 0), end: (0, 0) };
        prop_assert!(cached_source_span(&root, &root, &generated, &mut paths, &span).is_ok());
        prop_assert!(validate_source_paths(&root, &root, &generated, &paths).is_err());
        fs::write(root.join(&span.filename), "pub struct Api;").unwrap();
        validate_source_paths(&root, &root, &generated, &paths).unwrap();
        // A fresh operation must never reuse the saved mapping after deletion.
        fs::remove_file(root.join(&span.filename)).unwrap();
        prop_assert!(cached_source_span(&root, &root, &generated, &mut HashMap::new(), &span).is_err());
    }
}
