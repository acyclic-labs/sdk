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
        let span = rustdoc_types::Span { filename: filename.into(), begin: (0, 0), end: (0, 0) };
        prop_assert!(cached_source_span(&root, &root, &generated, &mut paths, &span).is_ok());
        prop_assert!(validate_source_paths(&root, &root, &generated, &paths).is_err());
        fs::write(root.join(&span.filename), "pub struct Api;").unwrap();
        validate_source_paths(&root, &root, &generated, &paths).unwrap();
        // A fresh operation must never reuse the saved mapping after deletion.
        fs::remove_file(root.join(&span.filename)).unwrap();
        prop_assert!(cached_source_span(&root, &root, &generated, &mut HashMap::new(), &span).is_err());
    }

    // Every raw spelling must receive the same validation as a cold lookup,
    // including component-equal paths with different filesystem semantics.
    #[test]
    fn source_cache_preserves_raw_spelling_validation(
        leaf in "[a-z]{1,8}", separators in 2usize..5, dots in 1usize..4,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        fs::create_dir(root.join("src")).unwrap();
        fs::write(root.join(format!("src/{leaf}.rs")), "pub struct Api;").unwrap();
        let original = format!("src/{leaf}.rs");
        let spellings = BTreeSet::from([
            original.clone(),
            format!("{original}/"),
            format!("{original}{}", "/".repeat(separators)),
            format!("src/{}{leaf}.rs", "./".repeat(dots)),
            format!("src{}{leaf}.rs", "/".repeat(separators)),
            format!("./{original}"),
            format!("SRC/{}.RS", leaf.to_ascii_uppercase()),
            format!(r"src\{leaf}.rs"),
        ]);
        let generated = HashMap::new();
        let mut paths = HashMap::new();
        let base = rustdoc_types::Span { filename: original.clone().into(), begin: (1, 2), end: (3, 4) };
        let initial = cached_source_span(&root, &root, &generated, &mut paths, &base).unwrap();
        let mut broken = HashMap::from([(base.filename.clone(), initial.path)]);
        let mut detected = false;
        let mut wrong_acceptance = false;
        let mut accepted = BTreeSet::from([std::ffi::OsString::from(&original)]);
        for spelling in spellings.into_iter().filter(|spelling| spelling != &original) {
            let span = rustdoc_types::Span { filename: spelling.clone().into(), ..base.clone() };
            let checks = SOURCE_PATH_CHECKS.get();
            let cached = cached_source_span(&root, &root, &generated, &mut paths, &span);
            prop_assert_eq!(SOURCE_PATH_CHECKS.get(), checks + 1,
                "raw spelling must be checked independently: {:?}", spelling);
            let fresh = source_span_from_base(&root, &root, &generated, &span);
            prop_assert_eq!(cached.as_ref().map(|s| s.path.as_str()).map_err(ToString::to_string),
                fresh.as_ref().map(|s| s.path.as_str()).map_err(ToString::to_string));
            if cached.is_ok() { accepted.insert(span.filename.as_os_str().to_owned()); }
            // Deliberately restore the former component-equality cache. It skips
            // validation of aliases, and on Unix accepts a file with a slash.
            let checks = SOURCE_PATH_CHECKS.get();
            let old = match broken.get(&span.filename) {
                Some(path) => Ok(path.clone()),
                None => {
                    let result = source_span_from_base(&root, &root, &generated, &span).map(|s| s.path);
                    if let Ok(path) = &result { broken.insert(span.filename.clone(), path.clone()); }
                    result
                }
            };
            detected |= SOURCE_PATH_CHECKS.get() == checks;
            wrong_acceptance |= old.is_ok() && fresh.is_err();
        }
        prop_assert_eq!(paths.keys().cloned().collect::<BTreeSet<_>>(), accepted);
        validate_source_paths(&root, &root, &generated, &paths).unwrap();
        prop_assert!(detected, "PathBuf negative control did not alias distinct spellings");
        if cfg!(unix) { prop_assert!(wrong_acceptance, "negative control missed the trailing-slash rejection"); }
    }

}
