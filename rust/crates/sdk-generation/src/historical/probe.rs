pub fn graft_path(bytes: &[u8]) -> Result<&str, String> {
    let text = std::str::from_utf8(bytes).map_err(|error| error.to_string())?;
    let mut lines = text.lines();
    if lines.next() != Some("false") {
        return Err("source frontier requires complete original Git ancestry".into());
    }
    let path = lines
        .next()
        .filter(|path| !path.is_empty())
        .ok_or("Git repository layout has no graft path")?;
    if lines.next().is_some() {
        return Err("unexpected Git repository layout record".into());
    }
    Ok(path)
}

#[cfg(test)]
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config {
        rng_seed: proptest::test_runner::RngSeed::Fixed(0),
        source_file: Some(file!()),
        ..proptest::test_runner::Config::default()
    })]
    #[test]
    fn repository_layout_preserves_original_path_and_refusals(
        directories in proptest::collection::vec("[a-zA-Z_ .:-]{1,12}", 0..8),
        arbitrary in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..128),
    ) {
        let original = include_bytes!("../../fuzz/corpus/fixture_replay/git-layout");
        proptest::prop_assert_eq!(crate::git_layout_oracle::check_input(original), Ok(".git/info/grafts"));
        let path = format!("{}/.git/info/grafts", directories.join("/"));
        let bytes = format!("false\n{path}\n");
        proptest::prop_assert_eq!(crate::git_layout_oracle::check_input(bytes.as_bytes()), Ok(path.as_str()));
        let _ = crate::git_layout_oracle::check_input(&arbitrary);
    }
}
