pub(super) fn graft_path(bytes: &[u8]) -> Result<&str, String> {
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
    #[test]
    fn repository_layout_round_trips_and_rejects_shallow_or_extra_records(
        directories in proptest::collection::vec("[a-zA-Z_ .:-]{1,12}", 0..8),
    ) {
        let path = format!("{}/.git/info/grafts", directories.join("/"));
        let bytes = format!("false\n{path}\n");
        proptest::prop_assert_eq!(graft_path(bytes.as_bytes()).unwrap(), path.as_str());
        let shallow = format!("true\n{path}\n");
        proptest::prop_assert!(graft_path(shallow.as_bytes()).is_err());
        let extra = format!("{bytes}extra\n");
        proptest::prop_assert!(graft_path(extra.as_bytes()).is_err());
        proptest::prop_assert!(graft_path(b"false\n").is_err());
    }
}
