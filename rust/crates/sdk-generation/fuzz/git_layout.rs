use super::historical;

pub fn check_input(bytes: &[u8]) -> Result<&str, String> {
    let result = historical::graft_path(bytes);
    if std::str::from_utf8(bytes).is_err() {
        assert!(result.is_err(), "Git layout admitted invalid UTF-8");
    }
    if let Ok(path) = result.as_ref() {
        assert!(!path.is_empty(), "Git layout admitted an empty path");
        // CRLF preserves a standalone trailing carriage return in the path.
        let canonical = format!("false\r\n{path}\r\n");
        assert_eq!(
            historical::graft_path(canonical.as_bytes()),
            Ok(*path),
            "Git layout changed the original path"
        );
        let shallow = format!("true\r\n{path}\r\n");
        assert!(
            historical::graft_path(shallow.as_bytes()).is_err(),
            "Git layout admitted shallow ancestry"
        );
        let extra = format!("{canonical}extra\r\n");
        assert!(
            historical::graft_path(extra.as_bytes()).is_err(),
            "Git layout admitted an extra record"
        );
        assert!(
            historical::graft_path(b"false\r\n\r\n").is_err(),
            "Git layout admitted an empty path"
        );
        assert!(
            historical::graft_path(b"false\r\n").is_err(),
            "Git layout admitted a missing path"
        );
        let mut invalid = canonical.into_bytes();
        invalid.push(0xff);
        assert!(
            historical::graft_path(&invalid).is_err(),
            "Git layout admitted invalid UTF-8"
        );
    }
    result
}
