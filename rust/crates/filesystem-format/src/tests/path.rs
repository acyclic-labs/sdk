use super::*;

#[test]
fn relative_validator_shares_grammar_and_stops_at_the_actual_invalid_prefix() {
    for (path, valid, examined) in [
        ("", false, 0),
        ("/a", false, 1),
        ("a//unexamined", false, 3),
        ("a\\unexamined", false, 2),
        ("a\0unexamined", false, 2),
        (".", false, 1),
        ("..", false, 2),
        ("../unexamined", false, 3),
        ("a/./unexamined", false, 4),
        ("a/../unexamined", false, 5),
        ("...", true, 3),
        ("a/.../b", true, 7),
        ("é/漢", true, 6),
        ("a/", false, 2),
    ] {
        let mut scanned = 0;
        let result = is_canonical_relative_with_step::<()>(path, || {
            scanned += 1;
            Ok(())
        });
        assert_eq!(result, Ok(valid), "{path:?}");
        assert_eq!(scanned, examined, "{path:?}");
        assert_eq!(is_canonical_relative(path), valid, "{path:?}");
    }
}

#[test]
fn relative_validator_admits_every_byte_before_examining_it() {
    let path = "é/漢";
    for allowance in 0..=path.len() {
        let mut scanned = 0;
        let result = is_canonical_relative_with_step(path, || {
            if scanned == allowance {
                return Err(scanned);
            }
            scanned += 1;
            Ok(())
        });
        if allowance < path.len() {
            assert_eq!(result, Err(allowance));
        } else {
            assert_eq!(result, Ok(true));
        }
        assert_eq!(scanned, allowance);
    }
}

#[test]
fn borrowed_path_validation_is_the_same_canonical_parser() {
    let limits = VolumeLimits {
        ..VolumeLimits::default()
    };
    for value in [
        "/",
        "/ab/c",
        "relative",
        "/a/",
        "/.",
        "/..",
        "/a//b",
        "/a\0",
        "/abcd",
        "/a/b/c",
        "/12345678",
    ] {
        assert_eq!(
            PortablePath::validate(value, limits),
            PortablePath::parse(value, limits).map(|path| path.depth())
        );
    }
}
