use super::window_name;
use crate::local::Error;
use proptest::prelude::*;

fn refused(name: &str) -> bool {
    matches!(
        window_name(name),
        Err(Error::Invalid("invalid deadline window"))
    )
}

/// Only the canonical form names a window. Negative control: the former
/// length check plus `str::parse` accepted a leading `+`.
#[test]
fn window_names_are_exactly_twenty_decimal_digits() {
    assert_eq!(window_name("00000000000000000001").ok(), Some(1));
    assert_eq!(window_name("18446744073709551615").ok(), Some(u64::MAX));
    for name in [
        "+0000000000000000001",
        "-0000000000000000001",
        " 0000000000000000001",
        "0000000000000000001 ",
        "0000000000000000000x",
        "000000000000000000001",
        "0000000000000000001",
        "18446744073709551616",
        "99999999999999999999",
        "",
        "\u{0661}000000000000000000",
    ] {
        assert!(refused(name), "{name:?} named a window");
    }
}

proptest! {
    /// Every window `window_path` writes reads back exactly.
    #[test]
    fn every_written_window_name_reads_back(window in any::<u64>()) {
        prop_assert_eq!(window_name(&format!("{window:020}")).ok(), Some(window));
    }

    /// Any name that is not the canonical rendering of its value is refused.
    #[test]
    fn a_non_canonical_name_is_refused(name in "[ +0-9a-z-]{0,24}") {
        let canonical = name
            .parse::<u64>()
            .is_ok_and(|window| format!("{window:020}") == name);
        prop_assert_eq!(window_name(&name).is_ok(), canonical);
        if !canonical {
            prop_assert!(refused(&name));
        }
    }
}
