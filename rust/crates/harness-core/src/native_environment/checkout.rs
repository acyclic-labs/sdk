//! Dependency-free task-binding snapshot; also compiled by the native build resolver.
use std::path::Path;

/// Native values copied into the durable context manifest by the caller.
#[derive(Clone, Debug)]
pub struct CheckoutFacts {
    /// Stable bound checkout directory, if known at admission.
    pub cwd: Option<String>,
    /// Host tool platform or explicit fixture value.
    pub os: String,
    /// Bound command tool shell.
    pub shell: String,
    /// UTC admission date.
    pub current_date: String,
    /// Zone of the recorded date.
    pub timezone: String,
    /// Optional status supplied only for an applicable Git view.
    pub git_status: Option<String>,
    /// Selected model name.
    pub model: String,
}

/// Gathers from explicit task inputs and platform policy, never process cwd or user locations.
pub fn checkout(
    cwd: Option<&Path>,
    now_unix_millis: u64,
    model: &str,
    platform: &super::PlatformPolicy,
    git_status: Option<String>,
) -> CheckoutFacts {
    let (os, shell) = platform.facts();
    CheckoutFacts {
        cwd: cwd.map(display),
        os: os.into(),
        shell: shell.into(),
        current_date: date(now_unix_millis),
        timezone: "UTC".into(),
        git_status,
        model: model.into(),
    }
}

/// The path as a user would type it: Windows verbatim prefixes are dropped.
pub(super) fn display(path: &Path) -> String {
    let text = path.to_string_lossy();
    if let Some(share) = text.strip_prefix(r"\\?\UNC\") {
        format!(r"\\{share}")
    } else if let Some(local) = text
        .strip_prefix(r"\\?\")
        .filter(|local| local.as_bytes().get(1) == Some(&b':'))
    {
        local.to_owned()
    } else {
        text.into_owned()
    }
}

/// Proleptic Gregorian UTC date, `YYYY-MM-DD`.
pub(super) fn date(unix_millis: u64) -> String {
    // Hinnant's days-to-civil over the 400-year era; u64 days never go negative.
    let days = unix_millis / 86_400_000 + 719_468;
    let era = days / 146_097;
    let day_of_era = days % 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = year_of_era + era * 400 + u64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn reference_date(millis: u64) -> String {
        let mut days = millis / 86_400_000;
        let mut year = 1970 + (days / 146_097) * 400;
        days %= 146_097;
        let leap = |year: u64| {
            year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
        };
        while days >= if leap(year) { 366 } else { 365 } {
            days -= if leap(year) { 366 } else { 365 };
            year += 1;
        }
        let mut month = 1;
        for length in [
            31,
            if leap(year) { 29 } else { 28 },
            31,
            30,
            31,
            30,
            31,
            31,
            30,
            31,
            30,
            31,
        ] {
            if days < length {
                break;
            }
            days -= length;
            month += 1;
        }
        format!("{year:04}-{month:02}-{:02}", days + 1)
    }

    proptest! {
        #[test]
        fn dates_match_independent_calendar(millis in any::<u64>()) {
            prop_assert_eq!(date(millis), reference_date(millis));
        }

        #[test]
        fn windows_display_preserves_drive_and_share_paths(drive in "[A-Z]", suffix in "[a-zA-Z0-9\\\\]{0,60}") {
            let local = format!(r"{drive}:\{suffix}");
            prop_assert_eq!(display(Path::new(&format!(r"\\?\{local}"))), local.as_str());
            prop_assert_eq!(display(Path::new(&local)), local);
            let share = format!(r"server\share\{suffix}");
            prop_assert_eq!(display(Path::new(&format!(r"\\?\UNC\{share}"))), format!(r"\\{share}"));
        }
    }

    #[test]
    fn dates_follow_the_gregorian_calendar_in_utc() {
        assert_eq!(date(0), "1970-01-01");
        assert_eq!(date(951_782_400_000), "2000-02-29");
        assert_eq!(date(1_792_324_800_000), "2026-10-18");
        assert_eq!(date(1_792_367_999_999), "2026-10-18");
        assert_eq!(date(1_792_368_000_000), "2026-10-19");
        assert_eq!(date(4_000_000_000_000), "2096-10-02");
    }

    #[test]
    fn verbatim_windows_prefixes_are_not_shown() {
        for (path, shown) in [
            (r"\\?\C:\work", r"C:\work"),
            (r"\\?\UNC\server\share", r"\\server\share"),
            (r"\\?\Volume{1}\work", r"\\?\Volume{1}\work"),
            ("/tmp/work", "/tmp/work"),
        ] {
            assert_eq!(display(Path::new(path)), shown);
        }
    }
}
