//! Configuration parsing, shared by the config fuzz targets and the config
//! tests: the fixture records [`Seen`] events and only [`INVARIANTS`] judge
//! them. Tests inject defects through `tamper`.
use acyclic_harness_tui::{
    InputKeymap,
    config::{ConfigFile, Diagnostic, DiagnosticKind, Scope, parse_bytes},
};
use serde_json::Value;

/// A parse result as the production parser returns it.
pub type Parsed = Result<Value, Vec<Diagnostic>>;

/// Which parse `tamper` receives.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Stage {
    /// A parsed file, encoded and parsed again.
    Reparsed,
    /// A keymap's canonical encoding, parsed as a file.
    Canonical,
    /// The defaults with one unknown top-level key.
    Unknown,
}

pub type Tamper = fn(Stage, &mut Parsed);

#[derive(Debug)]
pub enum Seen {
    Reparsed {
        file: ConfigFile,
        value: Value,
        reparsed: Parsed,
    },
    /// `keymap` is whether the canonical keymap parses back to itself.
    Canonical {
        value: Value,
        keymap: bool,
        reparsed: Parsed,
    },
    Unknown {
        file: ConfigFile,
        parsed: Parsed,
    },
}

pub type Check = fn(&Seen) -> Result<(), String>;

fn same(parsed: &Parsed, value: &Value) -> bool {
    matches!(parsed, Ok(parsed) if parsed == value)
}

pub const INVARIANTS: [(&str, Check); 3] = [
    ("roundtrip", |seen| match seen {
        Seen::Reparsed {
            file,
            value,
            reparsed,
        } if !same(reparsed, value) => Err(format!("{file:?} reparsed {value} as {reparsed:?}")),
        _ => Ok(()),
    }),
    ("canonical", |seen| match seen {
        Seen::Canonical {
            value,
            keymap,
            reparsed,
        } if !keymap || !same(reparsed, value) => Err(format!(
            "canonical keymap round trip {keymap} parsed {value} as {reparsed:?}"
        )),
        _ => Ok(()),
    }),
    ("unknown", |seen| match seen {
        Seen::Unknown { file, parsed }
            if !parsed.as_ref().is_err_and(|errors| {
                errors
                    .iter()
                    .any(|diagnostic| diagnostic.kind == DiagnosticKind::UnknownKey)
            }) =>
        {
            Err(format!(
                "{file:?} schema accepted an unknown key: {parsed:?}"
            ))
        }
        _ => Ok(()),
    }),
];

/// Fuzz entry: every invariant, as panics.
pub fn exercise(file: ConfigFile, bytes: &[u8]) {
    let judged = observe(file, bytes, |_, _| {}, &mut |seen| {
        for (name, check) in INVARIANTS {
            let result = check(&seen);
            assert!(result.is_ok(), "{name}: {result:?}");
        }
        Ok::<(), std::convert::Infallible>(())
    });
    if let Err(never) = judged {
        match never {}
    }
}

fn parse(file: ConfigFile, stage: Stage, bytes: &[u8], tamper: Tamper) -> Parsed {
    let mut parsed = parse_bytes(file, Scope::User, bytes);
    tamper(stage, &mut parsed);
    parsed
}

/// Records how `bytes` parse as `file` and round trip, and how the defaults
/// with an unknown key parse.
pub fn observe<S>(
    file: ConfigFile,
    bytes: &[u8],
    tamper: Tamper,
    see: &mut impl FnMut(Seen) -> Result<(), S>,
) -> Result<(), S> {
    if let Ok(value) = parse_bytes(file, Scope::User, bytes) {
        let encoded = value.to_string().into_bytes();
        see(Seen::Reparsed {
            file,
            value: value.clone(),
            reparsed: parse(file, Stage::Reparsed, &encoded, tamper),
        })?;
        if file == ConfigFile::Keymap {
            let parsed = InputKeymap::parse(&value.to_string());
            assert!(parsed.is_ok(), "validated keymap must parse: {parsed:?}");
            let Ok(map) = parsed else {
                return Ok(());
            };
            let canonical = map.encode();
            see(Seen::Canonical {
                keymap: InputKeymap::parse(&canonical).ok() == Some(map),
                reparsed: parse(file, Stage::Canonical, canonical.as_bytes(), tamper),
                value,
            })?;
        }
    }
    let defaults: Result<Value, _> = serde_json::from_str(file.defaults());
    assert!(defaults.is_ok(), "defaults must be JSON: {defaults:?}");
    let Ok(mut value) = defaults else {
        return Ok(());
    };
    assert!(
        matches!(value, Value::Object(_)),
        "defaults must be an object"
    );
    if let Value::Object(ref mut object) = value {
        object.insert("unknown-ui-setting".into(), true.into());
    }
    see(Seen::Unknown {
        file,
        parsed: parse(file, Stage::Unknown, value.to_string().as_bytes(), tamper),
    })
}
