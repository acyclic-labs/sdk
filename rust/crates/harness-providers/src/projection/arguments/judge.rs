//! One judge for argument normalization, shared by the fuzz target and the
//! test oracle: fixtures record [`Seen`] events and only [`INVARIANTS`] rule.
//! Tests inject defects through `encode` and `envelope`; the fuzz target
//! passes the production values.
use super::{Arguments, literal_at_depth};
use crate::Adapter;
use serde_json::Value;

/// One observation of argument normalization.
#[derive(Debug)]
pub(crate) enum Seen {
    /// Arguments as a wire site decoded them.
    Normalized {
        site: String,
        value: Value,
        normalized: Value,
    },
    /// Whether a wire site decoded within the generated depth budget.
    Decoded { site: String, error: Option<String> },
    /// Whether re-encoding the normalized arguments reproduced the bytes.
    Reencoded { equal: bool },
}

type Check = fn(&Seen) -> Result<(), String>;

/// At most three generated levels wrap arguments: Gemini part, functionCall and `_raw`.
pub(crate) const ENVELOPE: u32 = 3;

/// Every rule over [`Seen`] events, by name.
pub(crate) const INVARIANTS: [(&str, Check); 5] = [
    ("object", |seen| match seen {
        Seen::Normalized {
            site, normalized, ..
        } if !normalized.is_object() => Err(format!("{site}: {normalized} is not an object")),
        _ => Ok(()),
    }),
    ("identity", |seen| match seen {
        Seen::Normalized {
            site,
            value,
            normalized,
        } if value.is_object() && normalized != value => {
            Err(format!("{site}: object arguments changed to {normalized}"))
        }
        _ => Ok(()),
    }),
    ("raw", |seen| match seen {
        Seen::Normalized {
            site,
            value,
            normalized: Value::Object(object),
        } if !value.is_object() && (object.len() != 1 || object.get("_raw") != Some(value)) => {
            Err(format!(
                "{site}: non-object arguments need exactly one unchanged _raw entry, got {object:?}"
            ))
        }
        _ => Ok(()),
    }),
    ("decodes", |seen| match seen {
        Seen::Decoded {
            site,
            error: Some(error),
        } => Err(format!(
            "{site} exceeded the generated depth budget: {error}"
        )),
        _ => Ok(()),
    }),
    ("idempotent", |seen| match seen {
        Seen::Reencoded { equal: false } => {
            Err("normalized arguments re-encoded differently".into())
        }
        _ => Ok(()),
    }),
];

/// The production argument encoding.
pub(crate) fn encode(value: &Value) -> Vec<u8> {
    serde_json::to_vec(&Arguments(value)).unwrap_or_default()
}

/// Decodes `bytes` with the input bound plus `envelope` generated levels and
/// records the outcome; `None` means the run cannot continue at this site.
pub(crate) fn decoded<S>(
    see: &mut impl FnMut(Seen) -> Result<(), S>,
    site: &str,
    bytes: &[u8],
    envelope: u32,
) -> Result<Option<Value>, S> {
    let result = literal_at_depth(bytes, 128 + envelope);
    let error = result.as_ref().err().map(ToString::to_string);
    see(Seen::Decoded {
        site: site.into(),
        error,
    })?;
    Ok(result.ok())
}

/// Records how `encode` normalizes `value` directly and how every adapter's
/// call normalizes it, decoding with `envelope` generated levels.
pub(crate) fn normalization<S>(
    value: &Value,
    envelope: u32,
    encode: fn(&Value) -> Vec<u8>,
    see: &mut impl FnMut(Seen) -> Result<(), S>,
) -> Result<(), S> {
    let bytes = encode(value);
    let Some(normalized) = decoded(see, "arguments", &bytes, envelope)? else {
        return Ok(());
    };
    let again = encode(&normalized);
    see(Seen::Normalized {
        site: "arguments".into(),
        value: value.clone(),
        normalized,
    })?;
    see(Seen::Reencoded {
        equal: again == bytes,
    })?;
    for adapter in [
        Adapter::Anthropic,
        Adapter::Responses,
        Adapter::Chat,
        Adapter::Gemini,
    ] {
        let site = format!("{adapter:?} call");
        let call = super::super::call::Call {
            adapter,
            id: "call_0".into(),
            name: "echo".into(),
            arguments: value,
            signature: None,
        };
        let encoded = serde_json::to_vec(&call).unwrap_or_default();
        let Some(wire) = decoded(see, &site, &encoded, envelope)? else {
            return Ok(());
        };
        let arguments = match adapter {
            Adapter::Anthropic => wire["input"].clone(),
            Adapter::Gemini => wire["functionCall"]["args"].clone(),
            Adapter::Responses | Adapter::Chat => {
                let raw = if adapter == Adapter::Responses {
                    &wire["arguments"]
                } else {
                    &wire["function"]["arguments"]
                };
                // Codec fuzzing includes standalone values deeper than an
                // admitted ModelRequest. Validate those with the derived output
                // bound; actual request validator admission is checked separately.
                let raw = raw.as_str().unwrap_or_default().as_bytes();
                let Some(arguments) = decoded(see, &site, raw, envelope)? else {
                    return Ok(());
                };
                arguments
            }
        };
        see(Seen::Normalized {
            site,
            value: value.clone(),
            normalized: arguments,
        })?;
    }
    Ok(())
}

/// Fuzz entry: the model JSON round trip plus every invariant, as panics.
pub(crate) fn assert_normalization(value: &Value) {
    crate::fuzzing::assert_model_json_round_trip(value);
    let judged = normalization(value, ENVELOPE, encode, &mut |seen| {
        for (name, check) in INVARIANTS {
            if let Err(detail) = check(&seen) {
                panic!("{name}: {detail}");
            }
        }
        Ok::<(), std::convert::Infallible>(())
    });
    if let Err(never) = judged {
        match never {}
    }
}
