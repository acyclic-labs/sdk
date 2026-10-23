//! Byte goldens emitted by the unboxed implementation at main 32a840f9a.
//!
//! Keep these bytes pinned: regenerating them with the current implementation
//! would hide changes to tags, nesting, field order or omitted/default fields.
//! The workload fixture changes only the 15 scope workload fields and three
//! effect-receipt encodings deliberately introduced by main #1409. The original
//! fixture remains alongside it so those independent wire changes are explicit.
use super::{Action, Command, EffectStatus, EventPayload};
use crate::fork::{Capture, ResourceRevision};
use crate::{Error, Result};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::fmt::Debug;

#[derive(Deserialize)]
#[serde(tag = "type", content = "wire")]
enum Golden {
    EventPayload(String),
    Action(String),
    Command(String),
    EffectStatus(String),
    ResourceRevision(String),
    Capture(String),
}

fn round_trip<T: DeserializeOwned + Serialize + PartialEq + Debug>(wire: &str) -> Result<()> {
    let value: T = serde_json::from_str(wire).map_err(|error| Error::Invalid(error.to_string()))?;
    let encoded = serde_json::to_vec(&value).map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(encoded, wire.as_bytes(), "{}", std::any::type_name::<T>());
    let reopened: T =
        serde_json::from_slice(&encoded).map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(reopened, value);
    Ok(())
}

#[test]
fn boxed_variants_preserve_preboxing_wire_bytes_and_equality() -> Result<()> {
    let goldens: Vec<Golden> = serde_json::from_str(include_str!(
        "../../tests/fixtures/preboxing-workload-wire.json"
    ))
    .map_err(|error| Error::Invalid(error.to_string()))?;
    let mut counts = (0, 0, 0, 0, 0, 0);
    for golden in goldens {
        match golden {
            Golden::EventPayload(wire) => {
                round_trip::<EventPayload>(&wire)?;
                counts.0 += 1;
            }
            Golden::Action(wire) => {
                round_trip::<Action>(&wire)?;
                counts.1 += 1;
            }
            Golden::Command(wire) => {
                round_trip::<Command>(&wire)?;
                counts.2 += 1;
            }
            Golden::EffectStatus(wire) => {
                round_trip::<EffectStatus>(&wire)?;
                counts.3 += 1;
            }
            Golden::ResourceRevision(wire) => {
                round_trip::<ResourceRevision>(&wire)?;
                counts.4 += 1;
            }
            Golden::Capture(wire) => {
                round_trip::<Capture>(&wire)?;
                counts.5 += 1;
            }
        }
    }
    assert_eq!(counts, (15, 15, 15, 5, 8, 4));
    Ok(())
}
