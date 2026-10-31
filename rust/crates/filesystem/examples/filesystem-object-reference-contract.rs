//! Projects opaque reference admission from the canonical decoder into bindings.

use acyclic_fs::{Digest, ObjectId};
use serde_json::json;

fn main() -> Result<(), serde_json::Error> {
    let mut layouts = Vec::new();
    let mut fixtures = Vec::new();
    for tag in u8::MIN..=u8::MAX {
        let mut probe = [37; ObjectId::ENCODED_LEN];
        probe[0] = tag;
        let Ok((object, remaining)) = ObjectId::from_prefix(&probe) else {
            continue;
        };
        layouts.push(json!({
            "tag": tag,
            "kind": object.kind.canonical_tag(),
            "length": probe.len() - remaining.len(),
        }));
        fixtures.push(json!({
            "kind": object.kind.canonical_tag(),
            "bytes": object.to_bytes().as_slice(),
        }));
        if let Some(mut storage) = object.storage {
            storage.locator = Digest::from_bytes([19; 32]);
            let resealed = ObjectId {
                storage: Some(storage),
                ..object
            };
            fixtures.push(json!({
                "kind": resealed.kind.canonical_tag(),
                "bytes": resealed.to_bytes().as_slice(),
            }));
        }
    }
    println!(
        "{}",
        serde_json::to_string(&json!({ "layouts": layouts, "fixtures": fixtures }))?
    );
    Ok(())
}
