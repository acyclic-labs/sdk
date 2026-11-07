//! Capture caller seeds from the public serializer before injecting individual refusals.
use acyclic_actors::guest::{AdmittedCaller, CallerCapability};
use serde_json::Value;

fn capture() -> Result<(), Box<dyn std::error::Error>> {
    let directory = std::path::PathBuf::from(
        std::env::args_os()
            .nth(1)
            .ok_or("expected corpus directory")?,
    );
    let caller = AdmittedCaller::new(
        b"a".to_vec(),
        "o".into(),
        "p".into(),
        "c".into(),
        "i".into(),
        1,
    )?;
    let original = serde_json::to_value(&caller)?;
    let mut empty = original.clone();
    *empty
        .get_mut("accountId")
        .ok_or("original caller lacks accountId")? = Vec::<u8>::new().into();
    let mut expiry = original.clone();
    *expiry
        .get_mut("expiresAtMs")
        .ok_or("original caller lacks expiresAtMs")? = "01".into();
    let mut capability =
        serde_json::to_value(caller.with_capability(CallerCapability::new(1, "cap".into())?)?)?;
    let positional = Value::Array(
        [
            "accountId",
            "orgId",
            "principal",
            "credentialId",
            "actorId",
            "namespace",
            "expiresAtMs",
            "capability",
        ]
        .map(|field| capability.get(field).cloned())
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or("original capability lacks a positional field")?,
    );
    *capability
        .get_mut("capability")
        .and_then(|value| value.get_mut("version"))
        .ok_or("original capability lacks version")? = 0.into();
    let mut caller = serde_json::to_value(
        AdmittedCaller::new(
            b"account".to_vec(),
            "org".into(),
            "user".into(),
            "credential".into(),
            "actor".into(),
            u64::MAX,
        )?
        .with_capability(CallerCapability::new(1, "opaque".into())?)?,
    )?;
    caller
        .as_object_mut()
        .ok_or("original caller is not an object")?
        .insert("future".into(), true.into());
    for (name, value, accepted) in [
        ("caller.json", caller, true),
        ("caller-positional.json", positional, true),
        ("caller-empty.json", empty, false),
        ("caller-expiry-alias.json", expiry, false),
        ("caller-zero-version.json", capability, false),
    ] {
        let mut bytes = serde_json::to_vec(&value)?;
        bytes.push(b'\n');
        if serde_json::from_slice::<AdmittedCaller>(&bytes).is_ok() != accepted {
            return Err(format!("{name}: unexpected admission before capture").into());
        }
        std::fs::write(directory.join(name), &bytes)?;
        print!("captured {name}: {}", std::str::from_utf8(&bytes)?);
    }
    Ok(())
}

fn main() {
    if let Err(error) = capture() {
        eprintln!(
            "step=capture-guest-metadata: {error}; recovery=cargo run --manifest-path fuzz/Cargo.toml --example guest_metadata_corpus -- fuzz/corpus/guest_json"
        );
        std::process::exit(1);
    }
}
