//! Capture signed trust inputs from the original SDK custody implementation.
use acyclic_auth::trust::{EnvironmentFrontier, Issuer};
use ed25519_dalek::SigningKey;
use std::error::Error;
mod corpus;

fn capture() -> Result<(), Box<dyn Error>> {
    let issuer = Issuer::new("test", SigningKey::from_bytes(&[1; 32]));
    // Retain a complete record that also reached the former separator-only
    // oracle. Every candidate is signed and validated by the real writer.
    let mut foreign = None;
    for key in 3..=u8::MAX {
        let (_, records) = issuer.genesis("other", SigningKey::from_bytes(&[key; 32]), 100)?;
        if !records[0].contains(&0xA5) {
            foreign = Some((key, records));
            break;
        }
    }
    let (key, foreign) = foreign.ok_or("no separator-free signed foreign record")?;
    let (mut account, _) = issuer.genesis("account", SigningKey::from_bytes(&[2; 32]), 100)?;
    let mut environment = EnvironmentFrontier::genesis("test", issuer.anchor());
    let next_key = SigningKey::from_bytes(&[9; 32]);
    let rotation = issuer.environment_keyring(
        &mut environment,
        &[next_key.verifying_key().to_bytes()],
        100,
    )?;
    let next = Issuer::new("test", next_key);
    let rotation_next = next.environment_keyring(&mut environment, &issuer.anchor(), 100)?;
    let records = [
        (
            "foreign-keyring",
            foreign[0].clone(),
            format!("Issuer::genesis(other, signing-key[{key};32],100)[0]"),
        ),
        (
            "foreign-checkpoint",
            foreign[1].clone(),
            format!("Issuer::genesis(other, signing-key[{key};32],100)[1]"),
        ),
        (
            "selected-epoch",
            account.raise_minimum_epoch(2, 100)?,
            "AccountRoot::raise_minimum_epoch(2,100); original account key[2;32]".into(),
        ),
        (
            "environment-rotation",
            rotation,
            "Issuer::environment_keyring; original key[1;32] -> key[9;32]".into(),
        ),
        (
            "environment-successor",
            rotation_next,
            "Issuer::environment_keyring; key[9;32] -> original key[1;32]".into(),
        ),
    ];
    corpus::write(
        "fuzz/corpus/trust_record",
        "fuzz/corpus/trust-record-sources.tsv",
        records,
    )
}

fn main() {
    if let Err(error) = capture() {
        eprintln!(
            "STEP capture signed trust corpus failed: {error}; recovery from rust/crates/auth: cargo run --locked --example trust_corpus"
        );
        std::process::exit(1);
    }
}
