//! Capture request and every reply decision from the original issuance writers.
use acyclic_auth::{
    testing,
    trust::{
        Issuer,
        issuance::{Decision, Refusal, Reply, Request},
    },
    verify,
};
use ed25519_dalek::SigningKey;
use std::error::Error;
mod corpus;

fn capture() -> Result<(), Box<dyn Error>> {
    let signed = testing::root(|_| {});
    let verified = verify::verify(&signed, &testing::Snapshot::default(), 100)?;
    let claims = verified.blocks()[0]
        .certificates
        .as_ref()
        .ok_or("missing certificates")?
        .keys[0]
        .key
        .clone();
    let key = SigningKey::from_bytes(&[2; 32]);
    let request = Request::new(
        [7; 32],
        key.verifying_key().to_bytes(),
        "test".into(),
        claims.into(),
    )?;
    let issuer = Issuer::new("test", SigningKey::from_bytes(&[1; 32]));
    let birth = issuer.birth("account", &key.verifying_key(), 1)?;
    let (root, _) = issuer.genesis("account", key, 1)?;
    let certificate = root.certify_request(&birth, &request)?;
    let mut records = vec![("issuance-request", request.wire().to_vec(),
        "issuance::Request::new from original testing::root verified claims; operation[7;32], issuer key[2;32], environment=test".into())];
    for (name, decision) in [
        ("issuance-accepted", Decision::Accepted(certificate)),
        ("issuance-denied", Decision::Refused(Refusal::Denied)),
        ("issuance-conflict", Decision::Refused(Refusal::Conflict)),
        (
            "issuance-issuer-changed",
            Decision::Refused(Refusal::IssuerChanged),
        ),
        ("issuance-unknown", Decision::Unknown),
    ] {
        let wire = Reply {
            request_digest: *request.digest(),
            decision,
        }
        .into_wire();
        records.push((name, wire, format!("issuance::Reply::into_wire({name}); accepted certificate from original AccountRoot::certify_request")));
    }
    corpus::write(
        "fuzz/corpus/capability",
        "fuzz/corpus/issuance-sources.tsv",
        records,
    )
}

fn main() {
    if let Err(error) = capture() {
        eprintln!(
            "STEP capture original issuance corpus failed: {error}; recovery from rust/crates/auth: cargo run --locked --features test-support --example issuance_corpus"
        );
        std::process::exit(1);
    }
}
