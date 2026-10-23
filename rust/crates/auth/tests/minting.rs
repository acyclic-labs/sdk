//! Runtime minting through the same verifier used by capability holders.
#[path = "common/signed.rs"]
mod signed;
use acyclic_auth::{authority, contract::*, verify::*};
use signed::*;

fn certificates() -> Result<CertificateChain, Box<dyn std::error::Error>> {
    let envelope = authority::decode_capability(&root(|_| {}))?;
    let signed = envelope.blocks.first().ok_or("missing root block")?;
    Ok(authority::decode_block(&signed.payload)?.certificates.ok_or("missing certificates")?)
}

#[test]
fn minted_workloads_keep_certificate_grants_audience_expiry_and_revocation()
-> Result<(), Box<dyn std::error::Error>> {
    let trust = Snapshot::default();
    let signer = CertifiedSigner::new(certificates()?, key(3))?;
    let cap = signer.mint(vec![
        Caveat { kind: CaveatKind::Audience(ResourcePattern { name: "workers:/test".into(), prefix: false }) },
        Caveat { kind: CaveatKind::ExpiresAtUnixSeconds(NOW + 5) },
        Caveat { kind: CaveatKind::Grants(GrantSet { grants: vec![Grant { kind: GrantKind::Resource(ResourceGrant {
            resource: ResourcePattern { name: "fs:/public".into(), prefix: true },
            actions: vec!["fs/read".into()],
        }) }] }) },
    ], key(4), &trust, NOW)?;
    let child = cap.attenuate(vec![], &[5;32])?.seal()?;
    let attributes = RecordAttributes::default();
    let usage = limits(None);
    let allowed = request("fs:/public/item", &attributes, &usage);
    child.authorize(&trust, &allowed, authority::PolicyDecision::Allow)?;
    for (action, resource, audience) in [
        ("fs/read", "fs:/private/item", "workers:/test"),
        ("fs/write", "fs:/public/item", "workers:/test"),
        ("fs/read", "fs:/public/item", "workers:/other"),
    ] {
        let mut denied = request(resource, &attributes, &usage);
        denied.target = authority::Target::Resource { action, resource };
        denied.audience = audience;
        assert!(child.authorize(&trust, &denied, authority::PolicyDecision::Allow).is_err());
    }
    assert!(matches!(child.check(&trust, NOW + 5), Err(VerifyError::Time)));
    let mut revoked = Snapshot::default();
    revoked.revoked_blocks.push(*cap.block_ids().first().ok_or("missing root id")?);
    assert!(matches!(child.check(&revoked, NOW), Err(VerifyError::Revoked)));
    assert!(matches!(child.attenuate(vec![], &[6;32]), Err(VerifyError::Sealed)));
    Ok(())
}

#[test]
fn minting_never_bypasses_certificate_authentication_or_current_trust()
-> Result<(), Box<dyn std::error::Error>> {
    let trust = Snapshot::default();
    assert!(matches!(CertifiedSigner::new(certificates()?, key(9)), Err(VerifyError::Signature)));
    let mut forged = certificates()?;
    let signature = &mut forged.birth.signature;
    *signature.first_mut().ok_or("missing birth signature")? ^= 1;
    let signer = CertifiedSigner::new(forged, key(3))?;
    assert!(matches!(signer.mint(vec![], key(4), &trust, NOW), Err(VerifyError::Signature)));
    let signer = CertifiedSigner::new(certificates()?, key(3))?;
    let mut unavailable = Snapshot::default();
    unavailable.available = false;
    assert!(matches!(signer.mint(vec![], key(4), &unavailable, NOW), Err(VerifyError::Trust)));
    assert!(matches!(signer.mint(vec![Caveat { kind: CaveatKind::ExpiresAtUnixSeconds(NOW) }], key(4), &trust, NOW), Err(VerifyError::Time)));
    assert!(matches!(signer.mint(vec![], key(3), &trust, NOW), Err(VerifyError::RepeatedKey)));
    Ok(())
}
