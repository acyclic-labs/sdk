//! The real signed scope path is shared by properties and the access fuzzer.
use super::*;

pub(crate) fn check_filesystem_refinement(parent: &acyclic_fs::access::Capability) -> Result<()> {
    if parent.validate().is_err() {
        return Ok(());
    }
    let issuer = AuthorityRoot::new(
        "refinement",
        [29; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "refinement".into(),
        },
    );
    let verifier = issuer.verifier(&mut None)?;
    let root = issuer.bootstrap_scope(
        "root",
        &Capabilities::new([parent.to_scope_label().map_err(unauthorized)?]),
        None,
        &mut None,
    )?;
    let child = parent.clone().attenuate(Vec::new(), None);
    let child = issuer.attenuate(
        &root,
        "child",
        &Capabilities::new([child.to_scope_label().map_err(unauthorized)?]),
    )?;
    verifier.verify(&child, &mut None)?;
    let verified = verify(&verifier, issuer.audience(), &child, now(), &mut None)?;
    // Possessing the append key is insufficient to undo the previous ceiling.
    // Bypass the caller's subset check, sign the actual block, and admit it.
    let mut widened = child.clone();
    widened.id = "widened".into();
    widened.capabilities = root.capabilities.clone();
    widened.parent_proof = Some(*child.proof);
    append(&verified, &mut widened, issuer.audience(), None, None)?;
    assert!(
        verifier.verify(&widened, &mut None).is_err(),
        "signed refinement removed its ancestor ceiling"
    );
    // A label without its certified resource grant cannot mint authority.
    let mut ungranted = child.clone();
    ungranted.id = "ungranted".into();
    ungranted.parent_proof = Some(*child.proof);
    ungranted.capabilities = Capabilities::default();
    let label = child.capabilities.iter().next().unwrap().to_owned();
    let mut caveats = facts(&ungranted, &resource(issuer.audience())?, None)?;
    caveats.push(Caveat {
        kind: CaveatKind::Label(label.clone()),
    });
    let forged = verified
        .attenuate(caveats, &entropy())
        .map_err(unauthorized)?;
    ungranted.capabilities = Capabilities::new([label]);
    retain(&mut ungranted, &forged, &mut None)?;
    assert!(
        verifier.verify(&ungranted, &mut None).is_err(),
        "filesystem label gained uncertified authority"
    );
    Ok(())
}
