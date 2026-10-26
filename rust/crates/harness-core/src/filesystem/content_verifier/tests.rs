use super::*;
use crate::core::Authority;
use crate::{
    Capabilities,
    core::{AggregateKind, AuthorityRoot},
};

#[test]
fn native_reader_reuses_immutable_authentication_and_refuses_revocation() -> Result<()> {
    let provider = ProviderRef::new("reader".into(), "filesystem".into(), "1".into(), &mut None)?;
    let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
    let issuer = AuthorityRoot::new(
        "reader",
        [117; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "reader".into(),
        },
    );
    let scope = issuer.bootstrap_scope(
        "reader",
        &Capabilities::new(["event:append"]),
        None,
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None)?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 1_000_000),
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let reader = FilesystemContentVerifier::new(host.clone(), &verifier, &scope, 4096, Some(&io))?;
    let before = work.receipt().bytes_hashed;
    let examined = work.receipt().items_examined;
    for _ in 0..4 {
        reader.require_native_work(&work)?;
    }
    assert_eq!(
        work.receipt().bytes_hashed,
        before,
        "immutable reader reauthenticated its workload"
    );
    assert_eq!(
        work.receipt().items_examined,
        examined,
        "immutable reader reexamined its workload"
    );
    // The former physical path compares even a retained, unmodified scope.
    verifier.verify(&scope, &mut Some(&mut io.history_io()))?;
    assert!(work.receipt().items_examined > examined);
    let mut forged =
        serde_json::to_value(&scope).map_err(|error| Error::Invalid(error.to_string()))?;
    forged["id"] = "other".into();
    let forged =
        serde_json::from_value(forged).map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(FilesystemContentVerifier::new(host, &verifier, &forged, 4096, Some(&io)).is_err());
    issuer.revoke_workload(&scope)?;
    assert!(
        reader.require_native_work(&work).is_err(),
        "immutable evidence ignored current revocation"
    );
    Ok(())
}
