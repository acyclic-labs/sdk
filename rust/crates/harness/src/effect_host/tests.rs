use super::*;

use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
use acyclic_stream::MemoryStream;

fn budget() -> WorkBudget {
    WorkBudget {
        bytes_copied: 1_048_576,
        bytes_encoded: 1_048_576,
        bytes_hashed: 1_048_576,
        items_examined: 1024,
        allocation_operations: 1024,
        peak_allocation_bytes: 1_048_576,
        ..WorkBudget::default()
    }
}

fn ordinary_host() -> Result<ConversationEffectHost<MemoryStream>> {
    let authority = Authority {
        kind: crate::core::AggregateKind::Conversation,
        id: "native-effect-metadata-control".into(),
    };
    let issuer = AuthorityIssuer::new("original-effect-issuer", [41; 32], authority.clone());
    let scope = issuer
        .issue(
            "original-effect-scope",
            &crate::Capabilities::new([capability::EFFECT_RUN]),
            Some(crate::AgentId::from_bytes([42; 16])),
            None,
            &mut None,
        )
        .expect("scope proof");
    let files = Arc::new(crate::filesystem::FilesystemHost::new(
        acyclic_fs::Fs::memory(),
        crate::resources::ProviderRef::new(
            "effect-metadata-control".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?,
    )?);
    let content = crate::filesystem::FilesystemContentVerifier::new(
        files,
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        crate::conversation::Limits::default().file_bytes,
        None,
    )?;
    ConversationEffectHost::new(
        Arc::new(MemoryStream::default()),
        authority,
        issuer,
        scope,
        SchemaRegistry::new(),
        content,
        EffectRegistry::new(None)?,
        None,
        None,
    )
}

#[test]
fn native_host_metadata_survives_shared_clone_until_physical_drop() -> Result<()> {
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(budget(), &cancellation)
        .expect("finite original metadata ledger");
    let baseline = work
        .remaining()
        .expect("original allowance")
        .peak_allocation_bytes;
    let ordinary = ordinary_host()?;
    let host = ordinary.clone_with_native_work(&work)?;
    assert_eq!(host.authority, ordinary.authority);
    assert_eq!(host.scope, ordinary.scope);
    assert!(host.providers.require_native_metadata(&work).is_ok());
    assert!(
        host.providers
            .clone()
            .require_native_metadata(&work)
            .is_err()
    );
    let shared = host.into_arc_with_native_work(&work)?;
    let alias = Arc::clone(&shared);
    assert!(
        work.remaining()
            .expect("retained host")
            .peak_allocation_bytes
            < baseline
    );
    drop(shared);
    assert!(
        work.remaining()
            .expect("retained alias")
            .peak_allocation_bytes
            < baseline
    );
    drop(alias);
    assert_eq!(
        work.remaining()
            .expect("dropped payload")
            .peak_allocation_bytes,
        baseline
    );
    assert_eq!(work.receipt().backend_read_operations, 0);
    assert_eq!(work.receipt().backend_write_operations, 0);
    Ok(())
}

#[test]
fn native_catalog_denies_metadata_before_allocation() {
    let cancellation = CancellationToken::new();
    let mut denied = budget();
    denied.peak_allocation_bytes = NativeMaterializationWork::accounting_state_bytes()
        + std::mem::size_of::<EffectRegistry>() as u64
        - 1;
    let work = NativeMaterializationWork::new(denied, &cancellation)
        .expect("only meter backing is admitted");
    let before = work.receipt().allocation_operations;
    assert!(matches!(
        EffectRegistry::new(Some(&work)),
        Err(Error::NativeWork { .. })
    ));
    assert_eq!(work.receipt().allocation_operations, before);

    let work = NativeMaterializationWork::new(budget(), &cancellation)
        .expect("original cancellable ledger");
    cancellation.cancel();
    let before = work.receipt().allocation_operations;
    assert!(matches!(
        EffectRegistry::new(Some(&work)),
        Err(Error::NativeWork { .. })
    ));
    assert_eq!(work.receipt().allocation_operations, before);
}

#[test]
fn native_host_rejects_real_foreign_scope_and_cancelled_metadata_source() -> Result<()> {
    let cancellation = CancellationToken::new();
    let work =
        NativeMaterializationWork::new(budget(), &cancellation).expect("original scope ledger");
    let baseline = work
        .remaining()
        .expect("original allowance")
        .peak_allocation_bytes;
    let mut host = ordinary_host()?;
    let foreign = AuthorityIssuer::new("foreign-effect-issuer", [43; 32], host.authority.clone());
    host.scope = foreign
        .issue(
            "foreign-effect-scope",
            &crate::Capabilities::new([capability::EFFECT_RUN]),
            Some(crate::AgentId::from_bytes([42; 16])),
            None,
            &mut None,
        )
        .expect("scope proof");
    assert!(host.clone_with_native_work(&work).is_err());
    assert_eq!(
        work.remaining()
            .expect("rejected clone dropped")
            .peak_allocation_bytes,
        baseline
    );
    cancellation.cancel();
    let before = work.receipt().allocation_operations;
    assert!(matches!(
        host.clone_with_native_work(&work),
        Err(Error::NativeWork { .. })
    ));
    assert_eq!(work.receipt().allocation_operations, before);
    Ok(())
}

#[test]
fn native_host_refuses_a_foreign_arc_ledger_before_allocation() -> Result<()> {
    let cancellation = CancellationToken::new();
    let original =
        NativeMaterializationWork::new(budget(), &cancellation).expect("original ledger");
    let foreign =
        NativeMaterializationWork::new(budget(), &cancellation).expect("different original ledger");
    let baseline = original
        .remaining()
        .expect("original allowance")
        .peak_allocation_bytes;
    let host = ordinary_host()?.clone_with_native_work(&original)?;
    let before = foreign.receipt().allocation_operations;
    assert!(matches!(
        host.into_arc_with_native_work(&foreign),
        Err(Error::NativeBoundary { .. })
    ));
    assert_eq!(foreign.receipt().allocation_operations, before);
    assert_eq!(
        original
            .remaining()
            .expect("rejected host dropped")
            .peak_allocation_bytes,
        baseline
    );
    Ok(())
}
