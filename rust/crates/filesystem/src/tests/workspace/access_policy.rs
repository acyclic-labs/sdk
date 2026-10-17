use super::*;
use crate::model::{CheckoutMode, GenerationSelector};
use crate::{Checkout, NamedAttributeWriteMode};
use proptest::prelude::*;

type MemoryCheckout = Checkout<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>;
type MemoryWorkspace = Workspace<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>;

pub(super) async fn current(
    workspace: &MemoryWorkspace,
    writable: bool,
) -> Result<MemoryCheckout, Box<dyn Error>> {
    Ok(unbounded!(
        workspace,
        checkout(
            GenerationSelector::Head,
            if writable {
                CheckoutMode::tracking_transaction()
            } else {
                CheckoutMode::read_only_pinned()
            }
        )
    )?)
}

pub(super) async fn recorded(
    checkout: &mut MemoryCheckout,
) -> Result<Option<Vec<u8>>, Box<dyn Error>> {
    Ok(unbounded!(checkout, read_access_policy())?
        .0
        .map(|chunk| chunk.bytes.to_vec()))
}

pub(super) async fn publish(checkout: &mut MemoryCheckout) -> Result<bool, Box<dyn Error>> {
    let result = checkout
        .commit(
            crate::OperationId::new(),
            None,
            None,
            None,
            crate::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            allow::<FsError>,
        )
        .await?
        .value;
    match result {
        crate::kernel::LivePublicationObservation::Committed { .. } => Ok(true),
        crate::kernel::LivePublicationObservation::AlreadyCommitted { .. } => Ok(false),
        _ => Err("policy publication failed".into()),
    }
}

async fn initial_policy(bytes: &[u8]) -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let operation = crate::OperationId::new();
    let initialization = crate::VolumeInitialization {
        config: VolumeConfig::portable(Lifecycle::Ephemeral),
        access_policy: Some(bytes),
    };
    let initial = fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "initial-policy",
            initialization,
            operation,
            WorkBudget::UNBOUNDED,
            &cancellation,
            None,
        )
        .await?
        .value;
    let workspace = initial.workspace();
    let mut first = current(&workspace, false).await?;
    assert_eq!(first.generation_id(), initial.id());
    let position = unbounded!(first, generation_position(initial.id()))?;
    assert_eq!((position.0.get(), position.1.get()), (1, 1));
    assert_eq!(recorded(&mut first).await?.as_deref(), Some(bytes));

    // Recovery observes the immutable original. It must not restage policy,
    // reauthenticate a moving head as the original, or perform writes.
    let mut writer = current(&workspace, true).await?;
    unbounded!(
        writer,
        write_access_policy(b"later", NamedAttributeWriteMode::Replace)
    )?;
    publish(&mut writer).await?;
    let recovered = fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "initial-policy",
            initialization,
            operation,
            WorkBudget {
                backend_write_operations: 0,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
            None,
        )
        .await?;
    assert_eq!(recovered.value.id(), initial.id());
    assert_eq!(recovered.work.backend_write_operations, 0);
    assert_eq!(recorded(&mut first).await?.as_deref(), Some(bytes));
    let mut latest = current(&workspace, false).await?;
    assert_eq!(
        recorded(&mut latest).await?.as_deref(),
        Some(b"later".as_slice())
    );

    let child = fork_workspace(
        &workspace,
        "initial-policy-fork",
        ForkOptions::from_generation(initial, IdempotencyKey::new()).inherit_paths(vec![]),
    )
    .await?;
    assert_eq!(
        recorded(&mut current(&child, false).await?)
            .await?
            .as_deref(),
        Some(bytes)
    );
    Ok(())
}

#[tokio::test]
async fn access_policy_storage_is_atomic_with_original_creation() -> Result<(), Box<dyn Error>> {
    initial_policy(b"initial authenticated policy").await
}

// Each published root has its own metadata; staging never changes another view,
// and a fork inherits policy even when every namespace path is filtered out.
async fn policy_history(revisions: Vec<Vec<u8>>) -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("policy-parent"))?;
    let mut writer = current(&workspace, true).await?;
    assert_eq!(recorded(&mut writer).await?, None);
    let mut snapshots = Vec::new();
    let mut previous = None;
    let mut head_sequence = 1;
    for (index, revision) in revisions.iter().enumerate() {
        let mut reader = current(&workspace, false).await?;
        let rejected = reader
            .write_access_policy(
                revision,
                NamedAttributeWriteMode::Upsert,
                WorkBudget {
                    backend_read_operations: 0,
                    backend_write_operations: 0,
                    ..WorkBudget::UNBOUNDED
                },
                &CancellationToken::new(),
            )
            .await
            .err()
            .ok_or("read-only view staged policy")?;
        assert!(matches!(rejected.error, FsError::MutationNotAllowed));
        assert_eq!(rejected.work.backend_read_operations, 0);
        assert_eq!(rejected.work.backend_write_operations, 0);
        assert_eq!(recorded(&mut reader).await?, previous);
        snapshots.push((reader, previous.clone(), head_sequence));
        unbounded!(
            writer,
            write_access_policy(
                revision,
                if index == 0 {
                    NamedAttributeWriteMode::Create
                } else {
                    NamedAttributeWriteMode::Replace
                }
            )
        )?;
        assert_eq!(
            recorded(&mut writer).await?.as_deref(),
            Some(revision.as_slice())
        );
        let mut before = current(&workspace, false).await?;
        assert_eq!(recorded(&mut before).await?, previous);
        head_sequence += u64::from(publish(&mut writer).await?);
        let mut after = current(&workspace, false).await?;
        assert_eq!(
            recorded(&mut after).await?.as_deref(),
            Some(revision.as_slice())
        );
        let position = after
            .generation_position(
                after.generation_id(),
                WorkBudget {
                    authority_records_read: 0,
                    object_bytes_read: 0,
                    ..WorkBudget::UNBOUNDED
                },
                &CancellationToken::new(),
            )
            .await?
            .value;
        assert_eq!(
            (position.0.get(), position.1.get()),
            (head_sequence, head_sequence)
        );
        previous = Some(revision.clone());
    }
    let mut latest = current(&workspace, false).await?;
    let observed = unbounded!(workspace, current_access_policy_revision())?
        .ok_or("unchanged head lost its policy observation")?;
    assert_eq!(observed.0, latest.generation_id());
    assert_eq!(observed.1, unbounded!(latest, access_policy_revision())?);
    for (mut snapshot, expected, sequence) in snapshots {
        assert_eq!(recorded(&mut snapshot).await?, expected);
        let position = unbounded!(latest, generation_position(snapshot.generation_id()))?;
        assert_eq!(
            (position.0.get(), position.1.get()),
            (sequence, head_sequence)
        );
        let exact = unbounded!(
            workspace,
            checkout(
                GenerationSelector::Exact(snapshot.generation_id()),
                CheckoutMode::tracking_transaction()
            )
        )?;
        let exact_position = unbounded!(exact, generation_position(snapshot.generation_id()))?;
        assert_eq!(
            (exact_position.0.get(), exact_position.1.get()),
            (sequence, head_sequence)
        );
        if sequence < head_sequence {
            assert_ne!(
                exact_position.0, exact_position.1,
                "negative control: the former own-generation shortcut reports an old root as head"
            );
        }
        if snapshot.generation_id() != latest.generation_id() {
            assert!(unbounded!(snapshot, generation_position(latest.generation_id())).is_err());
        }
    }
    let head = unbounded!(workspace, head())?;
    for (name, paths) in [("policy-full", None), ("policy-empty", Some(Vec::new()))] {
        let options = ForkOptions::from_generation(head.clone(), IdempotencyKey::new());
        let options = match paths {
            None => options,
            Some(paths) => options.inherit_paths(paths),
        };
        let fork = fork_workspace(&workspace, name, options).await?;
        let mut child = current(&fork, false).await?;
        assert_eq!(recorded(&mut child).await?, revisions.last().cloned());
    }
    Ok(())
}

#[tokio::test]
async fn access_policy_storage_preserves_pins_and_filtered_forks() -> Result<(), Box<dyn Error>> {
    policy_history(vec![b"first".to_vec(), b"second".to_vec()]).await
}

#[tokio::test]
async fn access_policy_storage_failed_preconditions_leave_the_record_unchanged()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("policy-preconditions"))?;
    let mut checkout = current(&workspace, true).await?;
    let rejected = checkout
        .write_access_policy(
            b"absent",
            NamedAttributeWriteMode::Replace,
            WorkBudget {
                backend_write_operations: 0,
                ..WorkBudget::UNBOUNDED
            },
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("absent policy was replaced")?;
    assert!(matches!(rejected.error, FsError::CreationRejected));
    assert_eq!(rejected.work.backend_write_operations, 0);
    assert_eq!(recorded(&mut checkout).await?, None);
    unbounded!(
        checkout,
        write_access_policy(b"original", NamedAttributeWriteMode::Create)
    )?;
    assert!(
        unbounded!(
            checkout,
            write_access_policy(b"replacement", NamedAttributeWriteMode::Create)
        )
        .is_err()
    );
    assert_eq!(
        recorded(&mut checkout).await?.as_deref(),
        Some(b"original".as_slice())
    );
    Ok(())
}

#[tokio::test]
async fn access_policy_storage_refuses_oversized_records_before_payload_io()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("policy-bounds"))?;
    let mut checkout = current(&workspace, true).await?;
    let bytes = vec![b'x'; crate::access::MAXIMUM_POLICY_RECORD_BYTES as usize + 1];
    let failure = checkout
        .write_access_policy(
            &bytes,
            NamedAttributeWriteMode::Create,
            WorkBudget {
                backend_write_operations: 0,
                ..WorkBudget::UNBOUNDED
            },
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("oversized policy was stored")?;
    assert!(matches!(
        failure.error,
        FsError::FileRead(crate::kernel::FileRangeReadError::InvalidRange)
    ));
    assert_eq!(failure.work.backend_write_operations, 0);
    assert!(!checkout.has_pending_mutations());

    // A legacy writer can leave an invalid record; loading must reject its
    // authenticated length before reading/materializing the payload.
    let key = crate::kernel::AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        crate::access::POLICY_METADATA_KEY.to_vec(),
        255,
    )?;
    let root = checkout.root().root_file_id;
    unbounded!(
        checkout,
        change_named_attribute(root, &key, Some((&bytes, NamedAttributeWriteMode::Create)))
    )?;
    let failure = checkout
        .read_access_policy(
            WorkBudget {
                object_bytes_read: 16_384,
                ..WorkBudget::UNBOUNDED
            },
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("oversized legacy policy was read")?;
    assert!(matches!(
        failure.error,
        FsError::FileRead(crate::kernel::FileRangeReadError::InvalidRange)
    ));
    assert!(failure.work.object_bytes_read < crate::access::MAXIMUM_POLICY_RECORD_BYTES);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn access_policy_storage_revisions_are_isolated(
        revisions in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..96), 1..5),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        runtime.block_on(policy_history(revisions))
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
    }

    #[test]
    fn access_policy_storage_initial_revision_is_durable(
        bytes in prop::collection::vec(any::<u8>(), 0..96),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        runtime.block_on(initial_policy(&bytes))
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}
