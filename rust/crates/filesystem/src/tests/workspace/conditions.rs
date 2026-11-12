//! Conditional writes and removals.
use super::*;
use crate::FileCondition;

fn sha256(bytes: &[u8]) -> [u8; 32] {
    sha2::Digest::finalize(sha2::Digest::chain_update(
        <sha2::Sha256 as sha2::Digest>::new(),
        bytes,
    ))
    .into()
}

async fn seeded(
    name: &str,
) -> Result<Workspace<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>, Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace(name))?;
    let mut create = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    create.write_text("/file.txt", "current").await?;
    assert!(matches!(
        commit(&mut create).await?,
        TransactionCommit::Committed(_)
    ));
    Ok(workspace)
}

#[tokio::test]
async fn write_if_checks_absence_identity_and_digest() -> Result<(), Box<dyn Error>> {
    let workspace = seeded("conditional-write").await?;
    let actual = unbounded!(workspace, stat("/file.txt"))?.file_id;
    let mut stale = actual.into_bytes();
    stale[0] ^= 1;
    let stale = FileId::from_bytes(stale);
    let mut write = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    for refused in [
        FileCondition::Absent,
        FileCondition::FileId(stale),
        FileCondition::Digest(sha256(b"other")),
    ] {
        assert!(matches!(
            unbounded!(
                write,
                write_if("/file.txt", OwnedBytes::from_static(b"lost"), refused)
            ),
            Err(WorkspaceError::StaleIdentity)
        ));
    }
    assert!(matches!(
        unbounded!(
            write,
            write_if(
                "/new.txt",
                OwnedBytes::from_static(b"fresh"),
                FileCondition::FileId(actual)
            )
        ),
        Err(WorkspaceError::StaleIdentity)
    ));
    unbounded!(
        write,
        write_if(
            "/file.txt",
            OwnedBytes::from_static(b"by-digest"),
            FileCondition::Digest(sha256(b"current"))
        )
    )?;
    unbounded!(
        write,
        write_if(
            "/new.txt",
            OwnedBytes::from_static(b"fresh"),
            FileCondition::Absent
        )
    )?;
    assert!(matches!(
        commit(&mut write).await?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        unbounded!(workspace, read("/file.txt", 64))?.as_ref(),
        b"by-digest"
    );
    assert_eq!(
        unbounded!(workspace, read("/new.txt", 64))?.as_ref(),
        b"fresh"
    );
    Ok(())
}

/// Negative control: the condition is fenced to commit time. A concurrent
/// replacement that lands after `write_if` passed makes the commit conflict
/// instead of overwriting the other writer's file.
#[tokio::test]
async fn write_if_conflicts_when_the_condition_changes_before_commit() -> Result<(), Box<dyn Error>>
{
    let workspace = seeded("fenced-write").await?;
    let actual = unbounded!(workspace, stat("/file.txt"))?.file_id;
    let mut guarded = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(
        guarded,
        write_if(
            "/file.txt",
            OwnedBytes::from_static(b"guarded"),
            FileCondition::FileId(actual)
        )
    )?;
    let mut racer = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(racer, remove("/file.txt"))?;
    racer.write_text("/file.txt", "racer").await?;
    assert!(matches!(
        commit(&mut racer).await?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        commit(&mut guarded).await?,
        TransactionCommit::Conflict { .. }
    ));
    assert_eq!(
        unbounded!(workspace, read("/file.txt", 64))?.as_ref(),
        b"racer"
    );
    Ok(())
}

#[tokio::test]
async fn remove_when_requires_the_condition() -> Result<(), Box<dyn Error>> {
    let workspace = seeded("conditional-removal").await?;
    let mut remove = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    assert!(matches!(
        unbounded!(
            remove,
            remove_when("/file.txt", FileCondition::Digest(sha256(b"other")))
        ),
        Err(WorkspaceError::StaleIdentity)
    ));
    assert!(matches!(
        unbounded!(remove, remove_when("/file.txt", FileCondition::Absent)),
        Err(WorkspaceError::StaleIdentity)
    ));
    unbounded!(
        remove,
        remove_when("/file.txt", FileCondition::Digest(sha256(b"current")))
    )?;
    assert!(matches!(
        commit(&mut remove).await?,
        TransactionCommit::Committed(_)
    ));
    assert!(unbounded!(workspace, stat("/file.txt")).is_err());
    Ok(())
}

#[tokio::test]
async fn multi_chunk_digest_retires_consumed_output_before_the_next_read()
-> Result<(), Box<dyn Error>> {
    use acyclic_test_support::scale::{Bound, assert_scales};

    let cancellation = CancellationToken::new();
    let mut hashed = Vec::new();
    for chunks in [16_u64, 128] {
        let fs = Fs::memory();
        let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
        config.limits.maximum_object_bytes = 128 * 1024;
        let workspace = fs
            .create_workspace_with_config(
                "bounded-digest",
                config,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let length = chunks * crate::kernel::CONTENT_READ_CHUNK_BYTES;
        let bytes = OwnedBytes::from(vec![b'x'; usize::try_from(length)?]);
        let digest = sha256(&bytes);
        let mut seed = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
        unbounded!(seed, write("/content", bytes))?;
        assert!(matches!(
            commit(&mut seed).await?,
            TransactionCommit::Committed(_)
        ));
        let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
        let budget = WorkBudget {
            peak_allocation_bytes: 16 * config.limits.maximum_object_bytes,
            ..WorkBudget::UNBOUNDED
        };
        let receipt = transaction
            .remove_when(
                "/content",
                FileCondition::Digest(digest),
                budget,
                &cancellation,
            )
            .await?;
        assert!(receipt.work.peak_allocation_bytes <= budget.peak_allocation_bytes);
        assert!(receipt.work.bytes_hashed >= length);
        hashed.push((length, receipt.work.bytes_hashed));
        assert!(matches!(
            commit(&mut transaction).await?,
            TransactionCommit::Committed(_)
        ));
        assert!(unbounded!(workspace, stat("/content")).is_err());
    }
    assert_scales(Bound::Linear, hashed[0], hashed[1]);
    Ok(())
}
