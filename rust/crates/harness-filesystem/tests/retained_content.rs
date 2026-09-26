//! Durable content retention across workspace advances, restart, and local GC.
#![allow(clippy::too_many_lines)]
#![cfg(feature = "local")]

use acyclic_fs::{CancellationToken, Fs, LocalOptions, WorkBudget};
use acyclic_harness::conversation::{
    ContentGrant, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
};
use acyclic_harness::core::{AggregateKind, Authority, AuthorityIssuer};
use acyclic_harness::resources::ProviderRef;
use acyclic_harness::{AgentId, Capabilities, IdempotencyKey, Result};
use acyclic_harness_filesystem::FilesystemHost;

#[tokio::test]
async fn admitted_and_orphaned_generations_survive_restart_and_gc() -> Result<()> {
    let directory =
        tempfile::tempdir().map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    let options = LocalOptions::new(directory.path());
    let provider = ProviderRef::new("retention-test", "filesystem", "2")?;
    let agent = AgentId::from_bytes([31; 16]);
    let volume = VolumeRef::new(
        provider.clone(),
        "private",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    let issuer = AuthorityIssuer::new(
        "retention-test",
        [32; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "retention".into(),
        },
    );
    let scope = issuer.root_for_agent(
        agent,
        "agent",
        Capabilities::new([
            volume.capability(VolumeOperation::Read)?,
            volume.capability(VolumeOperation::Write)?,
        ]),
    );
    let write = ContentGrant::verify(&issuer.verifier(), &scope, &volume, VolumeOperation::Write)?;
    let read = ContentGrant::verify(&issuer.verifier(), &scope, &volume, VolumeOperation::Read)?;

    let host = FilesystemHost::new(
        Fs::local(options.clone())
            .await
            .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?,
        provider.clone(),
    )?;
    host.create_volume(&volume).await?;
    let admitted = host
        .put_content(
            &volume,
            &write,
            "messages/current.txt",
            b"first",
            "text/plain",
            "current.txt",
            64,
            &IdempotencyKey::new("admitted-content")?,
        )
        .await?;
    let orphan = host
        .put_content(
            &volume,
            &write,
            "messages/current.txt",
            b"second",
            "text/plain",
            "current.txt",
            64,
            &IdempotencyKey::new("unpublished-content")?,
        )
        .await?;
    // Sharing an exact ref or bounded subtree does not require a fork, a copied
    // volume, or parent/child lineage; neither grants the reader a write.
    let attached = AgentId::from_bytes([33; 16]);
    let attached_scope = issuer.root_for_agent(
        attached,
        "unrelated-reader",
        Capabilities::new([
            admitted.read_capability()?,
            volume.directory_read_capability("messages")?,
        ]),
    );
    let attached_read =
        ContentGrant::verify_file_read(&issuer.verifier(), &attached_scope, &admitted)?;
    assert_eq!(
        host.read_content(&admitted, &attached_read, 64)
            .await?
            .as_ref(),
        b"first"
    );
    assert!(
        host.read_content(&orphan, &attached_read, 64)
            .await
            .is_err()
    );
    let attached_directory = ContentGrant::verify_directory_read(
        &issuer.verifier(),
        &attached_scope,
        &volume,
        "messages",
    )?;
    let (listed_generation, page) = host
        .list_private_directory(&volume, &attached_directory, "messages", None, None, 10)
        .await?;
    assert_eq!(page.entries.len(), 1);
    assert_eq!(
        host.read_private_path(
            &volume,
            &attached_directory,
            "messages/current.txt",
            Some(&listed_generation),
            64
        )
        .await?
        .1
        .as_ref(),
        b"second"
    );
    assert!(
        ContentGrant::verify(
            &issuer.verifier(),
            &attached_scope,
            &volume,
            VolumeOperation::Write,
        )
        .is_err()
    );
    host.put_content(
        &volume,
        &write,
        "messages/current.txt",
        b"third",
        "text/plain",
        "current.txt",
        64,
        &IdempotencyKey::new("advanced-head")?,
    )
    .await?;
    let retried = host
        .put_content(
            &volume,
            &write,
            "messages/current.txt",
            b"first",
            "text/plain",
            "current.txt",
            64,
            &IdempotencyKey::new("admitted-content")?,
        )
        .await?;
    assert_eq!(retried, admitted);
    assert!(
        host.put_content(
            &volume,
            &write,
            "messages/current.txt",
            b"different",
            "text/plain",
            "current.txt",
            64,
            &IdempotencyKey::new("admitted-content")?,
        )
        .await
        .is_err()
    );
    drop(host);

    Fs::collect_local_garbage(
        options.clone(),
        16,
        1_024,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    let reopened = FilesystemHost::new(
        Fs::local(options)
            .await
            .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?,
        provider,
    )?;
    assert_eq!(
        reopened.read_content(&admitted, &read, 64).await?.as_ref(),
        b"first"
    );
    assert_eq!(
        reopened.read_content(&orphan, &read, 64).await?.as_ref(),
        b"second"
    );
    assert_eq!(
        reopened
            .read_content(&admitted, &attached_read, 64)
            .await?
            .as_ref(),
        b"first"
    );
    assert!(
        reopened
            .read_content(&orphan, &attached_read, 64)
            .await
            .is_err()
    );
    let (relisted_generation, relisted) = reopened
        .list_private_directory(
            &volume,
            &attached_directory,
            "messages",
            Some(&listed_generation),
            None,
            10,
        )
        .await?;
    assert_eq!(relisted_generation, listed_generation);
    assert_eq!(relisted.entries, page.entries);
    assert_eq!(
        reopened
            .read_private_path(
                &volume,
                &attached_directory,
                "messages/current.txt",
                Some(&listed_generation),
                64
            )
            .await?
            .1
            .as_ref(),
        b"second"
    );
    assert_eq!(
        reopened
            .read_private_path(
                &volume,
                &attached_directory,
                "messages/current.txt",
                None,
                64
            )
            .await?
            .1
            .as_ref(),
        b"third"
    );
    Ok(())
}
