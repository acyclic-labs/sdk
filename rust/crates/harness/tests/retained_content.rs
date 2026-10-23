//! Durable content retention across workspace advances, restart, and local GC.
#![allow(
    clippy::too_many_lines,
    reason = "one ordered scenario keeps each step next to the state it checks"
)]
#![cfg(feature = "filesystem-local")]

use acyclic_fs::{CancellationToken, Fs, LocalOptions};
use acyclic_harness::conversation::{
    ContentGrant, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
};
use acyclic_harness::core::{AggregateKind, Authority, AuthorityIssuer};
use acyclic_harness::filesystem::FilesystemHost;
use acyclic_harness::resources::ProviderRef;
use acyclic_harness::{AgentId, Capabilities, IdempotencyKey, Result};

#[tokio::test]
async fn admitted_and_orphaned_generations_survive_restart_and_gc() -> Result<()> {
    let directory =
        tempfile::tempdir().map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    let options = LocalOptions::new(directory.path());
    let provider = ProviderRef::new(
        "retention-test".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let agent = AgentId::from_bytes([31; 16]);
    let volume = VolumeRef::new(
        provider.clone(),
        "private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    let issuer = AuthorityIssuer::new(
        "retention-test",
        [32; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "retention".into(),
        },
    );
    let scope = issuer
        .issue(
            "agent",
            &Capabilities::new([
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let write = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        &volume,
        VolumeOperation::Write,
        65_536,
        &mut None,
    )?;
    let read = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        &volume,
        VolumeOperation::Read,
        65_536,
        &mut None,
    )?;

    let host = FilesystemHost::new(
        Fs::local(options.clone())
            .await
            .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?,
        &provider,
        None,
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
            None,
            acyclic_harness::conversation::ContentAccess::default(),
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
            None,
            acyclic_harness::conversation::ContentAccess::default(),
        )
        .await?;
    let attached = AgentId::from_bytes([33; 16]);
    let attached_scope = issuer
        .issue(
            "unrelated-reader",
            &Capabilities::new([
                admitted.read_capability(&mut None)?,
                volume.directory_read_capability("messages", &mut None)?,
            ]),
            Some(attached),
            None,
            &mut None,
        )
        .expect("scope proof");
    let attached_read = ContentGrant::verify_file_read(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &attached_scope,
        &admitted,
    )?;
    assert_eq!(
        host.read_content(&admitted, &attached_read, 64, None)
            .await?
            .as_ref(),
        b"first"
    );
    assert!(
        host.read_content(&orphan, &attached_read, 64, None)
            .await
            .is_err()
    );
    let attached_directory = ContentGrant::verify_directory_read(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &attached_scope,
        &volume,
        "messages",
        &mut None,
    )?;
    let (listed_generation, page) = host
        .list_private_directory(
            &volume,
            &attached_directory,
            "messages",
            acyclic_harness::filesystem::DirectoryOptions {
                generation: None,
                after: None,
                maximum_entries: 10,
                ceiling: None,
            },
        )
        .await?;
    assert_eq!(page.entries.len(), 1);
    assert_eq!(
        host.read_private_path(
            &volume,
            &attached_directory,
            "messages/current.txt",
            Some(&listed_generation),
            64,
            acyclic_harness::conversation::ContentAccess::default(),
        )
        .await?
        .1
        .as_ref(),
        b"second"
    );
    assert!(
        ContentGrant::verify(
            &issuer.verifier(&mut None).expect("verifier copy"),
            &attached_scope,
            &volume,
            VolumeOperation::Write,
            65_536,
            &mut None,
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
        None,
        acyclic_harness::conversation::ContentAccess::default(),
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
            None,
            acyclic_harness::conversation::ContentAccess::default(),
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
            None,
            acyclic_harness::conversation::ContentAccess::default()
        )
        .await
        .is_err()
    );
    // Never read this generation before collection, so no retaining read can
    // protect it. Overwrite the same path before dropping every SDK handle.
    let unread = host
        .put_content(
            &volume,
            &write,
            "messages/unread.txt",
            b"unread original",
            "text/plain",
            "unread.txt",
            64,
            &IdempotencyKey::new("unread-original")?,
            None,
            acyclic_harness::conversation::ContentAccess::default(),
        )
        .await?;
    host.put_content(
        &volume,
        &write,
        "messages/unread.txt",
        b"overwritten",
        "text/plain",
        "unread.txt",
        64,
        &IdempotencyKey::new("unread-overwrite")?,
        None,
        acyclic_harness::conversation::ContentAccess::default(),
    )
    .await?;
    drop(host);

    let fs = Fs::local(options)
        .await
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    fs.collect_local_garbage(None, &CancellationToken::new())
        .await
        .map_err(|error| acyclic_harness::Error::Storage(error.to_string()))?;
    let reopened = FilesystemHost::new(fs, &provider, None)?;
    let recovered = reopened
        .content_for_operation(
            &volume,
            &write,
            &IdempotencyKey::new("unread-original")?,
            "messages/unread.txt",
            64,
            acyclic_harness::conversation::ContentAccess::default(),
        )
        .await?
        .expect("published operation survives cold collection");
    assert_eq!(recovered, unread);
    let retried = reopened
        .put_content(
            &volume,
            &write,
            "messages/unread.txt",
            b"unread original",
            "text/plain",
            "unread.txt",
            64,
            &IdempotencyKey::new("unread-original")?,
            None,
            acyclic_harness::conversation::ContentAccess::default(),
        )
        .await?;
    assert_eq!(retried, unread);
    assert_eq!(
        reopened
            .read_content(&unread, &read, 64, None)
            .await?
            .as_ref(),
        b"unread original"
    );
    assert_eq!(
        reopened
            .read_content(&admitted, &read, 64, None)
            .await?
            .as_ref(),
        b"first"
    );
    assert_eq!(
        reopened
            .read_content(&orphan, &read, 64, None)
            .await?
            .as_ref(),
        b"second"
    );
    assert_eq!(
        reopened
            .read_content(&admitted, &attached_read, 64, None)
            .await?
            .as_ref(),
        b"first"
    );
    assert!(
        reopened
            .read_content(&orphan, &attached_read, 64, None)
            .await
            .is_err()
    );
    let (relisted_generation, relisted) = reopened
        .list_private_directory(
            &volume,
            &attached_directory,
            "messages",
            acyclic_harness::filesystem::DirectoryOptions {
                generation: Some(&listed_generation),
                after: None,
                maximum_entries: 10,
                ceiling: None,
            },
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
                64,
                acyclic_harness::conversation::ContentAccess::default(),
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
                64,
                acyclic_harness::conversation::ContentAccess::default(),
            )
            .await?
            .1
            .as_ref(),
        b"third"
    );
    Ok(())
}
