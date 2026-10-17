//! Harness content on an Objects provider. An owner stages files in one
//! bucket through `ObjectContentStore`; a reader with a delegated grant reads
//! one back. Every read must return exactly the staged bytes, every refusal
//! must be the expected one, and the provider-independent trace (descriptors,
//! replays and refusal kinds, never server-assigned versions) is what every
//! Objects provider must reproduce.
use super::{Remote, TestResult};
use acyclic_harness::{
    AgentId, Capabilities, OperationId,
    conversation::{
        Attachment, ContentPublisher as _, ContentResidencyVerifier as _, Limits, VolumeClass,
        VolumeOperation, VolumeOwner, VolumeRef, load_manifest,
    },
    core::{AggregateKind, Authority, AuthorityIssuer},
    objects::ObjectContentStore,
    resources::ProviderRef,
};
use acyclic_objects::v1::{GrpcObjects, ObjectsProvider, wire};
use serde_json::{Value, json};
use std::sync::Arc;

/// What one run stages, and what its reads must return.
pub(super) struct Content<'a> {
    /// The bytes the owner stages.
    pub staged: &'a [u8],
    /// The bytes every read must return.
    pub expected: &'a [u8],
}

impl<'a> Content<'a> {
    pub(super) fn exact(bytes: &'a [u8]) -> Self {
        Self {
            staged: bytes,
            expected: bytes,
        }
    }
}

const MEDIA: &str = "text/plain";
const MAXIMUM: u64 = 4_096;

impl Remote {
    /// An Objects client for this endpoint, trusting only its CA.
    pub(super) async fn objects(&self) -> TestResult<Arc<GrpcObjects>> {
        let ca = std::fs::read(&self.ca_file)?;
        Ok(Arc::new(
            GrpcObjects::connect(&self.endpoint, &self.bearer, Some(&ca), None).await?,
        ))
    }
}

fn check(what: &str, read: &[u8], expected: &[u8]) -> TestResult {
    if read != expected {
        return Err(format!(
            "{what} read {:?}, not {:?}",
            String::from_utf8_lossy(read),
            String::from_utf8_lossy(expected)
        )
        .into());
    }
    Ok(())
}

/// Stages `bytes` at `path` as `operation`, returning the staged file.
async fn stage(
    store: &ObjectContentStore,
    operation: u8,
    path: &str,
    bytes: &[u8],
) -> acyclic_harness::Result<acyclic_harness::conversation::FileRef> {
    let (file, _) = store
        .stage(
            OperationId::from_bytes([operation; 16]),
            path,
            bytes,
            MEDIA,
            "one.txt",
            None,
        )?
        .await?;
    Ok(file)
}

/// Reads `file` from `store`, allowing at most `maximum` bytes.
async fn read(
    store: &ObjectContentStore,
    file: &acyclic_harness::conversation::FileRef,
    maximum: u64,
) -> acyclic_harness::Result<acyclic_stream::OwnedBytes> {
    store.read(file, maximum, None)?.await
}

/// Runs the content scenario in a new `bucket` on `objects`, checks its oracle
/// against `content`, and returns the trace every provider must reproduce.
pub(super) async fn run(
    objects: Arc<dyn ObjectsProvider>,
    bucket: &str,
    content: &Content<'_>,
) -> TestResult<Value> {
    let bucket = objects
        .create_bucket(wire::CreateBucketRequest {
            name: bucket.into(),
            mutation: None,
        })
        .await?
        .bucket
        .ok_or("the created bucket has no identity")?;
    let owner = AgentId::from_bytes([7; 16]);
    let reader = AgentId::from_bytes([8; 16]);
    let provider = ProviderRef::new(
        "cloud-local".into(),
        "objects".into(),
        "1".into(),
        &mut None,
    )?;
    let volume = VolumeRef::new(
        provider.clone(),
        bucket.name.clone(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(owner),
        &mut None,
    )?;
    let issuer = AuthorityIssuer::new(
        "remote-objects",
        [9; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "remote-objects".into(),
        },
    );
    let owner_scope = issuer.issue(
        "owner",
        &Capabilities::new([
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(owner),
        None,
        &mut None,
    )?;
    let store = async |scope| {
        ObjectContentStore::new(
            objects.clone(),
            bucket.clone(),
            provider.clone(),
            volume.clone(),
            (issuer.verifier(&mut None)?, owner_scope.clone()),
            scope,
            MAXIMUM,
        )
        .await
    };
    let owner_store = store(owner_scope.clone()).await?;
    let staged = content.staged;
    let file = stage(&owner_store, 10, "notes/one.txt", staged).await?;
    // A retried operation, and another operation staging the same content,
    // both resolve to the staged file.
    let replayed = stage(&owner_store, 10, "notes/one.txt", staged).await? == file;
    let deduplicated = stage(&owner_store, 42, "notes/one.txt", staged).await? == file;
    let owned = read(&owner_store, &file, MAXIMUM).await?;
    check("the owner", owned.as_ref(), content.expected)?;
    let range = acyclic_fs::ByteRange {
        offset: 1,
        length: 3,
    };
    let ranged = owner_store.read_range(&file, range, None)?.await?;
    check(
        "a range",
        ranged.as_ref(),
        content.expected.get(1..4).unwrap_or_default(),
    )?;
    owner_store.verify(&file, None)?.await?;
    // An attachment manifest round-trips through the same bucket.
    let items = vec![Attachment {
        file: file.clone(),
        label: None,
    }];
    let manifest_bytes = serde_json::to_vec(&items)?;
    let manifest = owner_store
        .stage(
            OperationId::from_bytes([12; 16]),
            "lists/one.json",
            &manifest_bytes,
            "application/vnd.acyclic.harness.attachments+json",
            "one.json",
            None,
        )?
        .await?
        .0;
    owner_store
        .verify_manifest(&manifest, 1, &Limits::default(), None)?
        .await?;
    if load_manifest(owner_store.as_ref(), &manifest, 1, None)
        .await?
        .0
        != items
    {
        return Err("the manifest did not round-trip".into());
    }
    // A delegated reader reads the file, and can neither write nor read
    // beyond its byte allowance.
    let delegated = issuer.delegate_private_file_read(&owner_scope, reader, "attached", &file)?;
    let attached = store(delegated).await?;
    let shared = read(&attached, &file, MAXIMUM).await?;
    check("the reader", shared.as_ref(), content.expected)?;
    let writer = attached.bindings().writer.is_some();
    let denied = stage(&attached, 11, "notes/denied.txt", b"denied").await;
    if !matches!(&denied, Err(acyclic_harness::Error::Unauthorized(message))
        if message == "Objects writer is not bound to the original owner")
    {
        return Err(format!("reader publication returned {denied:?}").into());
    }
    let oversized = read(&attached, &file, 1).await;
    if !matches!(&oversized, Err(acyclic_harness::Error::Invalid(message))
        if message == "content exceeds selected byte allowance")
    {
        return Err(format!("bounded read returned {oversized:?}").into());
    }
    if (replayed, deduplicated, writer) != (true, true, false) {
        return Err(format!(
            "replayed {replayed}, deduplicated {deduplicated}, reader writes {writer}"
        )
        .into());
    }
    Ok(json!({
        "file": {
            "path": file.path(),
            "name": file.display_name(),
            "descriptor": file.descriptor(),
        },
        "manifest": manifest.descriptor(),
        "refused": {
            "reader stage": "unauthorized: Objects writer is not bound to the original owner",
            "over allowance": "invalid request: content exceeds selected byte allowance",
        },
    }))
}
