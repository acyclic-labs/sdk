//! Selected immutable attachment through the real account-scoped Objects engine.
#![cfg(not(target_arch = "wasm32"))]
use acyclic_manifest::{
    AssetRole, DeploymentManifest, EntryAsset, EntryClosure, ManifestError, PreparedEntry,
};
use acyclic_objects::{Blobs, Digest, MemoryMedium, Put, RootId, Store, Verified, wire::ErrorCode};
use acyclic_stream::{OwnedBytes, testing::Counted};
use acyclic_workers::entry::{EntryStore, Error};
use serde_json::json;
use sha2::{Digest as _, Sha256};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
const CODE: &[u8] = b"export default {fetch(){return new Response('selected')}}";
// Opaque verification inputs, not compiler output or executable module fixtures.
const MODULE: &[u8] = b"original selected module bytes";
const CHANGED: &[u8] = b"replaced selected module bytes";

fn manifest(module: &[u8]) -> TestResult<DeploymentManifest> {
    let mut manifest = acyclic_manifest::parse(include_bytes!(
        "../../manifest/fuzz/corpus/manifest_json/valid.json"
    ))?;
    let environment = manifest.environments.first_mut().ok_or("environment")?;
    let entry = environment.entries.first_mut().ok_or("entry")?;
    entry.artifact_sha256 = hex::encode(Sha256::digest(CODE));
    entry.assets = vec![EntryAsset {
        path: "selected.wasm".into(),
        sha256: hex::encode(Sha256::digest(module)),
        size: module.len().try_into()?,
        media_type: "application/wasm".into(),
        role: AssetRole::Module,
    }];
    // Real admitted declarations belonging only to another entry must disappear.
    let mut other = entry.clone();
    other.name = "other".into();
    other.module = "other".into();
    other.assets.clear();
    other.bindings = vec!["PRIVATE".into()];
    other.secrets = vec!["PRIVATE_KEY".into()];
    other.runtime = serde_json::from_value(json!({"kind":"worker","limits":{
        "cpuMillis":1,"memoryBytes":1024,"timeoutMillis":10
    }}))?;
    environment.entries.push(other);
    environment.bindings.push(serde_json::from_value(json!({
        "name":"PRIVATE","capability":"privateGrant","resource":"streams:/private/inbox"
    }))?);
    environment.grants.push(serde_json::from_value(json!({
        "name":"privateGrant","grant":{"resource":{"name":"streams:/private/inbox"},
        "actions":["streams/read"]},"limits":null
    }))?);
    environment.secrets.push(serde_json::from_value(json!({
        "name":"PRIVATE_KEY","reference":"secrets:/private/model"
    }))?);
    Ok(manifest)
}

fn prepare(module: &[u8]) -> TestResult<PreparedEntry> {
    Ok(PreparedEntry::new(
        manifest(module)?,
        "production",
        "agent",
        CODE.to_vec().into(),
        vec![("selected.wasm".into(), module.to_vec().into())],
    )?)
}

fn verified(put: Put) -> TestResult<Verified> {
    match put {
        Put::Created { verified } | Put::AlreadyExists { verified } => Ok(verified),
        Put::Unknown => Err("unexpected unknown publication".into()),
    }
}

fn preparation_controls() -> TestResult {
    let oversized = vec![b' '; EntryClosure::MAX_ENCODED_BYTES + 1];
    assert!(
        matches!(EntryClosure::decode(&oversized, "production", "agent"), Err(error)
        if error == ManifestError::invalid("entryClosure", "encoding-size"))
    );
    let artifact: OwnedBytes = CODE.to_vec().into();
    let module: OwnedBytes = MODULE.to_vec().into();
    let addresses = [artifact.as_ptr(), module.as_ptr()];
    let document = manifest(MODULE)?;
    let before = acyclic_stream::hashed_on_this_thread();
    let prepared = PreparedEntry::new(
        document,
        "production",
        "agent",
        artifact,
        vec![("selected.wasm".into(), module)],
    )?;
    let (descriptor, payloads) = prepared.into_parts();
    let bytes: usize = payloads.iter().map(|body| body.bytes().len()).sum();
    assert_eq!(
        acyclic_stream::hashed_on_this_thread() - before,
        u64::try_from(bytes + descriptor.bytes().len())?
    );
    for (body, address) in payloads.iter().zip(addresses) {
        assert_eq!(
            body.bytes().as_ptr(),
            address,
            "payload ownership survives admission"
        );
    }
    let mut unrelated = manifest(MODULE)?;
    unrelated
        .environments
        .first_mut()
        .ok_or("environment")?
        .entries
        .last_mut()
        .ok_or("other entry")?
        .artifact_sha256 = "33".repeat(32);
    let (unchanged, _) = PreparedEntry::new(
        unrelated,
        "production",
        "agent",
        CODE.to_vec().into(),
        vec![("selected.wasm".into(), MODULE.to_vec().into())],
    )?
    .into_parts();
    assert_eq!(
        unchanged.digest(),
        descriptor.digest(),
        "unrelated entries do not change selected identity"
    );
    let result = PreparedEntry::new(
        manifest(MODULE)?,
        "production",
        "agent",
        CODE.to_vec().into(),
        Vec::new(),
    );
    assert!(
        matches!(result, Err(error) if error == ManifestError::invalid("assets", "missing-asset"))
    );
    let result = PreparedEntry::new(
        manifest(MODULE)?,
        "production",
        "agent",
        CODE.to_vec().into(),
        vec![("selected.wasm".into(), CHANGED.to_vec().into())],
    );
    assert!(
        matches!(result, Err(error) if error == ManifestError::invalid("assets", "asset-digest"))
    );
    Ok(())
}

async fn untrusted_descriptor<P: Blobs>(
    store: &EntryStore<P>,
    provider: &P,
    descriptor: &[u8],
    artifact: Digest,
) -> TestResult {
    let missing = Digest::of(b"absent selected module");
    assert!(
        matches!(provider.put(b"missing closure".to_vec().into(), vec![artifact, missing]).await,
        Err(error) if error.code == ErrorCode::MissingReference)
    );
    let replacement = verified(provider.put(CHANGED.to_vec().into(), Vec::new()).await?)?;
    let mut document: serde_json::Value = serde_json::from_slice(descriptor)?;
    let asset = document
        .get_mut("assets")
        .and_then(|v| v.as_array_mut())
        .and_then(|v| v.first_mut())
        .ok_or("asset reference")?;
    *asset = json!({"digest":replacement.digest.as_bytes(),"length":replacement.length});
    let encoded: OwnedBytes = serde_json::to_vec(&document)?.into();
    let altered = verified(
        provider
            .put(encoded.clone(), vec![artifact, replacement.digest])
            .await?,
    )?;
    let loaded = store.load(&altered, "production", "agent").await?;
    assert!(
        matches!(loaded.module("selected.wasm").await, Err(Error::Manifest(error))
        if error == ManifestError::invalid("assets", "asset-digest"))
    );
    // The same descriptor without actual retention edges cannot be restored.
    // A fresh account avoids reusing the existing immutable envelope's edges.
    let detached = Store::new(MemoryMedium::default()).account(b"detached")?;
    let detached_ref = verified(detached.put(encoded, Vec::new()).await?)?;
    assert!(
        matches!(EntryStore::new(detached).load(&detached_ref, "production", "agent").await,
        Err(Error::Manifest(error)) if error == ManifestError::invalid("entryClosure", "object-references"))
    );
    Ok(())
}

#[tokio::test]
async fn selected_entry_retains_exact_closure_and_loads_lazily() -> TestResult {
    preparation_controls()?;
    let medium = Store::new(MemoryMedium::default());
    let account = medium.account(b"selected")?;
    let provider = Counted::new(account.clone());
    let store = EntryStore::new(provider.clone());
    let first_root = RootId::new(b"deployment:first".to_vec())?;
    let second_root = RootId::new(b"deployment:second".to_vec())?;
    let first = store.publish(&first_root, prepare(MODULE)?).await?;
    assert_eq!(
        provider.requests().data(),
        3,
        "batch, descriptor, retention only"
    );
    assert_eq!(provider.requests().control(), 0);
    assert_eq!(first.retention.references, 1);
    let mut oversized = first.object.clone();
    oversized.length = u64::try_from(EntryClosure::MAX_ENCODED_BYTES)? + 1;
    let before = provider.requests();
    assert!(
        matches!(store.load(&oversized, "production", "agent").await, Err(Error::Manifest(error))
        if error == ManifestError::invalid("entryClosure", "encoding-size"))
    );
    assert_eq!(
        provider.requests().since(before).data(),
        0,
        "size admission precedes I/O"
    );
    let retry = store.publish(&first_root, prepare(MODULE)?).await?;
    let second = store.publish(&second_root, prepare(MODULE)?).await?;
    assert_eq!(
        retry.object.digest, first.object.digest,
        "same intent reconciles the same object"
    );
    assert_eq!(second.object.digest, first.object.digest);
    store.release(&first_root).await?;
    // Three complete collection epochs, not readiness checks: the second root
    // must independently keep all bytes after the first root is released.
    for _ in 0..3 {
        account.collect().await?;
    }
    let before = provider.requests();
    let loaded = store.load(&second.object, "production", "agent").await?;
    assert_eq!(provider.requests().since(before).data(), 1);
    let environment = loaded
        .closure()
        .manifest()
        .environments
        .first()
        .ok_or("selected environment")?;
    assert_eq!(
        (
            environment.entries.len(),
            environment.bindings.len(),
            environment.grants.len(),
            environment.secrets.len()
        ),
        (1, 1, 1, 1)
    );
    let before = provider.requests();
    assert!(
        matches!(loaded.module("other.wasm").await, Err(Error::Manifest(error))
        if error == ManifestError::invalid("assets", "undeclared-asset"))
    );
    assert_eq!(provider.requests().since(before).data(), 0);
    assert_eq!(&**loaded.artifact().await?.body(), CODE);
    let before = provider.requests();
    assert_eq!(&**loaded.module("selected.wasm").await?.body(), MODULE);
    assert_eq!(provider.requests().since(before).data(), 1);
    assert_eq!(provider.requests().control(), 0);
    let artifact = Digest::from_bytes(loaded.closure().artifact().digest);
    untrusted_descriptor(&store, &provider, loaded.descriptor().body(), artifact).await?;
    let changed = store
        .publish(
            &RootId::new(b"deployment:changed".to_vec())?,
            prepare(CHANGED)?,
        )
        .await?;
    assert_ne!(
        first.object.digest, changed.object.digest,
        "unchanged JS cannot hide a changed sibling"
    );
    let foreign = EntryStore::new(medium.account(b"foreign")?);
    assert!(
        matches!(foreign.load(&first.object, "production", "agent").await,
        Err(Error::Objects(error)) if error.code == ErrorCode::NotFound)
    );
    assert!(
        matches!(store.load(&first.object, "production", "foreign").await,
        Err(Error::Manifest(error)) if error.rule == "unknown-entry")
    );
    store.release(&second_root).await?;
    for _ in 0..3 {
        account.collect().await?;
    }
    assert!(
        matches!(store.load(&second.object, "production", "agent").await,
        Err(Error::Objects(error)) if error.code == ErrorCode::NotFound)
    );
    // Real provider refusal ends publication immediately without changing its cause.
    let refusing = Counted::new(acyclic_objects::MemoryObjects::bounded(0));
    assert!(
        matches!(EntryStore::new(refusing.clone()).publish(&first_root, prepare(MODULE)?).await,
        Err(Error::Objects(error)) if error.code == ErrorCode::Unavailable)
    );
    assert_eq!(
        refusing.requests().data(),
        1,
        "failed batch has no descriptor or retention follow-up"
    );
    Ok(())
}
