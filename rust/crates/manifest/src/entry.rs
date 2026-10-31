//! One retained immutable entry closure for Actors and Workers.
use crate::{EntryClosure, EntryObject, ManifestError, PreparedDeployment, PreparedEntry};
use acyclic_actors::domain::{
    ActorObservation, CodeSha256, DeploymentObservation, DeploymentRequest, EntrySource,
};
use acyclic_objects::{
    Blobs, ByteRange, Digest, Kind, Object, Released, Retained, RootId, Verified,
};
use std::collections::HashSet;

/// Original publication, admission or read failure; no retry changes its cause.
#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    /// The original account-scoped Objects operation failed.
    #[error(transparent)]
    Objects(#[from] acyclic_objects::Error),
    /// Selected configuration or loaded bytes failed canonical admission.
    #[error(transparent)]
    Manifest(#[from] ManifestError),
}

/// Exact descriptor publication and durable retention acknowledgements.
#[derive(Debug)]
pub struct PublishedEntry {
    /// Original Objects identity, length and publication revision.
    pub object: Verified,
    /// Original retention result, including its covering account revision.
    pub retention: Retained,
}

/// A canonical deployment request whose exact immutable closure is retained.
#[derive(Debug)]
pub struct PublishedDeployment {
    /// Submit unchanged through the original Actors client and deployment intent.
    pub request: DeploymentRequest,
    /// Original Objects publication and durability acknowledgements.
    pub publication: PublishedEntry,
}

/// Publication and loading share the caller's existing account provider.
pub struct EntryStore<P> {
    objects: P,
}

fn confirmed(put: acyclic_objects::Put) -> Result<Verified, acyclic_objects::Error> {
    match put {
        acyclic_objects::Put::Created { verified }
        | acyclic_objects::Put::AlreadyExists { verified } => Ok(verified),
        acyclic_objects::Put::Unknown => {
            Err(acyclic_objects::wire::ErrorCode::OutcomeUnknown.into())
        }
    }
}

fn exact(object: &Object, length: u64) -> Result<(), ManifestError> {
    if object.head().kind != Kind::Blob || object.head().length != length {
        return Err(ManifestError::invalid("entryClosure", "object-reference"));
    }
    Ok(())
}

impl<P: Blobs> EntryStore<P> {
    /// Use the original admitted provider; this creates no connection or authority.
    #[must_use]
    pub const fn new(objects: P) -> Self {
        Self { objects }
    }

    /// Publish the admitted payloads through the canonical credited batch, then
    /// its descriptor with outgoing references, and retain the one closure root.
    /// Retention supplies the durability barrier. There are no preflight reads,
    /// per-payload flushes or retries; an unknown acknowledgement remains typed.
    /// `root` is the caller's stable deployment intent, independent of content:
    /// deployments sharing bytes retain and release their own roots.
    pub async fn publish(
        &self,
        root: &RootId,
        prepared: PreparedEntry,
    ) -> Result<PublishedEntry, Error> {
        let (closure, payloads) = prepared.into_parts()?;
        self.publish_parts(root, closure, payloads).await
    }

    /// Lower and retain the same admitted selection before its deployment call.
    /// Invalid configuration or intent fails before any Objects operation.
    pub async fn publish_deployment(
        &self,
        root: &RootId,
        prepared: PreparedEntry,
        idempotency_key: &str,
    ) -> Result<PublishedDeployment, Error> {
        self.publish_prepared(root, prepared.into_deployment(idempotency_key)?)
            .await
    }

    /// Publish the original lowered intent without repeating entry admission.
    pub async fn publish_prepared(
        &self,
        root: &RootId,
        prepared: PreparedDeployment,
    ) -> Result<PublishedDeployment, Error> {
        let (request, closure, payloads) = prepared.into_parts();
        let publication = self.publish_parts(root, closure, payloads).await?;
        Ok(PublishedDeployment {
            request,
            publication,
        })
    }

    async fn publish_parts(
        &self,
        root: &RootId,
        closure: acyclic_stream::SealedBytes,
        payloads: Vec<acyclic_stream::SealedBytes>,
    ) -> Result<PublishedEntry, Error> {
        let references = payloads.iter().map(Digest::sealed).collect();
        let batch = payloads
            .into_iter()
            .map(|body| (body, Vec::new()))
            .collect();
        for result in self.objects.put_batch_sealed(batch).await {
            confirmed(result?)?;
        }
        let object = confirmed(self.objects.put_sealed(closure, references).await?)?;
        let retention = self.objects.retain(root, vec![object.digest]).await?;
        Ok(PublishedEntry { object, retention })
    }

    /// Read the one descriptor using its original admitted byte length. Account
    /// authority stays in this provider, and the selected names stay exact.
    pub async fn load(
        &self,
        reference: &Verified,
        environment: &str,
        entry: &str,
    ) -> Result<LoadedEntry<'_, P>, Error> {
        self.load_selected(reference.digest, reference.length, environment, entry)
            .await
    }

    /// Restore a deployment from its original accepted observation. Missing or
    /// mismatched selection fails closed, without falling back to a code SHA.
    pub async fn load_deployment(
        &self,
        observation: &DeploymentObservation,
    ) -> Result<LoadedEntry<'_, P>, Error> {
        let source = observation
            .entry_source()
            .filter(|source| source.entry() == observation.name().as_str())
            .ok_or_else(|| ManifestError::invalid("entryClosure", "accepted-source"))?;
        self.load_source(source, observation.code_sha256()).await
    }

    /// Restore an actor using the exact selection in its accepted namespace
    /// configuration, independent of its instance id or checkpoint history.
    pub async fn load_actor(
        &self,
        observation: &ActorObservation,
    ) -> Result<LoadedEntry<'_, P>, Error> {
        let source = observation
            .entry_source()
            .filter(|source| source.entry() == observation.namespace())
            .ok_or_else(|| ManifestError::invalid("entryClosure", "accepted-source"))?;
        self.load_source(source, observation.code_sha256()).await
    }

    async fn load_source(
        &self,
        source: &EntrySource,
        code: &CodeSha256,
    ) -> Result<LoadedEntry<'_, P>, Error> {
        let loaded = self
            .load_selected(
                source.digest(),
                source.length(),
                source.environment(),
                source.entry(),
            )
            .await?;
        loaded.closure.verify_code(code)?;
        Ok(loaded)
    }

    async fn load_selected(
        &self,
        digest: Digest,
        length: u64,
        environment: &str,
        entry: &str,
    ) -> Result<LoadedEntry<'_, P>, Error> {
        EntryClosure::admit_length(length)?;
        let descriptor = self.objects.get(digest, ByteRange::FULL, length).await?;
        exact(&descriptor, length)?;
        let closure = EntryClosure::decode(descriptor.body(), environment, entry)?;
        let edges: HashSet<_> = closure
            .objects()
            .map(|object| Digest::from_bytes(object.digest))
            .collect();
        if descriptor.head().references.len() != edges.len()
            || descriptor
                .head()
                .references
                .iter()
                .any(|digest| !edges.contains(digest))
        {
            return Err(ManifestError::invalid("entryClosure", "object-references").into());
        }
        Ok(LoadedEntry {
            objects: &self.objects,
            descriptor,
            closure,
        })
    }

    /// Release the selected closure after no deployment names it. Collection
    /// remains the existing Objects operation and never runs on the read path.
    pub async fn release(&self, root: &RootId) -> Result<Released, Error> {
        Ok(self.objects.release(root).await?)
    }
}

/// A loaded descriptor retains the original result and provider, with lazy,
/// indexed access to selected module bytes. It never needs a builder directory.
pub struct LoadedEntry<'a, P> {
    objects: &'a P,
    descriptor: Object,
    closure: EntryClosure,
}

impl<P: Blobs> LoadedEntry<'_, P> {
    /// Original descriptor read, including its exact served revision and custody.
    #[must_use]
    pub const fn descriptor(&self) -> &Object {
        &self.descriptor
    }

    /// Canonically admitted selected configuration and module identities.
    #[must_use]
    pub const fn closure(&self) -> &EntryClosure {
        &self.closure
    }

    async fn read(&self, reference: EntryObject) -> Result<Object, Error> {
        let object = self
            .objects
            .get(
                Digest::from_bytes(reference.digest),
                ByteRange::FULL,
                reference.length,
            )
            .await?;
        exact(&object, reference.length)?;
        Ok(object)
    }

    /// Read and verify only the selected entry's code, preserving the original
    /// Objects result and its served revision without another byte copy.
    pub async fn artifact(&self) -> Result<Object, Error> {
        let object = self.read(self.closure.artifact()).await?;
        self.closure.verify_artifact(object.body())?;
        Ok(object)
    }

    /// Read and verify one declared sibling; unknown names fail before I/O.
    pub async fn module(&self, path: &str) -> Result<Object, Error> {
        let (asset, reference) = self.closure.module(path)?;
        let object = self.read(reference).await?;
        asset.verify_bytes(object.body())?;
        Ok(object)
    }
}
