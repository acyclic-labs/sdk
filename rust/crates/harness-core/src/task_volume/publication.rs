//! One retry path for independent immutable task-file publishers: after
//! another writer advances the volume, or after the owner's coordinator
//! revision advances under an unchanged volume, both of which leave nothing
//! published. Every retry first re-derives the owner condition, which fails
//! once the owner has actually lost its execution fence.
use super::*;

#[doc(hidden)]
pub async fn publish<P, F, Fut, E>(
    volume: &TaskVolume<P>,
    generation: Generation,
    write: &Write,
    authorize: F,
) -> std::result::Result<Generation, E>
where
    P: StreamProvider + ?Sized,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<(Vec<CommitCondition>, Option<u64>), E>>,
    E: From<Error>,
{
    retry(volume, generation, write, Mode::Append, None, authorize)
        .await
        .map(|(generation, _)| generation)
}

/// [`publish`] for owner files that carry their caller's native work.
#[cfg(feature = "filesystem")]
pub(crate) async fn publish_with_work<P: StreamProvider + ?Sized>(
    volume: &TaskVolume<P>,
    generation: Generation,
    write: &Write,
    work: NativeWork<'_>,
) -> Result<Generation> {
    retry(volume, generation, write, Mode::Append, work, || {
        std::future::ready(Ok::<_, Error>((Vec::new(), None)))
    })
    .await
    .map(|(generation, _)| generation)
}

/// Proof that one exact immutable file is in its provider's durable prefix.
/// It is built only from the provider's [`acyclic_stream::DurableReceipt`]: an acknowledgement,
/// including one made under `deferring_durability`, never yields it.
#[derive(Debug)]
pub struct DurableFile {
    key: FileKey,
    _receipt: acyclic_stream::DurableReceipt,
}

impl DurableFile {
    /// The exact file certified by this proof, for binding it at a consumer.
    pub fn key(&self) -> &FileKey {
        &self.key
    }
}

/// Publishes `write` and returns once the provider's durable receipt covers
/// this exact file. If the file is absent or only accepted (an identical copy
/// whose writer may still await its durability barrier, or one written under
/// `deferring_durability`), this publisher commits its own copy, which on a
/// flushing provider advances the durable prefix over every earlier record,
/// and asks for the receipt again. Without one it reports
/// [`Error::AwaitingDurability`] instead of a proof.
pub async fn publish_durable<P: StreamProvider + ?Sized>(
    volume: &TaskVolume<P>,
    write: &Write,
    work: NativeWork<'_>,
) -> Result<DurableFile> {
    let receipt = if let Some(receipt) = volume.file_receipt(write).await? {
        receipt
    } else {
        commit_acknowledged(volume, volume.head().await?, write, work).await?;
        volume
            .file_receipt(write)
            .await?
            .ok_or(Error::AwaitingDurability)?
    };
    Ok(DurableFile {
        key: write.key.clone(),
        _receipt: receipt,
    })
}

/// Commits this publisher's own copy of `write`. `Acknowledged` never returns
/// early on an identical accepted copy, whose writer may still be waiting for
/// the durability barrier.
async fn commit_acknowledged<P: StreamProvider + ?Sized>(
    volume: &TaskVolume<P>,
    generation: Generation,
    write: &Write,
    work: NativeWork<'_>,
) -> Result<()> {
    #[cfg(acyclic_harness_loom)]
    let mode = if model_checking::enabled(model_checking::Fault::TrustAcceptedCopy) {
        Mode::Append
    } else {
        Mode::Acknowledged
    };
    #[cfg(not(acyclic_harness_loom))]
    let mode = Mode::Acknowledged;
    retry(volume, generation, write, mode, work, || {
        std::future::ready(Ok::<_, Error>((Vec::new(), None)))
    })
    .await?;
    Ok(())
}

/// The one generation-retry loop. Returns the published generation and whether
/// this publisher's own commit was acknowledged (false for an identical copy).
async fn retry<P, F, Fut, E>(
    volume: &TaskVolume<P>,
    mut generation: Generation,
    write: &Write,
    mode: Mode,
    work: NativeWork<'_>,
    mut authorize: F,
) -> std::result::Result<(Generation, bool), E>
where
    P: StreamProvider + ?Sized,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<(Vec<CommitCondition>, Option<u64>), E>>,
    E: From<Error>,
{
    // The owner conditions a fence conflict failed; the same conditions
    // would fail again, so only a changed authorization is retried.
    let mut fenced: Option<Vec<CommitCondition>> = None;
    loop {
        let (conditions, deadline) = authorize().await?;
        if fenced.as_ref() == Some(&conditions) {
            return Err(Error::FenceConflict.into());
        }
        match volume
            .publish(generation, write, conditions.clone(), deadline, mode, work)
            .await
        {
            Err(Error::Conflict(current)) => {
                #[cfg(acyclic_harness_loom)]
                if model_checking::enabled(model_checking::Fault::StopAfterGenerationConflict) {
                    return Err(Error::Conflict(generation).into());
                }
                let current = super::refresh::advanced(current, &generation)?;
                volume
                    .validate_anchor(generation.position, &generation.commit)
                    .await?;
                generation = current;
            }
            // The owner condition names the shared coordinator's revision, so
            // any coordinator event, such as a sibling's, fails it; only the
            // next authorization knows whether this owner still holds its fence.
            Err(Error::FenceConflict) => {
                #[cfg(acyclic_harness_loom)]
                if model_checking::enabled(model_checking::Fault::StopAfterFenceConflict) {
                    return Err(Error::FenceConflict.into());
                }
                fenced = Some(conditions);
            }
            result => return result.map_err(Into::into),
        }
    }
}

#[cfg(test)]
#[cfg(acyclic_harness_loom)]
mod models;

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
mod tests;
