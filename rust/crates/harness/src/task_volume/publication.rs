//! One generation-retry path for independent immutable task-file publishers.
use super::*;

pub(crate) async fn publish<P, F, Fut, E>(
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
/// It is built only from the provider's [`DurableReceipt`]: an acknowledgement,
/// including one made under `deferring_durability`, never yields it.
#[derive(Debug)]
pub(crate) struct DurableFile {
    key: FileKey,
    _receipt: acyclic_stream::DurableReceipt,
}

impl DurableFile {
    /// The confirmed file.
    pub(crate) fn key(&self) -> &FileKey {
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
pub(crate) async fn publish_durable<P: StreamProvider + ?Sized>(
    volume: &TaskVolume<P>,
    write: &Write,
    work: NativeWork<'_>,
) -> Result<DurableFile> {
    let receipt = match volume.file_receipt(write).await? {
        Some(receipt) => receipt,
        None => {
            commit_acknowledged(volume, volume.head().await?, write, work).await?;
            volume
                .file_receipt(write)
                .await?
                .ok_or(Error::AwaitingDurability)?
        }
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
    loop {
        let (conditions, deadline) = authorize().await?;
        match volume
            .publish(generation, write, conditions, deadline, mode, work)
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
            result => return result.map_err(Into::into),
        }
    }
}

#[cfg(all(test, acyclic_harness_loom))]
mod models;

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
