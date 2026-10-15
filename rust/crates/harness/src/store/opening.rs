use super::*;
use crate::{
    wire::ErrorCode::{Invalid, Storage},
    wire_codec::CodecQuota,
};

impl<P: StreamProvider> StreamAggregate<P> {
    /// Restores an authenticated aggregate at a bounded committed cut.
    pub async fn open(
        client: &Arc<P>,
        binding: (Authority, AuthorityVerifier, SchemaRegistry),
        snapshot: Option<Snapshot>,
        limits: HistoryReadLimits,
        maximum_terminal_effects: usize,
        original_revision: Option<u64>,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<Self> {
        let (authority, authority_verifier, schemas) = binding;
        authority_verifier.verify_audience(&authority, &mut None)?;
        if limits.maximum_events == 0 || limits.maximum_bytes == 0 || maximum_terminal_effects == 0
        {
            return Err(Error::Invalid(
                "projection read bounds must be positive".into(),
            ));
        }
        let mut path_quota = CodecQuota::from_retained(work, 0);
        let (path, held) =
            operations::publication_path(&authority, "harness/v2/", None, path_quota.work())?;
        path_quota.adopt(held)?;
        let loaded = match snapshot {
            Some(snapshot) => {
                checkpoints::supplied(
                    client,
                    &authority,
                    snapshot,
                    limits,
                    original_revision,
                    path_quota.work(),
                )
                .await?
            }
            None => {
                checkpoints::load_with_work(
                    client,
                    &authority,
                    &authority_verifier,
                    limits,
                    original_revision,
                    path_quota.work(),
                )
                .await?
            }
        };
        let mut reducer = match loaded.snapshot {
            Some(snapshot) => {
                Reducer::restore(snapshot, authority_verifier, schemas, path_quota.work())?
            }
            None => Reducer::new(authority, authority_verifier, schemas),
        };
        reducer.set_resident_event_limit(1024)?;
        reducer.set_resident_terminal_effect_limit(maximum_terminal_effects)?;
        let start = reducer.revision();
        let through = loaded.through_revision;
        let count = u32::try_from(
            through
                .checked_sub(start)
                .filter(|count| *count <= u64::from(limits.maximum_events - loaded.consumed_events))
                .ok_or_else(|| {
                    Error::Invalid("projection suffix exceeds event allowance".into())
                })?,
        )
        .map_err(|_| Error::Invalid("projection suffix exceeds event allowance".into()))?;
        if count != 0 {
            replay_suffix(
                client,
                &path,
                &mut reducer,
                (
                    start,
                    through,
                    count,
                    limits.maximum_bytes.saturating_sub(loaded.consumed_bytes),
                ),
                path_quota.work(),
            )
            .await?;
        }
        if let Some(io) = path_quota.work().as_deref() {
            io.check_cancellation()?;
        }
        path_quota.transfer();
        Ok(Self {
            client: client.clone(),
            path,
            reducer,
            content_verifier: None,
            fork_verifier: None,
            merge_verifier: None,
            limits: Limits::default(),
        })
    }
}

async fn replay_suffix<P: StreamProvider>(
    client: &Arc<P>,
    path: &StreamPath,
    reducer: &mut Reducer,
    bounds: (u64, u64, u32, u64),
    work: &mut Option<&mut dyn history::HistoryIo>,
) -> Result<()> {
    let (start, through, count, maximum_bytes) = bounds;

    if maximum_bytes == 0 {
        return Err(guarded_boundary(
            work,
            Invalid,
            "projection suffix exceeds read allowance",
        ));
    }
    let mut records = operations::read_records(client, path, start, count, work)
        .await?
        .map_err(|error| operations::stream_error_with_work(error, work))?;
    let mut used = 0_u64;
    let mut output_bytes = 0_u64;
    loop {
        let mut quota = CodecQuota::from_retained(work, 0);
        if let Some(io) = quota.work().as_deref() {
            io.check_cancellation()?;
        }
        let Some(record) = records.next(quota.work()).await? else {
            break;
        };
        let raw = operations::sdk_record_retained_bytes(&record, quota.work());
        quota.adopt(raw)?;
        if reducer.revision() >= through || record.sequence != reducer.revision() {
            return Err(guarded_boundary(
                quota.work(),
                Storage,
                "history page is not bounded and contiguous",
            ));
        }
        let length = record.value.len() as u64;
        checkpoints::charge(&mut used, length, maximum_bytes, quota.work())?;
        // Canonical output excludes retirement-proof input bytes.
        output_bytes += length;
        let (event, held) = history::verify_history_record_with_retained_quota(
            reducer.event_verifier(),
            reducer.authority(),
            reducer.revision(),
            &record,
            quota.work(),
        )?;
        quota.adopt(held)?;
        if let Some(io) = quota.work().as_deref_mut() {
            io.admit_output(output_bytes, reducer.revision() - start + 1)?;
        }
        let (indexed, bytes) = effect_heads::Tree {
            client,
            authority: reducer.authority(),
        }
        .retirement_proof_with_work(&event, &record, maximum_bytes - used, quota.work())
        .await?;
        checkpoints::charge(&mut used, bytes, maximum_bytes, quota.work())?;
        let applied = reducer.apply_committed(event, quota.work())?;
        if indexed {
            let ApplyResult::Applied { event } = &applied else {
                return Err(guarded_boundary(
                    quota.work(),
                    Storage,
                    "projection suffix replayed its successor",
                ));
            };
            reducer.mark_indexed_terminal_effect_with_work(event, quota.work())?;
        }
        reducer.enforce_terminal_effect_limit()?;
        drop(applied);
        drop(record);
    }
    if reducer.revision() != through {
        return Err(guarded_boundary(
            work,
            Storage,
            "history page stopped before its pinned boundary",
        ));
    }
    if let Some(io) = work.as_deref_mut() {
        io.stage_output(output_bytes, u64::from(count))?;
    }
    Ok(())
}
