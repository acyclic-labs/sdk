use super::*;
use serde::{Deserialize, Serialize};
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LaunchReceipt {
    pub(super) dispatch: EffectDispatch,
    pub(super) plan: TaskEffectPlan,
    pub(super) conversation: crate::core::Authority,
    pub(super) task: crate::TaskId,
    pub(super) command: OperationId,
    pub(super) fence: crate::scheduler::LeaseFence,
    pub(super) approval: InteractionId,
    pub(super) approval_digest: [u8; 32],
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Receipt<L = LaunchReceipt, O = NativeObservation> {
    Launch(L),
    Observed(O),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeObservation {
    pub(super) effect: EffectObservation,
    pub(super) diagnostic: Option<crate::conversation::FileRef>,
    #[serde(default)]
    pub(super) published: Vec<NativeProcessPublishedVolume>,
}

impl super::super::PreparationRecord for Receipt {}

pub(super) async fn read_receipt<P: StreamProvider>(
    owner: &TaskJournalOwner<P>,
    path: &StreamPath,
    work: &NativeMaterializationWork,
) -> Result<Option<((LaunchReceipt, Option<NativeObservation>), NativeAllocation)>> {
    let io = PreparationIo::from_native_work(work)?;
    let mut history = io.history_io();
    let mut observer = Some(&mut history as &mut dyn crate::store::history::HistoryIo);
    owner.verify(true, &mut observer).await?;
    let client = owner.stream();
    let bounds =
        match crate::store::operations::bounds_with_work(&client, path, &mut observer).await? {
            Ok(bounds) => bounds,
            Err(error)
                if matches!(
                    &error,
                    StreamError::NotFound
                ) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(crate::store::operations::stream_error_with_work(error, &observer)),
        };
    if !(1..=2).contains(&bounds.tail) {
        return Err(Error::Storage("invalid native receipt length".into()));
    }
    let limit = bounds.tail as u32;
    let stream = crate::store::operations::stream(&client, path.as_str(), &mut observer)?;
    let mut records =
        crate::store::operations::read_records(&client, stream.0.path(), 0, limit, &mut observer)
            .await?
            .map_err(|error| crate::store::operations::stream_error_with_work(error, &observer))?;
    let first = records
        .next(&mut observer)
        .await?
        .ok_or_else(|| Error::Storage("missing native launch receipt".into()))?;
    let Receipt::Launch(launch) = retain_decoded(io.decode(
        std::marker::PhantomData::<Receipt>,
        &first.value,
        crate::wire::ErrorCode::Storage,
    )?) else {
        return Err(Error::Storage("native launch receipt required".into()));
    };
    if first.sequence != 0 {
        return Err(Error::Storage("native receipt starts after launch".into()));
    }
    let observed = match records.next(&mut observer).await? {
        Some(record) => {
            let Receipt::Observed(observed) = retain_decoded(io.decode(
                std::marker::PhantomData::<Receipt>,
                &record.value,
                crate::wire::ErrorCode::Storage,
            )?) else {
                return Err(Error::Storage("native observed receipt required".into()));
            };
            if record.sequence != 1
                || observed.effect.provider != launch.dispatch.provider
                || observed.effect.effect_id != launch.dispatch.effect_id
                || observed.effect.attempt_id != launch.dispatch.attempt_id
                || observed.effect.request_digest != launch.dispatch.request_digest
                || observed.effect.guarantee != launch.dispatch.guarantee
                || (observed.diagnostic.is_some()
                    && observed.effect.status != EffectStatus::Indeterminate)
                || matches!(
                    observed.effect.status,
                    EffectStatus::Planned | EffectStatus::Dispatched
                )
            {
                return Err(Error::Storage(
                    "native observation identity is invalid".into(),
                ));
            }
            Some(observed)
        }
        None => None,
    };
    if bounds.tail != 1 + u64::from(observed.is_some())
        || records.next(&mut observer).await?.is_some()
    {
        return Err(Error::Storage("native receipt sequence is invalid".into()));
    }
    owner.verify(true, &mut observer).await?;
    drop(observer);
    drop(history);
    let retained = io.owned_bytes()?;
    Ok(Some((
        (launch, observed),
        io.into_native_custody(retained)?,
    )))
}

pub(super) async fn append_receipt<P: StreamProvider, L: Serialize, O: Serialize>(
    owner: &TaskJournalOwner<P>,
    path: StreamPath,
    tail: u64,
    receipt: &Receipt<L, O>,
    write: JournalWrite,
    work: &NativeMaterializationWork,
) -> Result<bool> {
    let io = PreparationIo::from_native_work(work)?;
    let bytes = io.encode(receipt, acyclic_stream::MAX_RECORD_BYTES as u64, true)?;
    let key = super::super::native_execution::journal::key(&bytes, work)?;
    owner
        .append(
            path,
            tail,
            &key,
            bytes,
            write,
            &mut Some(&mut io.history_io()),
        )
        .await
}

impl super::super::PreparationRecord for EffectDispatch {}

pub(super) fn copy<T: Serialize + super::super::PreparationRecord>(
    value: &T,
    io: &PreparationIo,
) -> Result<T> {
    let bytes = io.encode(value, acyclic_stream::MAX_RECORD_BYTES as u64, true)?;
    Ok(retain_decoded(io.decode(
        std::marker::PhantomData::<T>,
        &bytes,
        crate::wire::ErrorCode::Storage,
    )?))
}
