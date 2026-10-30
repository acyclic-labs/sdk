//! On-demand checks of the production funded JSON and checksum-frame codecs.
use super::*;

fn check_type<T: DeserializeOwned + Serialize>(
    bytes: &[u8],
    work: &crate::NativeMaterializationWork,
) -> bool {
    // Keep the returned tuple intact: its value retires before allocation custody.
    let Ok(decoded) =
        measured_decode_reader::<T>(std::io::Cursor::new(bytes), bytes.len() as u64, work)
    else {
        return true;
    };
    let Ok(encoded) = serde_json::to_vec(&decoded.0) else {
        return false;
    };
    let Ok(repeated) =
        measured_decode_reader::<T>(std::io::Cursor::new(&encoded), encoded.len() as u64, work)
    else {
        return false;
    };
    serde_json::to_vec(&repeated.0).is_ok_and(|bytes| bytes == encoded)
}

/// Exercise every core-state payload through the same funded visitor as recovery.
pub fn check(bytes: &[u8]) -> bool {
    let cancellation = crate::CancellationToken::new();
    let Ok(work) =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
    else {
        return false;
    };
    macro_rules! check {
        ($($kind:ty),+ $(,)?) => { $(if !check_type::<$kind>(bytes, &work) { return false; })+ };
    }
    check!(
        RecordKey,
        RecordValue,
        Change,
        LogFrame<Change>,
        ContextChildren,
        ContextChildCount,
        ContextChildIndex,
        WorkspaceContext,
        WorkspaceLineageRecord,
        LazyWorkspaceState,
        GitCompatState,
        MaterializationJournal,
        MultiRootPublication,
        Option<WorkspaceContext>,
        Option<ContextChildren>,
        Option<ContextChildCount>,
        Option<ContextChildIndex>,
        Option<WorkspaceLineageRecord>,
        Option<LazyWorkspaceState>
    );
    let mut offset = 0;
    while offset < bytes.len() {
        let before = offset;
        let Ok(Some(frame)) = checked_log_frame(bytes, &mut offset, Some(&work)) else {
            break;
        };
        if offset <= before || !check_type::<LogFrame<Change>>(frame, &work) {
            return false;
        }
    }
    true
}

/// Capture real checkpoint writers and the same checksum writer used by commit.
pub fn seeds() -> Result<Vec<(String, Vec<u8>)>, LocalCoreStateStoreError> {
    let mut seeds = Vec::new();
    for (name, value) in [
        ("core-absent", None),
        (
            "core-children",
            Some(RecordValue::Children(ContextChildren::default())),
        ),
        (
            "core-child-count",
            Some(RecordValue::ChildCount(ContextChildCount(1))),
        ),
        (
            "core-child-index",
            Some(RecordValue::ChildIndex(ContextChildIndex(1))),
        ),
    ] {
        let mut encoded = Vec::new();
        RecordValue::write_checkpoint(value.as_ref(), &mut encoded, None)?;
        seeds.push((name.into(), encoded));
    }
    let change = Change::default();
    let frame = CoreLog::encode_frame(Uuid::from_bytes([1; 16]), 0, &change, None)?;
    seeds.push(("core-journal".into(), frame.bytes));
    Ok(seeds)
}
