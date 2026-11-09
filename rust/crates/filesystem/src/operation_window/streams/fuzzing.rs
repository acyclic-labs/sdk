//! Shared oracle for the original persisted-window writer and reader.
use super::*;

/// Replays a mutation program against records produced by the public store.
/// The first byte selects a valid phase, a known invalid state or an arbitrary
/// payload mutation. Its upper two bits omit optional parent fields. Remaining
/// bytes supply the arbitrary payload or byte edit.
pub fn check_persisted_window(input: &[u8]) {
    futures::executor::block_on(check(input));
}

async fn check(input: &[u8]) {
    let selector = input.first().copied().unwrap_or(0);
    let case = selector % 16;
    let workspace = WorkspaceId::from_bytes([161; 16]);
    let parent = GenerationId::new(crate::Digest::from_bytes([152; 32]));
    let pending =
        (selector & 0x40 == 0).then_some(GenerationId::new(crate::Digest::from_bytes([156; 32])));
    let subsequent =
        (selector & 0x80 == 0).then_some(GenerationId::new(crate::Digest::from_bytes([157; 32])));
    let lease_id = OperationLeaseId::from_bytes([153; 16]);
    let active = || OperationWindowPhase::Active {
        pinned_parent: parent,
        leases: BTreeMap::from([(
            lease_id,
            OperationLease {
                id: lease_id,
                owner: "tool λ".into(),
                expires_at_millis: u64::MAX,
            },
        )]),
        pending_parent: pending,
    };
    let reconcile = || OperationWindowPhase::Reconciling {
        ticket: OperationId::from_bytes([154; 16]),
        pinned_parent: parent,
        pending_parent: pending,
        subsequent_parent: subsequent,
    };
    let phase_case = if (12..=14).contains(&case) {
        (selector >> 4) % 3
    } else {
        case
    };
    let phase = match phase_case {
        1 | 6..=10 => active(),
        2 | 11 => reconcile(),
        _ => OperationWindowPhase::Idle,
    };
    let original = OperationWindowSnapshot {
        version: STATE_VERSION,
        revision: 1,
        workspace_id: workspace,
        phase,
    };
    let provider = Arc::new(acyclic_stream::MemoryStream::default());
    let store = StreamOperationWindowStore::new(provider.clone());
    assert!(
        store
            .compare_and_swap(workspace, 0, original.clone())
            .await
            .expect("original public writer must admit the valid state")
    );
    assert_eq!(
        store.load(workspace).await.unwrap(),
        Some(original.clone()),
        "persisted window must preserve original fields"
    );
    let path = stream_window_path(workspace).unwrap();
    let wire = provider
        .head(path.clone())
        .await
        .unwrap()
        .last
        .unwrap()
        .value;
    let mut changed = original.clone();
    match case {
        3 => changed.version = 0,
        4 => changed.workspace_id = WorkspaceId::from_bytes([162; 16]),
        5 => changed.revision += 1,
        6..=10 => {
            let OperationWindowPhase::Active { leases, .. } = &mut changed.phase else {
                unreachable!();
            };
            let mut lease = leases.remove(&lease_id).unwrap();
            let key = match case {
                7 => {
                    lease.id = OperationLeaseId::from_bytes([0; 16]);
                    lease.id
                }
                8 => OperationLeaseId::from_bytes([155; 16]),
                _ => lease_id,
            };
            if case == 9 {
                lease.owner = " \t\n".into();
            }
            if case == 10 {
                lease.expires_at_millis = 0;
            }
            if case != 6 {
                leases.insert(key, lease);
            }
        }
        11 => {
            let OperationWindowPhase::Reconciling { ticket, .. } = &mut changed.phase else {
                unreachable!();
            };
            *ticket = OperationId::from_bytes([0; 16]);
        }
        _ => {}
    }
    let payload = match case {
        0..=2 | 15 => wire.to_vec(),
        12 => input.get(1..).unwrap_or_default().to_vec(),
        13 => {
            let mut payload = wire.to_vec();
            let position = edit_position(input) % payload.len();
            payload[position] ^= input.get(3).copied().unwrap_or(0xff);
            payload
        }
        14 => wire[..edit_position(input) % wire.len()].to_vec(),
        _ => serde_json::to_vec(&changed).unwrap(),
    };
    // Reopen through a different public store owner. Its head is supplied by
    // the actual backend, not a mock head with invented sequence metadata.
    let reopened = Arc::new(acyclic_stream::MemoryStream::default());
    let path = if case == 15 {
        acyclic_stream::StreamPath::new(format!("{}/child", path.as_str())).unwrap()
    } else {
        path
    };
    reopened
        .append(acyclic_stream::AppendRequest {
            path,
            records: vec![payload.into()],
            if_tail: Some(0),
            idempotency_key: acyclic_stream::IdempotencyKey::new(b"window-input".to_vec()).unwrap(),
        })
        .await
        .unwrap();
    let store = StreamOperationWindowStore::new(reopened);
    let observed = store.load(workspace).await;
    match case {
        0..=2 => assert_eq!(observed.unwrap(), Some(original)),
        3..=11 | 15 => assert!(
            matches!(observed, Err(StreamOperationWindowStoreError::Corrupt(_))),
            "persisted window must retain its typed corruption refusal: {observed:?}"
        ),
        _ => match observed {
            Err(StreamOperationWindowStoreError::Corrupt(_)) => {}
            Ok(Some(snapshot)) => {
                // Every accepted decoded state must be admissible to the SAME
                // original writer and preserve all fields on a fresh reopen.
                let round_trip = StreamOperationWindowStore::new(Arc::new(
                    acyclic_stream::MemoryStream::default(),
                ));
                assert!(
                    round_trip
                        .compare_and_swap(workspace, 0, snapshot.clone())
                        .await
                        .unwrap()
                );
                assert_eq!(round_trip.load(workspace).await.unwrap(), Some(snapshot));
            }
            other => panic!("present window must decode or retain typed corruption: {other:?}"),
        },
    }
}

fn edit_position(input: &[u8]) -> usize {
    usize::from(u16::from_le_bytes([
        input.get(1).copied().unwrap_or(0),
        input.get(2).copied().unwrap_or(0),
    ]))
}
