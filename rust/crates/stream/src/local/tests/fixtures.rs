use crate::{
    OwnedBytes,
    test_meter::{Meter, NativeDenial},
};
use acyclic_native_runtime::ReadAllocation;
#[derive(Debug)]
pub(super) enum NativeGuardFailure {
    Provider(StreamError),
    Io(std::io::Error),
    Owner(u64),
}

impl From<StreamError> for NativeGuardFailure {
    fn from(error: StreamError) -> Self {
        Self::Provider(error)
    }
}

impl std::fmt::Display for NativeGuardFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Provider(error) => std::fmt::Display::fmt(error, f),
            Self::Io(error) => std::fmt::Display::fmt(error, f),
            Self::Owner(identity) => write!(f, "original guard {identity}"),
        }
    }
}

impl std::error::Error for NativeGuardFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Provider(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::Owner(_) => None,
        }
    }
}

pub(super) struct GuardLeaseWitness(pub(super) Arc<AtomicU64>);

impl Drop for GuardLeaseWitness {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

pub(super) fn assert_original_denial(error: &std::io::Error, expected: &'static str) {
    let cause = error
        .get_ref()
        .and_then(|cause| cause.downcast_ref::<NativeDenial>());
    assert_eq!(cause.map(|cause| cause.0), Some(expected));
}

pub(super) fn keyed_append(path: &str, tail: u64) -> Result<AppendRequest, StreamError> {
    Ok(AppendRequest {
        path: StreamPath::new(path)?,
        records: vec![OwnedBytes::from_static(b"native-append-record")],
        if_tail: Some(tail),
        idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from(format!(
            "{path}@{tail}"
        )))?),
    })
}

pub(super) fn request_control_capacity(request: &CommitRequest) -> usize {
    request.conditions.capacity() * std::mem::size_of::<crate::CommitCondition>()
        + request.mutations.capacity() * std::mem::size_of::<crate::CommitMutation>()
        + request
            .mutations
            .iter()
            .map(|mutation| {
                let records = match mutation {
                    crate::CommitMutation::Append { records, .. }
                    | crate::CommitMutation::Fork { records, .. } => records,
                };
                records.capacity() * std::mem::size_of::<OwnedBytes>()
            })
            .sum::<usize>()
}

pub(super) async fn prepared_fixture(provider: &LocalStream) -> Result<CommitRequest, StreamError> {
    for path in ["prepared/owner", "prepared/target"] {
        provider
            .append(AppendRequest {
                path: StreamPath::new(path)?,
                records: vec![OwnedBytes::from_static(b"seed")],
                if_tail: Some(0),
                idempotency_key: None,
            })
            .await?;
    }
    Ok(CommitRequest {
        conditions: vec![
            crate::CommitCondition::Tail {
                path: StreamPath::new("prepared/owner")?,
                expected: 1,
            },
            crate::CommitCondition::Tail {
                path: StreamPath::new("prepared/target")?,
                expected: 1,
            },
        ],
        mutations: vec![crate::CommitMutation::Append {
            path: StreamPath::new("prepared/target")?,
            records: vec![OwnedBytes::new()],
        }],
        idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"original-prepared-key"))?,
    })
}
