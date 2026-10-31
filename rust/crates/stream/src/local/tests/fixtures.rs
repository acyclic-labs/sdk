use crate::{
    OwnedBytes,
    test_meter::{Meter, NativeDenial},
};
use acyclic_native_runtime::ReadAllocation;

/// The actual worker start or its caller's terminal, without a receiver task.
async fn journal_started<T>(
    started: tokio::sync::oneshot::Receiver<()>,
    producer: &mut tokio::task::JoinHandle<T>,
) -> std::io::Result<()> {
    tokio::select! {
        result = started => result.map_err(|error| std::io::Error::new(std::io::ErrorKind::BrokenPipe, error)),
        result = producer => {
            result.map_err(std::io::Error::other)?;
            Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "journal operation ended before its held worker cut",
            ))
        }
    }
}

/// Drives actual owned-task/native boundaries, including detached retirement.
async fn journal_lifecycle<F: Future>(
    schedule: &acyclic_native_runtime::testing::Schedule,
    boundaries: &mut tokio::sync::mpsc::UnboundedReceiver<
        acyclic_native_runtime::testing::Boundary,
    >,
    operation: F,
) -> std::io::Result<F::Output> {
    let operation = schedule.scope(operation);
    tokio::pin!(operation);
    let output = loop {
        tokio::select! {
            output = &mut operation => break output,
            boundary = boundaries.recv() => boundary
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "journal boundary producer ended"))?
                .release(),
        }
    };
    let drained = schedule.drained();
    tokio::pin!(drained);
    loop {
        tokio::select! {
            () = &mut drained => return Ok(output),
            boundary = boundaries.recv() => boundary
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "journal retirement producer ended"))?
                .release(),
        }
    }
}

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
        records: vec![(OwnedBytes::from_static(b"native-append-record")).into()],
        if_tail: Some(tail),
        idempotency_key: IdempotencyKey::new(OwnedBytes::from(format!("{path}@{tail}")))?,
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

pub(super) async fn prepared_fixture<P: StreamProvider + ?Sized>(
    provider: &P,
) -> Result<CommitRequest, StreamError> {
    for path in ["prepared/owner", "prepared/target"] {
        provider
            .append(AppendRequest {
                path: StreamPath::new(path)?,
                records: vec![(OwnedBytes::from_static(b"seed")).into()],
                if_tail: Some(0),
                idempotency_key: crate::IdempotencyKey::unique(),
            })
            .await?;
    }
    Ok(CommitRequest {
        deadline_unix_millis: None,
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
            records: vec![(OwnedBytes::new()).into()],
        }],
        idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"original-prepared-key"))?,
    })
}
