use super::*;
use crate::NativeMaterializationWork;
use crate::kernel::allocation::{KernelAccounting, retained::AuthorityLifetime};
#[cfg(not(target_arch = "wasm32"))]
use acyclic_stream::NativeStreamProvider as _;
use acyclic_stream::{
    CommitCondition, CommitMutation, IdempotencyKey, MemoryStream, OwnedBytes, StreamPath,
};

fn request() -> Result<CommitRequest, acyclic_stream::StreamError> {
    let path = StreamPath::new("publication")?;
    Ok(CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![CommitCondition::Absent { path: path.clone() }],
        mutations: vec![CommitMutation::Append {
            path,
            records: vec![OwnedBytes::from_static(b"sealed").into()],
        }],
        idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"sealed"))?,
    })
}

fn committed(
    result: NativeStreamResult<CommitOutcome, AuthorityError<AuthorityStoreError>>,
) -> bool {
    result.is_ok_and(|result| {
        result.is_ok_and(|output| matches!(output.into_parts().0, CommitOutcome::Committed(_)))
    })
}

async fn commit(
    original: &NativeMaterializationWork,
    guarded: bool,
) -> NativeStreamResult<CommitOutcome, AuthorityError<AuthorityStoreError>> {
    let provider = MemoryStream::default();
    let mut work = AuthorityWork::native(original, original.cancellation());
    let request = request().map_err(std::io::Error::other)?;
    if guarded {
        NativeAuthorityGuard(|_: &NativeMaterializationWork| {
            std::future::ready(Ok::<_, AuthorityStoreError>(()))
        })
        .native_commit(&provider, request, None, &mut work)
        .await
    } else {
        ().native_commit(&provider, request, None, &mut work).await
    }
}

#[tokio::test]
async fn both_native_commit_guards_retain_the_sealed_cancellation_lifetime() {
    let mut observed = Vec::new();
    for guarded in [false, true] {
        for windowed in [false, true] {
            let cancellation = CancellationToken::new();
            let original =
                NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation)
                    .unwrap();
            let scoped = if windowed {
                original.invocation_window().unwrap()
            } else {
                original.clone()
            };
            let lifetime =
                AuthorityLifetime::admit(&mut KernelAccounting::native(&scoped, &cancellation))
                    .unwrap();
            let durable = scoped.authority_publication_view(&lifetime).unwrap();
            cancellation.cancel();
            let published = committed(commit(&durable, guarded).await);
            let caller_rejected = commit(&scoped, guarded).await.is_err();
            observed.push((guarded, windowed, published, caller_rejected));
        }
    }
    assert!(
        observed
            .iter()
            .all(|(_, _, published, rejected)| *published && *rejected),
        "provider lost sealed lifetime or bypassed caller cancellation: {observed:?}"
    );
}

#[tokio::test]
async fn both_native_commit_guards_enforce_the_existing_resource_ceiling() {
    let mut observed = Vec::new();
    for guarded in [false, true] {
        for windowed in [false, true] {
            let cancellation = CancellationToken::new();
            let original =
                NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation)
                    .unwrap();
            let scoped = original
                .native_volume_work(WorkBudget {
                    bytes_hashed: 0,
                    ..crate::performance::BOUNDARY_BUDGET
                })
                .unwrap();
            let scoped = if windowed {
                scoped.invocation_window().unwrap()
            } else {
                scoped
            };
            let rejected = commit(&scoped, guarded).await.is_err();
            let published = committed(commit(&original, guarded).await);
            observed.push((guarded, windowed, rejected, published));
        }
    }
    assert!(
        observed
            .iter()
            .all(|(_, _, rejected, published)| *rejected && *published),
        "provider bypassed resource ceiling or rejected a funded request: {observed:?}"
    );
}
