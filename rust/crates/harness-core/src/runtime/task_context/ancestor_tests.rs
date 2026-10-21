use super::*;
use proptest::prelude::*;

fn task(index: usize) -> TaskId {
    TaskId::from_bytes((index as u128).to_le_bytes())
}

fn index(task: TaskId) -> usize {
    u128::from_le_bytes(task.into_bytes()) as usize
}

async fn conversation(
    depth: usize,
    bound: usize,
    cap: Option<usize>,
) -> Result<(TaskId, Option<TaskId>, usize, usize)> {
    let parents = std::cell::Cell::new(0);
    let mut lookups = 0;
    let ancestors = ancestor_stream(task(depth), |current| {
        parents.set(parents.get() + 1);
        std::future::ready(if cap.is_some_and(|limit| parents.get() >= limit) {
            Err(Error::Invalid(
                "old message limit capped the conversation".into(),
            ))
        } else {
            Ok(index(current).checked_sub(1).map(task))
        })
    });
    futures::pin_mut!(ancestors);
    while let Some(current) = futures::TryStreamExt::try_next(&mut ancestors).await? {
        lookups += 1;
        if index(current) == bound {
            return Ok((current, Some(current), parents.get(), lookups));
        }
    }
    panic!("workspace binding is absent")
}

#[tokio::test]
async fn ten_thousand_turns_resolve_their_workspace_and_detect_the_old_cap() -> Result<()> {
    let (ancestor, binding, parents, lookups) = conversation(10_000, 0, None).await?;
    assert_eq!((ancestor, binding), (task(0), Some(task(0))));
    assert_eq!((parents, lookups), (10_000, 10_001));
    let broken = conversation(10_000, 0, Some(64)).await;
    assert!(
        matches!(broken, Err(Error::Invalid(message)) if message.contains("old message limit")),
        "the negative control must reject the old lifetime cap"
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn workspace_lookup_work_is_exactly_the_distance_to_the_nearest_binding(
        depth in 0usize..10_001, seed in any::<usize>(),
    ) {
        futures::executor::block_on(async {
            let bound = seed % (depth + 1);
            let (ancestor, binding, parents, lookups) = conversation(depth, bound, None).await.unwrap();
            prop_assert_eq!((ancestor, binding), (task(bound), Some(task(bound))));
            prop_assert_eq!(parents, depth - bound);
            prop_assert_eq!(lookups, depth - bound + 1);
            Ok(())
        })?;
    }

    #[test]
    fn ancestor_cycles_fail_closed(length in 1usize..33) {
        futures::executor::block_on(async {
            let ancestors = ancestor_stream(task(0), |current| {
                std::future::ready(Ok(Some(task((index(current) + 1) % length))))
            });
            let result = futures::TryStreamExt::try_collect::<Vec<_>>(ancestors).await;
            prop_assert!(matches!(result, Err(Error::Invalid(_))));
            Ok(())
        })?;
    }
}
