use super::*;
use std::task::Poll;
use tracing::instrument::WithSubscriber as _;

async fn body(owner: &'static str) {
    tracing::info!(owner, "test capture");
    let mut yielded = false;
    futures::future::poll_fn(|cx| {
        if std::mem::replace(&mut yielded, true) {
            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
    let _span = tracing::info_span!("test.capture", owner);
}

fn owns_exactly(seen: &Seen, owner: &str, count: usize) -> bool {
    let seen = seen.lock().unwrap();
    let owners: Vec<_> = seen
        .iter()
        .filter(|(_, field, value)| *field == "owner" && !value.is_empty())
        .collect();
    owners.len() == count && owners.iter().all(|(_, _, value)| value == owner)
}

#[tokio::test]
async fn spawned_tasks_retain_their_test_subscriber() {
    async fn test(owner: &'static str, lose_dispatch: bool) -> bool {
        run(|seen| async move {
            let group = crate::live::TaskGroup::new(1, None).unwrap();
            let crate::Admission::Accepted(task) = group
                .spawn(crate::OperationId::new(), async move {
                    if lose_dispatch {
                        body(owner).with_subscriber(tracing::Dispatch::none()).await;
                    } else {
                        body(owner).await;
                    }
                })
                .await
            else {
                panic!("task was rejected")
            };
            assert!(matches!(task.result().await, crate::Outcome::Succeeded(())));
            owns_exactly(&seen, owner, 2)
        })
        .await
    }
    let (first, second) = futures::join!(test("first", false), test("second", false));
    assert!(first && second);
    assert!(
        !test("lost", true).await,
        "lost task dispatcher escaped the oracle"
    );
}
