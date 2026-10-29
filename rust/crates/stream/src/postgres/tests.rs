use super::*;
use crate::replicated::{CommandJournal as _, ReplayObserver};
use futures::FutureExt as _;

async fn append(provider: &PostgresStream, path: &str) -> Result<(), StreamError> {
    provider
        .append(AppendRequest {
            path: StreamPath::new(path)?,
            records: vec!["value".into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    Ok(())
}

struct AppendDuringReplay<'a> {
    writer: &'a PostgresStream,
    appended: bool,
}

impl ReplayObserver for AppendDuringReplay<'_> {
    async fn observe<'a>(
        &'a mut self,
        _: &'a MemoryStream,
        _: &'a WriteRequest,
        after: bool,
    ) -> Result<(), StreamError> {
        if after && !self.appended {
            self.appended = true;
            append(self.writer, "paged/extra").await?;
        }
        Ok(())
    }
}

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL database through STORAGE_POSTGRES_URL"]
async fn paged_refresh_retains_observed_head_after_consuming_its_wake()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let database = std::env::var("STORAGE_POSTGRES_URL")?;
    let namespace = format!("paged-refresh-{}", uuid::Uuid::new_v4());
    let follower = PostgresStream::connect(&database, &namespace).await?;
    let writer = PostgresStream::connect(&database, &namespace).await?;
    // Hold the real follower's replica lock while another connection publishes
    // enough commands to require a second page. Its LISTEN driver remains live.
    let mut session = follower.inner.session.lock().await;
    for _ in 0..=PAGE {
        append(&writer, "paged/history").await?;
    }
    let target = writer.inner.session.lock().await.cursor;
    let Session {
        client,
        cursor,
        observed_head,
        publish,
        read_fenced,
        wakes,
    } = &mut *session;
    let mut replica = Replicated::new(
        PgJournal {
            storage: Storage::Connection(client),
            publish,
            read_fenced,
            namespace: &namespace,
            wakes,
            at_millis: 0,
            head: observed_head,
        },
        follower.inner.core.clone(),
        Arc::clone(&follower.inner.clock),
        *cursor,
    );
    replica
        .refresh_with(
            PAGE,
            &mut AppendDuringReplay {
                writer: &writer,
                appended: false,
            },
        )
        .await?;
    // The refresh keeps its first snapshot's cut, while its second real SQL
    // page sees the append made during replay of the first page.
    assert_eq!(replica.cursor, target);
    *cursor = replica.cursor;
    drop(replica);
    assert!(*observed_head > *cursor);
    // Expose exactly the consumed-notification case, without another write,
    // query, readiness loop or background task repairing the pending work.
    wakes.borrow_and_update();
    let mut forgotten = *cursor;
    let mut journal = PgJournal {
        storage: Storage::Connection(client),
        publish,
        read_fenced,
        namespace: &namespace,
        wakes,
        at_millis: 0,
        head: &mut forgotten,
    };
    // Negative control: reconstructing from only the applied cursor parks.
    assert!(journal.changed(*cursor).now_or_never().is_none());
    journal.head = observed_head;
    assert_eq!(journal.changed(*cursor).now_or_never(), Some(Ok(())));
    let mut replica = Replicated::new(
        journal,
        follower.inner.core.clone(),
        Arc::clone(&follower.inner.clock),
        *cursor,
    );
    replica.refresh(PAGE).await?;
    *cursor = replica.cursor;
    assert_eq!(
        replica
            .provider
            .tail(StreamPath::new("paged/extra")?)
            .await?,
        1
    );
    Ok(())
}
