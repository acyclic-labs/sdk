use super::*;
use crate::{AppendRequest, IdempotencyKey, MemoryStream, ReadRequest, StreamPath, StreamProvider};
use futures::{StreamExt as _, TryStreamExt as _};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[tokio::test]
async fn required_ranges_refuse_missing_history_and_stop_at_the_original_cut()
-> Result<(), StreamError> {
    let provider = MemoryStream::default();
    let path = StreamPath::new("account/conversation")?;
    provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec!["zero".into(), "one".into(), "two".into(), "three".into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::new("original")?,
        })
        .await?;
    let read = |from, limit| ReadRequest {
        path: path.clone(),
        key: None,
        from,
        limit,
        at_least: Default::default(),
    };
    let observed = Arc::new(AtomicUsize::new(0));
    let inspected = observed.clone();
    let original = provider.read(read(1, 3)).await?.inspect(move |_| {
        inspected.fetch_add(1, Ordering::SeqCst);
    });
    let selected = ExactRange::new(original, 1..3)?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(
        selected
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        observed.load(Ordering::SeqCst),
        2,
        "the bound must not consume the next source item"
    );
    // The same real source under the former at-most collection accepts the
    // missing required record. An exact selection must refuse that history.
    let shortened = provider
        .read(read(1, 4))
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(shortened.len(), 3);
    let mut required = ExactRange::new(provider.read(read(1, 4)).await?, 1..5)?;
    for sequence in 1..4 {
        assert_eq!(
            required.try_next().await?.map(|record| record.sequence),
            Some(sequence)
        );
    }
    assert!(matches!(
        required.try_next().await,
        Err(StreamError::Unavailable)
    ));
    assert!(required.next().await.is_none(), "producer loss is terminal");
    // Dropping the first actual record cannot splice a later one into its cut.
    let mut gap = ExactRange::new(provider.read(read(0, 4)).await?.skip(1), 0..4)?;
    assert!(matches!(
        gap.try_next().await,
        Err(StreamError::Unavailable)
    ));
    assert!(gap.next().await.is_none());
    // The original channel producer may disappear before the first pull.
    // Its one closure event must become a terminal typed range failure.
    let (producer, source) = futures::channel::mpsc::channel(1);
    drop(producer);
    let mut withdrawn = ExactRange::new(source, 0..1)?;
    assert!(matches!(
        withdrawn.try_next().await,
        Err(StreamError::Unavailable)
    ));
    assert!(withdrawn.next().await.is_none());
    Ok(())
}
