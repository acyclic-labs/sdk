use super::*;

#[tokio::test]
async fn replay_seeks_to_a_revision_after_interleaved_non_workflow_publications() {
    let (_, _, _, _, host) = fixture().await;
    let owner = placement();
    host.place(owner.clone(), json!(0)).await.unwrap();
    let journal = host.journal(&host.package().unwrap(), &owner).unwrap();
    for id in 2..152 {
        host.deliver(owner.clone(), event(id, json!(1)))
            .await
            .unwrap();
        if id == 60 {
            let current = journal.load().await.unwrap();
            let mut metadata = current.last;
            metadata.workflow = None;
            metadata.event = None;
            journal
                .publish(&current.head, current.tail, metadata, None)
                .await
                .unwrap();
        }
    }
    let future = journal.replay(130, 3, None).unwrap();
    let (records, _) = match future {
        crate::store::HistoryContentFuture::Ordinary { future, .. } => future.await.unwrap(),
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        crate::store::HistoryContentFuture::Native(_) => panic!("ordinary callback expected"),
    };
    assert_eq!(records.len(), 3);
    for (record, expected) in records.iter().zip(130..133) {
        assert_eq!(record.prior.revision, expected);
        assert_eq!(record.prior.state, json!(expected));
    }
    let recovered = host.recover(owner).await.unwrap();
    assert_eq!(
        (recovered.checkpoint.revision, recovered.consumed),
        (150, 150)
    );
    assert_eq!(recovered.checkpoint.state, json!(150));
}
