use super::*;
use crate::distributed::{WorkPull, Worker};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn b11_pull_response_loss_keeps_original_reservation() -> Result<()> {
    for mode in 0..3 {
        let stream = Arc::new(crate::test_stream::LostSessionAck::default());
        let f = Fixture::new(
            stream.clone(),
            acyclic_fs::Fs::memory(),
            json!({"type":"integer"}),
            None,
            &[],
        )
        .await?;
        f.host.admit(f.admission(1, json!(0))).await?;
        if mode == 1 {
            stream.lose_ack.store(true, Ordering::SeqCst);
            stream.hide_receipt.store(true, Ordering::SeqCst);
        }
        if mode == 2 {
            stream.location_fault.store(4, Ordering::SeqCst);
        }
        let response = f
            .host
            .pull_operation(
                &Worker {
                    id: "original".into(),
                    available: Default::default(),
                    labels: BTreeMap::new(),
                },
                OperationId::from_bytes([1; 16]),
            )
            .await?;
        let expected = match response {
            WorkPull::Claimed(lease) if mode == 0 => lease,
            WorkPull::Unresolved { lease, .. } if mode != 0 => lease,
            _ => return Err(Error::Conflict("pull fault did not run".into())),
        };
        let reopened = f.reopen(&f.issuer, "owner").await?;
        stream.forbid_writes.store(true, Ordering::SeqCst);
        let recovered = reopened.recover_work(TaskId::from_bytes([1; 16])).await?;
        assert_eq!(stream.observation_writes.load(Ordering::SeqCst), 0);
        if mode == 2 {
            assert!(matches!(recovered, WorkPull::Idle));
            continue;
        }
        let WorkPull::Claimed(lease) = recovered else {
            return Err(Error::NotFound("original reservation".into()));
        };
        assert_eq!(lease, expected);
        stream.forbid_writes.store(false, Ordering::SeqCst);
        reopened.start_task(&lease).await?;
        reopened.cancel(TaskId::from_bytes([1; 16])).await?;
        stream.forbid_writes.store(true, Ordering::SeqCst);
        let WorkPull::Unresolved {
            lease: cancelled, ..
        } = reopened.recover_work(TaskId::from_bytes([1; 16])).await?
        else {
            return Err(Error::Conflict("cancelled lease became runnable".into()));
        };
        assert_eq!(cancelled.reservation, lease.reservation);
        assert!(reopened.start_task(&lease).await.is_err());
    }
    Ok(())
}
