//! The original lost-ack recovery history, against the Stream primitive itself.
#![cfg(loom)]

use acyclic_stream::{AppendOutcome, AppendRequest, IdempotencyKey, MemoryStream, StreamPath};
use acyclic_stream::{StreamProvider, StreamProviderExt as _};
use futures::{Stream as _, StreamExt as _};
use loom::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Wake, Waker};

struct Pushed(AtomicBool);

impl Wake for Pushed {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn lost_ack_restart_and_append(replace_retry_key: bool) {
    acyclic_stream::verification::check(move || {
        use acyclic_stream::verification::block_on;

        let path = StreamPath::new("inbox")?;
        let request = AppendRequest {
            path: path.clone(),
            records: vec![b"original".to_vec().into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::new(b"original".to_vec())?,
        };
        let retained = MemoryStream::default();
        // The publication commits before its caller loses the acknowledgement.
        let original = block_on(retained.append(request.clone()))?;
        let restarted = MemoryStream::default();
        block_on(acyclic_stream::verification::recover(&restarted, &retained))?;
        let first = block_on(restarted.append(request.clone()))?;
        let (AppendOutcome::Committed(before), AppendOutcome::Committed(after)) =
            (&original, &first)
        else {
            return Err("original publication was not committed".into());
        };
        assert_eq!(
            (before.start, before.end, before.tail, before.commit_id),
            (after.start, after.end, after.tail, after.commit_id),
            "recovery changed immutable receipt"
        );
        assert_ne!(
            (&before.revision.scope, before.revision.incarnation),
            (&after.revision.scope, after.revision.incarnation),
            "replacement provider reused the old revision authority"
        );
        assert_eq!(
            block_on(retained.read_commit(before.commit_id))?,
            block_on(restarted.read_commit(after.commit_id))?,
            "recovery changed the immutable envelope"
        );
        drop(retained);
        let mut follow = acyclic_stream::records_only(block_on(
            restarted.follow(acyclic_stream::FollowRequest::new(path.clone(), 1)),
        )?);
        let pushed = Arc::new(Pushed(AtomicBool::new(false)));
        let waker = Waker::from(pushed.clone());
        assert!(
            std::pin::Pin::new(&mut follow)
                .poll_next(&mut Context::from_waker(&waker))
                .is_pending()
        );

        // Two original operations use the production publication/state locks.
        let retry = loom::thread::spawn({
            let provider = restarted.clone();
            move || {
                let mut request = request;
                if replace_retry_key {
                    request.idempotency_key = IdempotencyKey::unique();
                }
                block_on(provider.append(request))
            }
        });
        block_on(restarted.append(AppendRequest {
            path: path.clone(),
            records: vec![b"concurrent".to_vec().into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        }))?;
        let repeated = retry.join().map_err(|_| "retry panicked")??;
        assert_eq!(repeated, first, "retry changed committed receipt");
        assert!(pushed.0.load(Ordering::SeqCst), "publication lost its wake");
        let delivered = block_on(follow.next()).ok_or("follow ended")??;
        assert_eq!(delivered.sequence, 1);
        assert_eq!(delivered.value.as_slice(), b"concurrent");
        assert_eq!(block_on(restarted.tail(path.clone()))?, 2);
        let mut history = acyclic_stream::records_only(block_on(
            restarted.follow(acyclic_stream::FollowRequest::new(path, 0)),
        )?);
        let original = block_on(history.next()).ok_or("restored record missing")??;
        assert_eq!(original.sequence, 0);
        assert_eq!(original.value.as_slice(), b"original");
        Ok(())
    });
}

#[test]
fn lost_ack_recovery_and_retry_preserve_receipts_and_follow_wakes() {
    lost_ack_restart_and_append(false);
}

#[test]
#[should_panic(expected = "retry changed committed receipt")]
fn replacing_the_retry_key_duplicates_the_recovered_publication() {
    lost_ack_restart_and_append(true);
}
