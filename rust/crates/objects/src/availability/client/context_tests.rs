//! The same original-stop controls run through both shipping transports.
use super::*;
use acyclic_auth::operation::Stopped;
use futures::{FutureExt as _, channel::oneshot};
use std::sync::{Arc, Mutex};

fn stop(
    reason: u8,
    expire: &mut Option<oneshot::Sender<()>>,
    cancel: &mut Option<oneshot::Sender<()>>,
) -> Stopped {
    match reason {
        0 => {
            expire.take().unwrap().send(()).unwrap();
            Stopped::Deadline
        }
        1 => {
            cancel.take().unwrap().send(()).unwrap();
            Stopped::Cancelled
        }
        _ => {
            drop(cancel.take());
            Stopped::Cancelled
        }
    }
}

pub(crate) async fn verify(
    make: impl Fn(Context, CredentialProvider) -> Box<dyn crate::Blobs>,
    source: AvailabilitySource,
) -> Result<(), Error> {
    let digest = Digest::of(b"original availability context");
    for phase in 0..3 {
        for reason in 0..3 {
            let (expire, expired) = oneshot::channel();
            let (cancel, cancelled) = oneshot::channel();
            let (mut expire, mut cancel) = (Some(expire), Some(cancel));
            let context = Context::new(
                u64::MAX,
                async {
                    let _ = expired.await;
                },
                async {
                    let _ = cancelled.await;
                },
            );
            let (entered, started) = oneshot::channel();
            let (release, released) = oneshot::channel();
            let admission = Arc::new(Mutex::new(Some((entered, released))));
            let original = admission.clone();
            let client = make(
                context.clone(),
                CredentialProvider::new(move || {
                    let (entered, released) = original.lock().unwrap().take().unwrap();
                    entered.send(()).unwrap();
                    async move {
                        released.await.map_err(|error| {
                            acyclic_auth::credential::CredentialError::Provider(Box::new(error))
                        })?;
                        Ok("first".into())
                    }
                }),
            );
            if phase == 0 {
                let expected = stop(reason, &mut expire, &mut cancel);
                let result = client.availability(digest).now_or_never();
                assert!(
                    result.is_some(),
                    "original stop must settle availability opening"
                );
                assert_eq!(result.unwrap().err().unwrap().stopped, Some(expected));
                assert!(
                    admission.lock().unwrap().is_some(),
                    "stopped opening called credentials"
                );
                continue;
            }
            let mut opening = Box::pin(client.availability(digest));
            futures::select! {
                _ = started.fuse() => {},
                result = opening.as_mut().fuse() => panic!("credential still held: {}", result.is_ok()),
            }
            // A supplied budget cannot inherit or restart the default timeout.
            tokio::time::advance(std::time::Duration::from_secs(31)).await;
            assert!(opening.as_mut().now_or_never().is_none());
            if phase == 1 {
                let expected = stop(reason, &mut expire, &mut cancel);
                assert_eq!(
                    opening.now_or_never().unwrap().err().unwrap().stopped,
                    Some(expected)
                );
            } else {
                release.send(()).unwrap();
                let mut observation = opening.await?;
                assert!(observation.changed().now_or_never().is_none());
                source.restored(digest)?;
                observation.changed().await?;
                let expected = stop(reason, &mut expire, &mut cancel);
                assert_eq!(
                    observation
                        .changed()
                        .now_or_never()
                        .unwrap()
                        .unwrap_err()
                        .stopped,
                    Some(expected)
                );
            }
            assert_eq!(
                client
                    .availability(digest)
                    .now_or_never()
                    .unwrap()
                    .err()
                    .unwrap()
                    .stopped,
                Some(if reason == 0 {
                    Stopped::Deadline
                } else {
                    Stopped::Cancelled
                })
            );
        }
    }
    Ok(())
}
