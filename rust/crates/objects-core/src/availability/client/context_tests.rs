//! The same original-stop controls run through both shipping transports.
use super::*;
use acyclic_auth::operation::Stopped;
use futures::{FutureExt as _, channel::oneshot};
use std::sync::{Arc, Mutex};

fn stop(
    reason: u8,
    expire: &mut Option<oneshot::Sender<()>>,
    cancel: &mut Option<oneshot::Sender<()>>,
) -> Result<Stopped, Error> {
    let fire = |sender: &mut Option<oneshot::Sender<()>>| {
        sender
            .take()
            .ok_or_else(broken)?
            .send(())
            .map_err(|()| broken())
    };
    Ok(match reason {
        0 => {
            fire(expire)?;
            Stopped::Deadline
        }
        1 => {
            fire(cancel)?;
            Stopped::Cancelled
        }
        _ => {
            drop(cancel.take());
            Stopped::Cancelled
        }
    })
}

/// A control the harness itself owns was spent or dropped out of order.
fn broken() -> Error {
    ErrorCode::Unspecified.into()
}

fn stopped<T>(settled: Option<Result<T, Error>>) -> Option<Option<Stopped>> {
    settled.and_then(Result::err).map(|error| error.stopped)
}

/// Runs the original-stop controls through one shipping transport.
pub async fn verify(
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
                    let admitted = original
                        .lock()
                        .ok()
                        .and_then(|mut slot| slot.take())
                        .and_then(|(entered, released)| entered.send(()).ok().map(|()| released));
                    async move {
                        let released = admitted.ok_or_else(|| {
                            acyclic_auth::credential::CredentialError::Provider(
                                "credentials called after the admission was spent".into(),
                            )
                        })?;
                        released.await.map_err(|error| {
                            acyclic_auth::credential::CredentialError::Provider(Box::new(error))
                        })?;
                        Ok("first".into())
                    }
                }),
            );
            if phase == 0 {
                let expected = stop(reason, &mut expire, &mut cancel)?;
                let result = client.availability(digest).now_or_never();
                assert_eq!(
                    stopped(result),
                    Some(Some(expected)),
                    "original stop must settle availability opening"
                );
                assert!(
                    admission.lock().is_ok_and(|slot| slot.is_some()),
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
                let expected = stop(reason, &mut expire, &mut cancel)?;
                assert_eq!(stopped(opening.now_or_never()), Some(Some(expected)));
            } else {
                release.send(()).map_err(|()| broken())?;
                let mut observation = opening.await?;
                assert!(observation.changed().now_or_never().is_none());
                source.restored(digest)?;
                observation.changed().await?;
                let expected = stop(reason, &mut expire, &mut cancel)?;
                assert_eq!(
                    stopped(observation.changed().now_or_never()),
                    Some(Some(expected))
                );
            }
            assert_eq!(
                stopped(client.availability(digest).now_or_never()),
                Some(Some(if reason == 0 {
                    Stopped::Deadline
                } else {
                    Stopped::Cancelled
                }))
            );
        }
    }
    Ok(())
}
