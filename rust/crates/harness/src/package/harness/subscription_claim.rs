use crate::{Error, Result};
use acyclic_stream::{OperationClaim, OperationClaims};
use futures::{channel::oneshot, future::Either};

/// One provider-owned supervisor per command session. Reconnecting observers
/// wait for its admitted work to drain; a second execution owner cannot enter.
pub(super) async fn acquire<'a>(
    claims: &'a OperationClaims,
    key: [u8; 48],
    detached: &mut oneshot::Receiver<()>,
) -> Result<Option<OperationClaim<'a, ()>>> {
    loop {
        if let Some(claim) = claims.claim(key, || Ok::<_, Error>(()), |_| Ok(()))? {
            return Ok(Some(claim));
        }
        match futures::future::select(&mut *detached, Box::pin(claims.wait_until_released(key)))
            .await
        {
            Either::Left(_) => return Ok(None),
            Either::Right(((), _)) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    #[test]
    fn reconnect_cannot_enter_until_the_original_subscription_releases() {
        futures::executor::block_on(async {
            let claims = OperationClaims::default();
            let key = [7; 48];
            let (_first, mut first_attached) = oneshot::channel();
            let first = acquire(&claims, key, &mut first_attached)
                .await
                .unwrap()
                .unwrap();
            let (_second, mut second_attached) = oneshot::channel();
            let mut second = Box::pin(acquire(&claims, key, &mut second_attached));
            assert!(second.as_mut().now_or_never().is_none());
            drop(first);
            assert!(second.await.unwrap().is_some());
        });
    }

    #[test]
    fn a_detached_reconnect_releases_its_wait_without_entering() {
        futures::executor::block_on(async {
            let claims = OperationClaims::default();
            let key = [7; 48];
            let (_first, mut first_attached) = oneshot::channel();
            let _first = acquire(&claims, key, &mut first_attached)
                .await
                .unwrap()
                .unwrap();
            let (second, mut second_attached) = oneshot::channel();
            drop(second);
            assert!(
                acquire(&claims, key, &mut second_attached)
                    .await
                    .unwrap()
                    .is_none()
            );
        });
    }

    #[test]
    fn negative_control_attachment_owned_claims_admit_two_supervisors() {
        futures::executor::block_on(async {
            let first_owner = OperationClaims::default();
            let second_owner = OperationClaims::default();
            let key = [7; 48];
            let (_first, mut first_attached) = oneshot::channel();
            let _first = acquire(&first_owner, key, &mut first_attached)
                .await
                .unwrap()
                .unwrap();
            let (_second, mut second_attached) = oneshot::channel();
            // The old independent-client shape has no provider-wide owner.
            // Identical session identities can therefore enter twice.
            let second = acquire(&second_owner, key, &mut second_attached).now_or_never();
            assert!(second.unwrap().unwrap().is_some());
        });
    }
}
