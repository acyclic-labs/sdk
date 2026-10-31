//! Original credential custody shared by both availability transports.
use super::*;
use acyclic_auth::credential::{CredentialProvider, ResolvedCredential};
use acyclic_auth::operation::Context;
use futures::future::{self, BoxFuture, Either};
use std::future::Future;

#[cfg(test)]
pub(crate) mod context_tests;

fn denied() -> Error {
    ErrorCode::AccessDenied.into()
}

pub(crate) struct Credential {
    pub(crate) lease: Option<ResolvedCredential>,
    stop: BoxFuture<'static, ()>,
}

impl Credential {
    pub(crate) async fn resolve(provider: Option<&CredentialProvider>) -> Result<Self, Error> {
        let Some(provider) = provider else {
            return Ok(Self {
                lease: None,
                stop: Box::pin(future::pending()),
            });
        };
        let observed_at = tokio::time::Instant::now();
        let watch = provider.watch().await.map_err(|_| denied())?;
        let lease = provider.resolve().await.map_err(|_| denied())?;
        lease.check().await.map_err(|_| denied())?;
        let expiry = watch
            .as_ref()
            .zip(lease.binding())
            .map(|(watch, binding)| {
                observed_at
                    .checked_add(std::time::Duration::from_secs(
                        binding
                            .expires_at_unix_seconds
                            .saturating_sub(watch.validity().now_unix_seconds),
                    ))
                    .ok_or_else(denied)
            })
            .transpose()?;
        let stop = Box::pin(async move {
            let change = async move {
                match watch {
                    Some(watch) => {
                        let _ = watch.await;
                    }
                    None => future::pending().await,
                }
            };
            let expiry = async move {
                match expiry {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => future::pending().await,
                }
            };
            future::select(std::pin::pin!(change), std::pin::pin!(expiry)).await;
        });
        Ok(Self {
            lease: Some(lease),
            stop,
        })
    }

    async fn next<S: Stream<Item = Result<crate::wire::AvailabilityCursor, Error>> + Unpin>(
        &mut self,
        source: &mut S,
    ) -> Result<crate::wire::AvailabilityCursor, Error> {
        let lease = &self.lease;
        let delivery = async {
            let next = source.next().await.ok_or_else(unavailable)??;
            if let Some(lease) = lease {
                lease.check().await.map_err(|_| denied())?;
            }
            Ok(next)
        };
        match future::select(self.stop.as_mut(), std::pin::pin!(delivery)).await {
            Either::Left(_) => return Err(denied()),
            Either::Right((next, _)) => next,
        }
    }
}

pub(crate) async fn observe(
    source: impl Stream<Item = Result<crate::wire::AvailabilityCursor, Error>> + Send + 'static,
    mut credential: Credential,
    context: Option<Context>,
) -> Result<Availability, Error> {
    let mut source = Box::pin(source);
    let initial = credential.next(&mut source).await?.try_into()?;
    let changes = stream::try_unfold(
        (source, credential, context),
        |(mut source, mut credential, context)| async move {
            let next = crate::operation::run(context.as_ref(), credential.next(&mut source))
                .await?
                .try_into()?;
            Ok(Some((next, (source, credential, context))))
        },
    );
    Ok(Availability::new(initial, changes))
}

/// Opening and delivery retain the same caller-owned stop.
pub(crate) async fn open(
    context: Option<&Context>,
    work: impl Future<Output = Result<Availability, Error>>,
) -> Result<Availability, Error> {
    crate::operation::run(context, work).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_auth::credential::{
        CredentialCache, CredentialRevision, CredentialSnapshot, CredentialValidity,
    };
    use futures::FutureExt as _;

    #[tokio::test(start_paused = true)]
    async fn idle_observation_ends_on_original_credential_change_or_absolute_expiry() {
        for revoke in [false, true] {
            let revision = CredentialRevision::from([1; 32]);
            let issued = revision.clone();
            let cache = CredentialCache::new(
                move |_| {
                    let revision = issued.clone();
                    async move { CredentialSnapshot::new("original".into(), revision, 11) }
                },
                move || CredentialValidity {
                    now_unix_seconds: 1,
                    revision: Some(revision.clone()),
                },
            );
            let credential = Credential::resolve(Some(&cache.clone().into()))
                .await
                .unwrap();
            let source = AvailabilitySource::new();
            let key = Digest::of(b"idle credential");
            let registered = source.subscribe(key).unwrap();
            // Opening delay cannot extend the original absolute expiry.
            tokio::time::advance(std::time::Duration::from_secs(7)).await;
            let mut observation = observe(
                crate::route::availability_frames(registered),
                credential,
                None,
            )
            .await
            .unwrap();
            if revoke {
                cache.invalidate().unwrap();
            } else {
                tokio::time::advance(std::time::Duration::from_secs(3)).await;
            }
            assert_eq!(
                observation.changed().now_or_never().unwrap().unwrap_err(),
                denied()
            );
        }
    }
}
