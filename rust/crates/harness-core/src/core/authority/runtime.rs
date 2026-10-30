//! One runtime owner: native custody or an admitted platform operation port.
use super::*;
use crate::OperationId;
use std::{future::Future, task::Poll};

/// The original turn event fences both dispatch and same-poll completion.
/// Dropping this future drops the single in-flight operation without replay.
pub(crate) async fn until_turn_stopped<T>(
    stop: Option<acyclic_actors::guest::CancellationToken>,
    operation: impl Future<Output = Result<T>>,
) -> Result<T> {
    let cancelled = async {
        match &stop {
            Some(stop) => stop.cancelled().await,
            None => std::future::pending().await,
        }
    };
    futures::pin_mut!(operation, cancelled);
    std::future::poll_fn(|cx| {
        if cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(Error::InteractionRejected(
                crate::InteractionRejection::Cancelled,
            )));
        }
        let result = operation.as_mut().poll(cx);
        if result.is_ready() && stop.as_ref().is_some_and(|stop| stop.is_cancelled()) {
            return Poll::Ready(Err(Error::InteractionRejected(
                crate::InteractionRejection::Cancelled,
            )));
        }
        result
    })
    .await
}

/// Authority used by the shared server, turn executor and volume factory.
/// Native roots stay in the host. A platform runtime retains only its original
/// admitted invocation and never creates a root from guest-supplied material.
#[derive(Clone)]
pub struct RuntimeAuthority {
    custody: Custody,
}

#[derive(Clone)]
enum Custody {
    Native(AuthorityRoot),
    Platform {
        authority: Arc<AdmittedAuthority>,
        operation: OperationId,
    },
}

impl From<AuthorityRoot> for RuntimeAuthority {
    fn from(root: AuthorityRoot) -> Self {
        Self {
            custody: Custody::Native(root),
        }
    }
}

impl RuntimeAuthority {
    /// Observe the same authenticated trust publications as the retained owner.
    #[cfg(all(target_os = "linux", feature = "native-execution"))]
    pub(crate) fn trust_changes(&self) -> Arc<dyn acyclic_auth::trust::ChangeSource> {
        match &self.custody {
            Custody::Native(root) => root.trust_changes(),
            Custody::Platform { authority, .. } => authority.caller.verifier.public.changes.clone(),
        }
    }

    /// Original invocation stop, when custody belongs to an admitted platform.
    pub fn operation_context(&self) -> Option<&acyclic_auth::operation::Context> {
        match &self.custody {
            Custody::Native(_) => None,
            Custody::Platform { authority, .. } => Some(&authority.context),
        }
    }

    /// Bind the original operation identity supplied by the authenticated host.
    /// Its uncertainty is always reported under this identity, without replay.
    pub fn admitted(authority: AdmittedAuthority, operation: OperationId) -> Self {
        Self {
            custody: Custody::Platform {
                authority: Arc::new(authority),
                operation,
            },
        }
    }

    /// The admitted grant, present only for a platform-owned invocation.
    pub fn caller(&self) -> Option<(&Authority, &Scope)> {
        match &self.custody {
            Custody::Native(_) => None,
            Custody::Platform { authority, .. } => Some(authority.caller()),
        }
    }

    /// Public verification with the same retained custody lifetime.
    pub fn verifier(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<AuthorityVerifier> {
        match &self.custody {
            Custody::Native(root) => root.verifier(work),
            Custody::Platform {
                authority,
                operation,
            } => {
                authority.check()?;
                let mut verifier = authority.caller.verifier.copy(work)?;
                verifier.admission =
                    runtime_signing::AdmissionPort::platform(authority, *operation);
                Ok(verifier)
            }
        }
    }

    /// A local root may create another audience. An admitted invocation keeps
    /// its original audience; a request cannot rebind it to another recipient.
    pub fn with_audience(&self, audience: Authority) -> Result<Self> {
        match &self.custody {
            Custody::Native(root) => Ok(Self::from(root.with_audience(audience))),
            Custody::Platform { authority, .. } => {
                authority.check()?;
                if authority.caller().0 != &audience {
                    return Err(Error::Unauthorized(
                        "platform authority belongs to another audience".into(),
                    ));
                }
                Ok(self.clone())
            }
        }
    }

    /// Issue only at the trusted runtime boundary. The platform producer checks
    /// the actual resource or acting agent before returning sealed evidence.
    pub async fn issue(
        &self,
        parent: Option<&Scope>,
        id: &str,
        capabilities: &Capabilities,
        agent: Option<AgentId>,
    ) -> Result<Scope> {
        match &self.custody {
            Custody::Native(root) => match parent {
                Some(parent) => root.issue_derived(parent, id, capabilities, agent, &mut None),
                None => root.bootstrap_scope(id, capabilities, agent, &mut None),
            },
            Custody::Platform {
                authority,
                operation,
            } => {
                authority
                    .derive(
                        &authority.context,
                        *operation,
                        parent.unwrap_or(authority.caller().1),
                        id,
                        capabilities,
                        agent,
                    )
                    .await
            }
        }
    }

    /// Typed private-read delegation under the same admitted owner.
    pub async fn delegate(
        &self,
        parent: &Scope,
        id: &str,
        request: ScopeDerivation<'_>,
    ) -> Result<Scope> {
        match &self.custody {
            Custody::Native(root) => {
                root.verifier(&mut None)?.verify(parent, &mut None)?;
                let (capabilities, agent) = request.resolve(parent)?;
                root.issue_derived(parent, id, capabilities.as_ref(), agent, &mut None)
            }
            Custody::Platform {
                authority,
                operation,
            } => {
                authority
                    .derive_request(&authority.context, *operation, parent, id, request)
                    .await
            }
        }
    }

    /// Native-only enrollment operations require the host's custody explicitly.
    /// A platform invocation is qualified for its admitted port instead.
    pub fn native(&self) -> Option<&AuthorityRoot> {
        match &self.custody {
            Custody::Native(root) => Some(root),
            Custody::Platform { .. } => None,
        }
    }
}
