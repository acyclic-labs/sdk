use super::*;
/// Routes provider observations into their owning conversation history.
pub struct ConversationEffectHost<P> {
    stream: Arc<P>,
    authority: Authority,
    issuer: AuthorityIssuer,
    scope: Scope,
    schemas: SchemaRegistry,
    content: Arc<dyn ContentResidencyVerifier>,
    providers: EffectRegistry,
    native_metadata: NativeCustody,
}
impl<P: StreamProvider> ConversationEffectHost<P> {
    /// Binds the signed conversation and catalog in one shared host.
    pub fn new(
        stream: Arc<P>,
        binding: (Authority, AuthorityIssuer, Scope),
        schemas: SchemaRegistry,
        content: Arc<dyn ContentResidencyVerifier>,
        providers: EffectRegistry,
        custody: NativeCustody,
        work: NativeWork<'_>,
    ) -> Result<Arc<Self>> {
        let (authority, issuer, scope) = binding;
        let host = Self {
            stream,
            authority,
            issuer,
            scope,
            schemas,
            content,
            providers,
            native_metadata: custody,
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut host = host;
        #[cfg(feature = "filesystem")]
        let mut io = history(work)?;
        #[cfg(feature = "filesystem")]
        let observed = &mut io.as_mut().map(|io| io as &mut dyn HistoryIo);
        #[cfg(not(feature = "filesystem"))]
        let observed: &mut Option<&mut dyn HistoryIo> = &mut None;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        match work {
            Some(work) => {
                host.require_native_metadata(work)?;
                host.providers.require_native_metadata(work)?;
            }
            None if host.native_metadata.is_some() => {
                return Err(boundary(
                    &None,
                    Unauthorized,
                    "native effect metadata requires original work",
                ));
            }
            None => {}
        }
        if let Some(io) = observed.as_deref_mut() {
            io.check_cancellation()?;
        }
        if host.authority.kind != crate::core::AggregateKind::Conversation {
            return Err(boundary(
                observed,
                Invalid,
                "effect host requires a conversation",
            ));
        }
        let verifier = host.issuer.verifier(observed)?;
        verifier.verify_audience(&host.authority, observed)?;
        verifier.verify(&host.scope, observed)?;
        if !host.scope.capabilities().contains(capability::EFFECT_RUN) {
            return Err(boundary(observed, Unauthorized, "scope lacks effect:run"));
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            let mut slot = work.claim(
                std::mem::size_of::<Self>() as u64,
                1,
                acyclic_fs::WorkCounters::UNCHARGED,
            )?;
            host.native_metadata
                .as_mut()
                .ok_or(Error::NativeBoundary {
                    code: Invalid,
                    reason: "effect host lacks metadata custody",
                })?
                .absorb(&mut slot)?;
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        if work.is_some() || host.native_metadata.is_some() {
            return Err(Error::Invalid("native effects are unavailable".into()));
        }
        Ok(Arc::new(host))
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn require_native_metadata(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        if self
            .native_metadata
            .as_ref()
            .is_some_and(|guard| guard.work().shares_ledger(work))
        {
            Ok(())
        } else {
            Err(Error::NativeBoundary {
                code: Invalid,
                reason: "effect metadata belongs to another work",
            })
        }
    }
}
mod accounting;
mod reading;
mod task;
use accounting::*;
