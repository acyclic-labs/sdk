use super::*;

/// Resolves a pinned provider attempt into its owning conversation history.
pub struct ConversationEffectHost<P> {
    stream: Arc<P>,
    pub(super) authority: Authority,
    issuer: AuthorityIssuer,
    pub(super) scope: Scope,
    schemas: SchemaRegistry,
    content: Arc<dyn ContentResidencyVerifier>,
    pub(super) providers: EffectRegistry,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    native_metadata: crate::NativeCustody,
}

impl<P: StreamProvider> ConversationEffectHost<P> {
    /// Binds the conversation, signed scope, providers and original custody.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream: std::sync::Arc<P>,
        authority: Authority,
        issuer: AuthorityIssuer,
        scope: Scope,
        schemas: SchemaRegistry,
        content: Arc<dyn ContentResidencyVerifier>,
        providers: EffectRegistry,
        custody: crate::NativeCustody,
        work: crate::NativeWork<'_>,
    ) -> Result<Self> {
        #[allow(unused_mut)]
        let mut host = Self {
            stream,
            authority,
            issuer,
            scope,
            schemas,
            content,
            providers,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_metadata: custody,
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            host.require_native_metadata(work)?;
            host.providers.require_native_metadata(work)?;
            let mut slot = work
                .claim(
                    std::mem::size_of::<Self>() as u64,
                    0,
                    acyclic_fs::WorkCounters::UNCHARGED,
                )
                .map_err(|cause| Error::NativeWork {
                    cause,
                    custody: None,
                })?;
            host.native_metadata
                .as_mut()
                .expect("validated native host")
                .absorb(&mut slot)
                .map_err(|cause| Error::NativeWork {
                    cause,
                    custody: None,
                })?;
            let mut io = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(work)?,
            );
            host.validate_binding(&mut Some(&mut io))?;
            return Ok(host);
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if host.native_metadata.is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native effect metadata requires original work",
            });
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = custody;
        let _ = work;
        host.validate_binding(&mut None)?;
        Ok(host)
    }
}

mod admission;
pub(crate) use admission::effect_error;
use admission::*;
mod accounting;
use accounting::*;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native;
mod reading;
mod task;
