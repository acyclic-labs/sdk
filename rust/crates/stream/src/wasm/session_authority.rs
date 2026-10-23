//! Explicit owner-authenticated public trust for browser-local sessions.
use super::*;
use crate::session::{Authority, AuthorityState, TrustSnapshot, runtime::Executor};
use acyclic_auth::{trust::Snapshot, verify::VerifyError};
use std::sync::{Arc, Mutex};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicTrust {
    environment: String,
    environment_keys: Vec<[u8; 32]>,
    account: String,
    issuer_keys: Vec<[u8; 32]>,
    minimum_key_epoch: u64,
    revoked_key_ids: std::collections::HashSet<String>,
    revoked_block_ids: std::collections::HashSet<[u8; 32]>,
}
impl From<PublicTrust> for Snapshot {
    fn from(value: PublicTrust) -> Self {
        Self {
            environment: value.environment,
            environment_keys: value.environment_keys,
            account: value.account,
            issuer_keys: value.issuer_keys,
            minimum_key_epoch: value.minimum_key_epoch,
            revoked_key_ids: value.revoked_key_ids,
            revoked_block_ids: value.revoked_block_ids,
        }
    }
}
struct State {
    current: Mutex<(Arc<Snapshot>, u64)>,
    changed: tokio::sync::Notify,
}
impl AuthorityState for State {
    fn snapshot(&self) -> Result<TrustSnapshot, VerifyError> {
        let current = self.current.lock().map_err(|_| VerifyError::Trust)?;
        Ok(TrustSnapshot {
            trust: current.0.clone(),
            now_unix_seconds: (Date::now() / 1000.0) as u64,
            revision: current.1,
        })
    }
    fn changed(&self, revision: u64) -> crate::BoxProviderFuture<'_, ()> {
        Box::pin(async move {
            let notified = self.changed.notified();
            futures::pin_mut!(notified);
            notified.as_mut().enable();
            if self
                .current
                .lock()
                .map_or(true, |current| current.1 != revision)
            {
                return;
            }
            notified.await;
        })
    }
    fn expires_at(&self, unix_seconds: u64) -> crate::BoxProviderFuture<'_, ()> {
        let milliseconds = (unix_seconds as f64 * 1000.0 - Date::now()).max(0.0);
        crate::session::runtime::PlatformExecutor
            .sleep(milliseconds.min(f64::from(i32::MAX)) as u32)
    }
}

/// Public keyring feed supplied by the owner, independently of credentials.
/// `update` must receive authenticated current keyring state from that runtime.
#[wasm_bindgen]
pub struct WasmSessionAuthority {
    state: Arc<State>,
    pub(super) authority: Arc<Authority>,
}
#[wasm_bindgen]
impl WasmSessionAuthority {
    /// Accepts public trust JSON, a canonical recipient and presenting account.
    /// No signing key is accepted or inferred from a workload credential.
    #[wasm_bindgen(constructor)]
    pub fn new(
        public_trust: &str,
        audience: String,
        holder: Option<String>,
    ) -> Result<Self, JsValue> {
        let snapshot = parse(public_trust)?;
        let state = Arc::new(State {
            current: Mutex::new((Arc::new(snapshot), 0)),
            changed: tokio::sync::Notify::new(),
        });
        let authority =
            Arc::new(Authority::new(state.clone(), audience, holder).map_err(js_error)?);
        Ok(Self { state, authority })
    }

    /// Pushes a monotonic authenticated revocation update to every open session.
    pub fn update(&self, public_trust: &str) -> Result<(), JsValue> {
        let next = parse(public_trust)?;
        {
            let mut current = self
                .state
                .current
                .lock()
                .map_err(|_| js_error(StreamError::Unavailable))?;
            if current.0.environment != next.environment
                || current.0.account != next.account
                || current.0.minimum_key_epoch > next.minimum_key_epoch
                || !current.0.revoked_key_ids.is_subset(&next.revoked_key_ids)
                || !current
                    .0
                    .revoked_block_ids
                    .is_subset(&next.revoked_block_ids)
            {
                return Err(js_error(StreamError::AccessDenied));
            }
            let revision = current
                .1
                .checked_add(1)
                .ok_or_else(|| js_error(StreamError::Capacity))?;
            *current = (Arc::new(next), revision);
        }
        self.state.changed.notify_waiters();
        Ok(())
    }
}
fn parse(public_trust: &str) -> Result<Snapshot, JsValue> {
    check_command_size(public_trust.as_bytes())?;
    let snapshot: PublicTrust =
        serde_json::from_str(public_trust).map_err(|_| js_error(StreamError::InvalidArgument))?;
    if snapshot.environment.is_empty()
        || snapshot.account.is_empty()
        || snapshot.environment_keys.is_empty()
        || snapshot.issuer_keys.is_empty()
    {
        return Err(js_error(StreamError::InvalidArgument));
    }
    Ok(snapshot.into())
}

#[cfg(all(test, target_arch = "wasm32"))]
mod tests;
