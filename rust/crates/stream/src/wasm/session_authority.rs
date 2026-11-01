//! Explicit owner-authenticated public trust for browser-local sessions.
use super::*;
use crate::runtime::Executor;
use crate::session::{Authority, AuthorityState, TrustSnapshot};
use acyclic_auth::{
    trust::{ChangeSource, Changed, Snapshot, SourceClosed},
    verify::VerifyError,
};
use futures::channel::oneshot;
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
/// The owner's current public trust and the sessions waiting for its next
/// update. Dropping the owner resolves every wait as [`SourceClosed`].
struct State {
    current: Mutex<Arc<Snapshot>>,
    waiters: Mutex<Vec<oneshot::Sender<()>>>,
}
impl State {
    /// Wakes every registered wait; the new trust is already current.
    fn publish(&self) {
        let waiters = self
            .waiters
            .lock()
            .map(|mut waiters| std::mem::take(&mut *waiters))
            .unwrap_or_default();
        for waiter in waiters {
            let _ = waiter.send(());
        }
    }
}
impl ChangeSource for State {
    fn check_open(&self) -> Result<(), SourceClosed> {
        Ok(())
    }
    fn changed(&self) -> Changed {
        let (sender, receiver) = oneshot::channel();
        // A wait that cannot register resolves closed, never unchanged.
        if let Ok(mut waiters) = self.waiters.lock() {
            if waiters.len() == waiters.capacity() {
                waiters.retain(|waiter| !waiter.is_canceled());
            }
            waiters.push(sender);
        }
        Box::pin(async move { receiver.await.map_err(|_| SourceClosed) })
    }
}
impl AuthorityState for State {
    fn snapshot(&self) -> Result<TrustSnapshot, VerifyError> {
        let current = self.current.lock().map_err(|_| VerifyError::Trust)?;
        Ok(TrustSnapshot {
            trust: current.clone(),
            now_unix_seconds: (Date::now() / 1000.0) as u64,
        })
    }
    fn changes(&self) -> &dyn ChangeSource {
        self
    }
    fn expires_at(
        &self,
        unix_seconds: u64,
    ) -> crate::BoxProviderFuture<'_, Result<(), SourceClosed>> {
        let milliseconds = (unix_seconds as f64 * 1000.0 - Date::now()).max(0.0);
        let expiry =
            crate::runtime::PlatformExecutor.sleep(milliseconds.min(f64::from(i32::MAX)) as u32);
        Box::pin(async move {
            expiry.await;
            Ok(())
        })
    }
}

/// Public keyring feed supplied by the owner, independently of credentials.
/// `update` must receive authenticated current keyring state from that runtime.
#[wasm_bindgen]
pub struct WasmSessionAuthority {
    state: Arc<State>,
    pub(super) authority: Arc<Authority>,
}
impl WasmSessionAuthority {
    /// The same authenticated trust feed for an embedded product server.
    #[must_use]
    pub fn authority(&self) -> Arc<Authority> {
        self.authority.clone()
    }
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
            current: Mutex::new(Arc::new(snapshot)),
            waiters: Mutex::default(),
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
            if current.environment != next.environment
                || current.account != next.account
                || current.minimum_key_epoch > next.minimum_key_epoch
                || !current.revoked_key_ids.is_subset(&next.revoked_key_ids)
                || !current.revoked_block_ids.is_subset(&next.revoked_block_ids)
            {
                return Err(js_error(StreamError::AccessDenied));
            }
            *current = Arc::new(next);
        }
        self.state.publish();
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
