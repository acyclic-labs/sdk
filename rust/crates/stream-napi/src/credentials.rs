use acyclic_stream::grpc::{CredentialError, CredentialProvider};
use napi::{
    bindgen_prelude::{Promise, Status},
    threadsafe_function::ThreadsafeFunction,
};
use std::sync::Arc;

pub(super) type Callback = ThreadsafeFunction<(), Promise<String>, (), Status, false, true>;

pub(super) fn provider(callback: Callback) -> CredentialProvider {
    let callback = Arc::new(callback);
    CredentialProvider::new(move || {
        let callback = callback.clone();
        async move {
            // Catch both synchronous throws and rejected promises. A weak N-API
            // handle does not keep an otherwise idle Node process alive.
            let result = callback
                .call_async_catch(())
                .await
                .map_err(|_| CredentialError::Invalid)?;
            result.await.map_err(|_| CredentialError::Invalid)
        }
    })
}
