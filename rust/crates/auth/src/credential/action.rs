//! An owner operation is immediate in-process or awaited across an owner boundary.
use super::CredentialError;
use std::{
    future::{Future, Ready},
    pin::Pin,
    task::{Context, Poll},
};

/// An operation owned by the credential custodian. Dropping it cancels this
/// interest; the operation must not retain the producer of its completion event.
#[cfg(not(target_arch = "wasm32"))]
pub type CredentialPending<T> = Pin<Box<dyn Future<Output = Result<T, CredentialError>> + Send>>;
/// Browser owners stay on their JavaScript thread.
#[cfg(target_arch = "wasm32")]
pub type CredentialPending<T> = Pin<Box<dyn Future<Output = Result<T, CredentialError>>>>;

/// One exact owner operation. Native cache checks and registration use `Ready`
/// without a future allocation; foreign owners use their own completion event.
/// The caller's original deadline/cancellation must surround the entire action.
pub enum CredentialAction<T> {
    /// In-process admission, with no scheduler hop or allocation.
    Ready(Ready<Result<T, CredentialError>>),
    /// Admission by a thread-confined owner, without blocking transport workers.
    Pending(CredentialPending<T>),
}

impl<T> From<Result<T, CredentialError>> for CredentialAction<T> {
    fn from(result: Result<T, CredentialError>) -> Self {
        Self::Ready(std::future::ready(result))
    }
}

impl<T> Future for CredentialAction<T> {
    type Output = Result<T, CredentialError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut *self {
            Self::Ready(result) => Pin::new(result).poll(cx),
            Self::Pending(result) => result.as_mut().poll(cx),
        }
    }
}
