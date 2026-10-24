//! Shared storage owners retain the provider's future and admission boundary.
#[macro_use]
#[path = "../src/async_storage/contract.rs"]
mod contract;

use acyclic_stream::{ProviderPlatform, ProviderTask};
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

/// Generic input used to exercise the production forwarding rule.
pub trait Input: Send + Sync {
    /// Reads the input without extending its lifetime.
    fn value(&self) -> usize;
}
impl Input for usize {
    fn value(&self) -> usize {
        *self
    }
}

// Instantiate the production contract generator's ordinary, generic and default
// method forms. Their forwarding rule is shared by every storage operation.
contract! {
/// Asynchronous method forms accepted by the contract generator.
pub trait Probe: ProviderPlatform {
    /// Ordinary method with synchronous provider admission.
    fn ordinary(&self, value: usize) -> impl Future<Output = usize> + ProviderTask;
    /// Generic method borrowing its input.
    fn generic<G: Input>(&self, value: &G) -> impl Future<Output = usize> + ProviderTask;
    /// Default body whose shared implementation must still delegate.
    fn defaulted(&self, value: usize) -> impl Future<Output = usize> + ProviderTask {
        std::future::ready(value)
    }
}
}

struct Provider(AtomicUsize);
impl Probe for Provider {
    fn ordinary(&self, value: usize) -> impl Future<Output = usize> + ProviderTask {
        self.0.fetch_add(1, Ordering::SeqCst);
        std::future::ready(value)
    }
    fn generic<G: Input>(&self, value: &G) -> impl Future<Output = usize> + ProviderTask {
        self.0.fetch_add(1, Ordering::SeqCst);
        std::future::ready(value.value())
    }
    fn defaulted(&self, value: usize) -> impl Future<Output = usize> + ProviderTask {
        self.0.fetch_add(1, Ordering::SeqCst);
        std::future::ready(value)
    }
}

#[test]
fn shared_contract_preserves_provider_admission_without_polling() {
    let provider = Arc::new(Provider(AtomicUsize::new(0)));
    let outer = Arc::new(provider.clone());
    let direct = Probe::ordinary(provider.as_ref(), 7);
    let shared = Probe::ordinary(&provider, 7);
    let nested = Probe::ordinary(&outer, 7);
    let value = 7;
    let direct_generic = Probe::generic(provider.as_ref(), &value);
    let shared_generic = Probe::generic(&provider, &value);
    let nested_generic = Probe::generic(&outer, &value);
    let direct_defaulted = Probe::defaulted(provider.as_ref(), value);
    let shared_defaulted = Probe::defaulted(&provider, value);
    let nested_defaulted = Probe::defaulted(&outer, value);
    assert_eq!(
        provider.0.load(Ordering::SeqCst),
        9,
        "shared forwarding must enter the actual provider at admission"
    );
    assert_eq!(futures::executor::block_on(direct), 7);
    assert_eq!(futures::executor::block_on(shared), 7);
    assert_eq!(futures::executor::block_on(nested), 7);
    assert_eq!(futures::executor::block_on(direct_generic), 7);
    assert_eq!(futures::executor::block_on(shared_generic), 7);
    assert_eq!(futures::executor::block_on(nested_generic), 7);
    assert_eq!(futures::executor::block_on(direct_defaulted), 7);
    assert_eq!(futures::executor::block_on(shared_defaulted), 7);
    assert_eq!(futures::executor::block_on(nested_defaulted), 7);
    assert_eq!(provider.0.load(Ordering::SeqCst), 9);
}

#[test]
fn shared_contract_retains_the_leaf_future_layout_for_generic_and_ordinary_methods() {
    let provider = Arc::new(Provider(AtomicUsize::new(0)));
    let outer = Arc::new(provider.clone());
    let value = 9;
    let ordinary = std::mem::size_of_val(&Probe::ordinary(provider.as_ref(), value));
    let generic = std::mem::size_of_val(&Probe::generic(provider.as_ref(), &value));
    let defaulted = std::mem::size_of_val(&Probe::defaulted(provider.as_ref(), value));
    assert_eq!(
        std::mem::size_of_val(&Probe::ordinary(&provider, value)),
        ordinary
    );
    assert_eq!(
        std::mem::size_of_val(&Probe::ordinary(&outer, value)),
        ordinary
    );
    assert_eq!(
        std::mem::size_of_val(&Probe::generic(&provider, &value)),
        generic
    );
    assert_eq!(
        std::mem::size_of_val(&Probe::generic(&outer, &value)),
        generic
    );
    assert_eq!(
        std::mem::size_of_val(&Probe::defaulted(&provider, value)),
        defaulted
    );
    assert_eq!(
        std::mem::size_of_val(&Probe::defaulted(&outer, value)),
        defaulted
    );
}
