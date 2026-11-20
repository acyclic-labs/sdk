//! Observations of checkout state used by failure-atomicity cases.

use super::{Checkout, CheckoutDependencies, CheckoutMode, GenerationRoot, Head, ObjectId};

/// The state that an operation must preserve when its candidate is refused.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckoutState {
    base_generation_root: ObjectId,
    generation_root: ObjectId,
    base_root: GenerationRoot,
    root: GenerationRoot,
    authority_head: Option<Head>,
    prepared_merge_parent: Option<ObjectId>,
    dependencies: CheckoutDependencies,
    mode: CheckoutMode,
}

/// Captures the complete candidate and its original publication and read dependencies.
pub fn checkout_state<A, O>(checkout: &Checkout<A, O>) -> CheckoutState {
    CheckoutState {
        base_generation_root: checkout.root.base,
        generation_root: checkout.generation_root,
        base_root: checkout.base_root.clone(),
        root: checkout.root.clone(),
        authority_head: checkout.authority_head,
        prepared_merge_parent: checkout.prepared_merge_parent,
        dependencies: checkout.dependencies.proof().dependencies.clone(),
        mode: checkout.mode,
    }
}
