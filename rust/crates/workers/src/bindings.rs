//! Materialization of admitted deployment declarations into executable bindings.
use crate::service::{WorkerInvoker, WorkerServiceBinding};
use acyclic_actors::{
    client, domain,
    namespace::{Invoker, Namespace},
};
use std::{collections::BTreeMap, sync::Arc};

/// A selected declaration materialized by the holder's authorized host adapter.
pub enum DeploymentBinding<T> {
    /// An actor namespace: IDs are local values and first invocation activates.
    Namespace(Namespace<T>),
    /// A named Worker service, without public HTTP authentication.
    Service(WorkerServiceBinding<T>),
}

/// Build only Actors/Workers env handles; other product handles belong to their adapters.
pub fn materialize_bindings<T: Invoker + WorkerInvoker>(
    declarations: &[domain::Binding],
    authority: Arc<T>,
) -> Result<BTreeMap<String, DeploymentBinding<T>>, client::Error> {
    let mut env = BTreeMap::new();
    for binding in declarations {
        let value = if binding.namespace_target().is_some() {
            DeploymentBinding::Namespace(
                Namespace::from_binding(binding, authority.clone())
                    .map_err(|error| client::Error::Configuration(error.to_string()))?,
            )
        } else if binding.service_target().is_some() {
            DeploymentBinding::Service(WorkerServiceBinding::from_binding(
                binding,
                authority.clone(),
            )?)
        } else {
            return Err(domain::DomainError::InvalidBinding.into());
        };
        if env.insert(binding.name().to_owned(), value).is_some() {
            return Err(domain::DomainError::InvalidBinding.into());
        }
    }
    Ok(env)
}
