use super::{AuthorityVerifier, Scope};
use crate::{Error, Result};
use acyclic_fs::access::Capability;

impl AuthorityVerifier {
    /// Verify a signed scope and extract its single filesystem grant payload.
    /// Call once when binding a view, then use `acyclic_fs::access::PathTrie` for
    /// checks. Principal attributes and operation context come from the host.
    pub fn filesystem_capability(&self, scope: &Scope) -> Result<Capability> {
        self.verify(scope, &mut None)?;
        let mut result = None;
        for label in scope.capabilities().iter() {
            if let Some(capability) = Capability::from_scope_label(label)
                .map_err(|error| Error::Unauthorized(error.to_string()))?
            {
                if result.is_some() {
                    return Err(Error::Unauthorized(
                        "scope has multiple filesystem grants".into(),
                    ));
                }
                capability
                    .validate()
                    .map_err(|error| Error::Unauthorized(error.to_string()))?;
                result = Some(capability);
            }
        }
        result.ok_or_else(|| Error::Unauthorized("scope has no filesystem grant".into()))
    }
}
