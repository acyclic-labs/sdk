use super::*;

/// Immutable selected entry admitted with a deployment configuration.
/// The digest identifies bytes in the original account's Objects provider;
/// neither this reference nor its selected names confer resource authority.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = EntrySource::validate_from_proto)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/EntrySource.ts", rename_all = "camelCase")]
pub struct EntrySource {
    #[wire(tag = 1, bytes, from = source_digest, into = digest_bytes)]
    #[ts(type = r#"import("../../readonly.js").ReadonlyBytes"#)]
    digest: acyclic_objects::Digest,
    #[wire(tag = 2, uint64)]
    length: PositiveU64,
    #[wire(tag = 3)]
    environment: String,
    #[wire(tag = 4)]
    entry: String,
}

fn source_digest(bytes: Vec<u8>) -> Result<acyclic_objects::Digest, DomainError> {
    let bytes = bytes
        .try_into()
        .map_err(|_| DomainError::Contract(crate::ContractError::InvalidArgument))?;
    Ok(acyclic_objects::Digest::from_bytes(bytes))
}

fn digest_bytes(digest: acyclic_objects::Digest) -> Vec<u8> {
    digest.as_bytes().to_vec()
}

impl EntrySource {
    /// Retain the exact publication identity and original selected names.
    pub fn new(
        digest: acyclic_objects::Digest,
        length: u64,
        environment: String,
        entry: String,
    ) -> Result<Self, DomainError> {
        let source = Self {
            digest,
            length: PositiveU64::new(length)?,
            environment,
            entry,
        };
        source.validate_from_proto()?;
        Ok(source)
    }

    fn validate_from_proto(&self) -> Result<(), DomainError> {
        Self::validate_selection(&self.environment, &self.entry)
    }

    pub(crate) fn validate_selection(environment: &str, entry: &str) -> Result<(), DomainError> {
        crate::addressing::validate_dns_label(environment.as_bytes())
            .map_err(|_| DomainError::Contract(crate::ContractError::InvalidArgument))?;
        crate::addressing::NamespaceId::validate_deployment(entry.as_bytes())?;
        Ok(())
    }

    /// Canonical Objects content identity, without another hash or codec.
    pub const fn digest(&self) -> acyclic_objects::Digest {
        self.digest
    }

    /// Exact descriptor byte admission.
    pub fn length(&self) -> u64 {
        self.length.get()
    }

    /// Original selected environment; restore never chooses an overlay.
    pub fn environment(&self) -> &str {
        &self.environment
    }

    /// Original selected entry, independent of an actor instance identifier.
    pub fn entry(&self) -> &str {
        &self.entry
    }
}
