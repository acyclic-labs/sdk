use super::*;

/// An explicitly selected HTTPS handle. Absence denies outbound HTTPS.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = FetchBinding::validate_from_proto)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/FetchBinding.ts", rename_all = "camelCase")]
pub struct FetchBinding {
    #[wire(tag = 1)]
    name: String,
    #[wire(tag = 2, message)]
    egress: Vec<EgressRule>,
}

impl FetchBinding {
    /// Select one environment handle and its complete destination allow-list.
    pub fn new(name: String, egress: Vec<EgressRule>) -> Result<Self, DomainError> {
        let selection = Self { name, egress };
        selection.validate_from_proto()?;
        Ok(selection)
    }

    /// Selected environment property.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Exact destinations; hosts must resolve, validate and pin addresses per connection.
    pub fn egress(&self) -> &[EgressRule] {
        &self.egress
    }

    fn validate_from_proto(&self) -> Result<(), DomainError> {
        let mut destinations = std::collections::HashSet::new();
        if !valid_environment_name(&self.name)
            || self.egress.is_empty()
            || self
                .egress
                .iter()
                .any(|rule| !destinations.insert((rule.host(), rule.port())))
        {
            return Err(DomainError::Contract(crate::ContractError::InvalidArgument));
        }
        Ok(())
    }
}

/// Exact canonical DNS authority. TLS and certificate validation are mandatory.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = EgressRule::validate_from_proto)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/EgressRule.ts", rename_all = "camelCase")]
pub struct EgressRule {
    #[wire(tag = 1)]
    host: String,
    #[wire(tag = 2)]
    port: u32,
}

impl EgressRule {
    /// Reject IP literals, alternate numeric address forms and local DNS suffixes.
    pub fn new(host: String, port: u32) -> Result<Self, DomainError> {
        let rule = Self { host, port };
        rule.validate_from_proto()?;
        Ok(rule)
    }

    /// Lowercase canonical ASCII DNS name.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Valid nonzero TLS port.
    pub fn port(&self) -> u32 {
        self.port
    }

    /// Shared authoring/admission grammar; public address admission still belongs to the host.
    pub fn valid_host(value: &str) -> bool {
        value.len() <= 253
            && value.contains('.')
            && value
                .split('.')
                .all(|label| crate::addressing::validate_dns_label(label.as_bytes()).is_ok())
            && value
                .rsplit('.')
                .next()
                .is_some_and(|tail| !tail.bytes().all(|b| b.is_ascii_digit()))
            && !value.split('.').any(|part| {
                part.strip_prefix("0x")
                    .is_some_and(|digits| digits.bytes().all(|b| b.is_ascii_hexdigit()))
            })
            && !value.ends_with(".localhost")
            && !value.ends_with(".local")
    }

    fn validate_from_proto(&self) -> Result<(), DomainError> {
        if !Self::valid_host(&self.host) || !(1..=u32::from(u16::MAX)).contains(&self.port) {
            return Err(DomainError::Contract(crate::ContractError::InvalidArgument));
        }
        Ok(())
    }
}
