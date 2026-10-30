//! One codec for retained Machine journal envelopes.
use super::*;

#[derive(Serialize, Deserialize)]
pub(super) struct Operation {
    pub(super) digest: [u8; 32],
    pub(super) phase: OperationPhase,
    pub(super) outcome: Option<MutationOutcome>,
}

#[derive(Serialize, Deserialize)]
enum Version {
    #[serde(rename = "1")]
    V1,
}
#[derive(Serialize, Deserialize)]
struct Envelope<T> {
    version: Version,
    value: T,
}

pub(super) fn encode(value: &impl Serialize) -> Result<Vec<u8>, ProviderError> {
    serde_json::to_vec(&Envelope {
        version: Version::V1,
        value,
    })
    .map_err(encoding)
}

pub(super) fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, ProviderError> {
    // The persisted encoder always writes the version first. Admit that fixed
    // header before serde can visit an arbitrary or truncated payload.
    acyclic_auth::format::header(
        bytes,
        b"{\"version\":",
        b"\"1\",\"value\":",
        unsupported,
        unsupported,
    )?;
    let envelope: Envelope<T> = serde_json::from_slice(bytes).map_err(encoding)?;
    Ok(envelope.value)
}

fn unsupported() -> ProviderError {
    ProviderError::Unsupported(
        "unsupported Machine journal version; recovery: restore the original version-one records"
            .into(),
    )
}

#[cfg(feature = "test-support")]
/// On-demand checks of the actual retained Machine journal codec.
pub mod codec_fuzz {
    use super::*;

    fn check_type<T: serde::de::DeserializeOwned + Serialize>(bytes: &[u8]) -> bool {
        let Ok(value) = decode::<T>(bytes) else {
            return true;
        };
        let Ok(encoded) = encode(&value) else {
            return false;
        };
        decode::<T>(&encoded)
            .and_then(|value| encode(&value))
            .as_deref()
            == Ok(encoded.as_slice())
    }

    /// An accepted envelope reaches one canonical fixed point for every payload.
    pub fn check(bytes: &[u8]) -> bool {
        check_type::<Operation>(bytes) && check_type::<MachineObservation>(bytes)
    }

    /// Capture the production encoder, using stable public contract values.
    pub fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>, ProviderError> {
        let id = MachineId::from_digest([1; 32]);
        let machine = MachineObservation {
            id,
            state: MachineState::Running,
            contract: MachineContract {
                image: Image::custom([2; 32])
                    .map_err(|error| ProviderError::Invalid(error.to_string()))?,
                selected_volume_ref: None,
                capabilities: BTreeSet::new(),
                compatibility: CompatibilityPolicy::BestEffort,
                compatibility_revision: [3; 32],
                suspension: SuspensionPolicy::Manual,
                expiration: ExpirationPolicy::Never,
                network_policy_digest: [4; 32],
                budgets: Budgets::default(),
            },
            endpoints: Vec::new(),
            last_checkpoint: None,
            created_at_unix_ms: 42,
            changed_at_unix_ms: 42,
        };
        Ok(vec![
            ("machine", encode(&machine)?),
            (
                "pending",
                encode(&Operation {
                    digest: [5; 32],
                    phase: OperationPhase::Pending,
                    outcome: None,
                })?,
            ),
            (
                "settled",
                encode(&Operation {
                    digest: [5; 32],
                    phase: OperationPhase::Succeeded,
                    outcome: Some(MutationOutcome::Created(machine)),
                })?,
            ),
        ])
    }
}

fn encoding(error: serde_json::Error) -> ProviderError {
    ProviderError::Rejected(format!(
        "Machine journal encoding failed: {error}; recovery: restore the original version-one records"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn version_refusal_precedes_journal_payload() -> Result<(), Box<dyn std::error::Error>> {
        let value = Operation {
            digest: [1; 32],
            phase: OperationPhase::Pending,
            outcome: None,
        };
        let encoded = encode(&value)?;
        let decoded: Operation = decode(&encoded)?;
        assert_eq!(encode(&decoded)?, encoded);
        let version_offset = b"{\"version\":\"".len();
        for version in [b'0', b'2', u8::MAX] {
            let mut rejected = encoded
                .get(..version_offset + 2)
                .ok_or("version header")?
                .to_vec();
            *rejected.get_mut(version_offset).ok_or("version")? = version;
            assert!(matches!(
                decode::<Operation>(&rejected),
                Err(ProviderError::Unsupported(_))
            ));
        }
        let mut trailing = encoded;
        trailing.extend_from_slice(b" false");
        assert!(matches!(
            decode::<Operation>(&trailing),
            Err(ProviderError::Rejected(_))
        ));
        Ok(())
    }
}
