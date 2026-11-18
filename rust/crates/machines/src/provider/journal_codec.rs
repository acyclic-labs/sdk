//! One codec for retained Machine journal envelopes.
use super::*;

#[derive(Serialize, Deserialize)]
pub struct Intent {
    pub digest: [u8; 32],
}

#[derive(Serialize, Deserialize)]
pub struct Completed<T = MutationOutcome> {
    pub digest: [u8; 32],
    pub outcome: T,
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

pub fn encode(value: &impl Serialize) -> Result<Vec<u8>, ProviderError> {
    serde_json::to_vec(&Envelope {
        version: Version::V1,
        value,
    })
    .map_err(encoding)
}

pub fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, ProviderError> {
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
        check_type::<Intent>(bytes)
            && check_type::<Completed>(bytes)
            && check_type::<MachineObservation>(bytes)
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
        let value = Intent { digest: [1; 32] };
        let encoded = encode(&value)?;
        let decoded: Intent = decode(&encoded)?;
        assert_eq!(encode(&decoded)?, encoded);
        let version_offset = b"{\"version\":\"".len();
        for version in [b'0', b'2', u8::MAX] {
            let mut rejected = encoded
                .get(..version_offset + 2)
                .ok_or("version header")?
                .to_vec();
            *rejected.get_mut(version_offset).ok_or("version")? = version;
            assert!(matches!(
                decode::<Intent>(&rejected),
                Err(ProviderError::Unsupported(_))
            ));
        }
        let mut trailing = encoded;
        trailing.extend_from_slice(b" false");
        assert!(matches!(
            decode::<Intent>(&trailing),
            Err(ProviderError::Rejected(_))
        ));
        Ok(())
    }
}
