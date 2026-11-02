//! One codec for retained Machine journal envelopes.
use super::*;

#[derive(Serialize, Deserialize)]
pub(super) struct Intent {
    pub(super) digest: [u8; 32],
}

#[derive(Serialize, Deserialize)]
pub(super) struct Completed<T = MutationOutcome> {
    pub(super) digest: [u8; 32],
    pub(super) outcome: T,
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
        check_type::<Intent>(bytes)
            && check_type::<Completed>(bytes)
            && check_type::<MachineObservation>(bytes)
    }

    /// Capture the shipped simulator's public create/fork/recovery outcomes
    /// through the production journal encoder. These are process-local
    /// simulation records, not physical guest or deployed transport captures.
    pub fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>, ProviderError> {
        use crate::{SimulatedMachines, simulation::MemoryIntent};

        futures::executor::block_on(async {
            let provider = SimulatedMachines::default();
            let key = IdempotencyKey::from_digest([5; 32]);
            let request = CreateMachine::new(key, Some(Image::custom([2; 32])?), [4; 32]);
            let digest = intent_digest(&MemoryIntent::Create(request.clone()))?;
            provider.create(request).await?;
            let created = provider.recover(key).await?;
            let MutationOutcome::Created(machine) = &created else {
                return Err(ProviderError::Rejected("Machine corpus create recovery has no original observation; recovery: run the original simulator create before capture".into()));
            };
            let fork_key = IdempotencyKey::from_digest([6; 32]);
            let count = NonZeroU32::MIN.saturating_add(1);
            let fork_digest = intent_digest(&MemoryIntent::ForkMachine(machine.id, count.get()))?;
            provider.fork_machine(machine.id, count, fork_key).await?;
            let forked = provider.recover(fork_key).await?;
            Ok(vec![
                ("machine", encode(machine)?),
                (
                    "result-observation",
                    crate::results::record(machine)?.value.to_vec(),
                ),
                ("pending", encode(&Intent { digest })?),
                (
                    "settled",
                    encode(&Completed {
                        digest,
                        outcome: created,
                    })?,
                ),
                (
                    "fork-pending",
                    encode(&Intent {
                        digest: fork_digest,
                    })?,
                ),
                (
                    "fork-settled",
                    encode(&Completed {
                        digest: fork_digest,
                        outcome: forked,
                    })?,
                ),
            ])
        })
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

    #[cfg(feature = "test-support")]
    #[test]
    fn original_fork_terminals_enter_the_permanent_corpus() -> Result<(), Box<dyn std::error::Error>>
    {
        let seeds = codec_fuzz::seeds()?;
        let mut created = false;
        let mut forked = false;
        for (name, bytes) in seeds {
            // The capture tool can retain these original producer bytes without
            // a second execution or a hand-written wire fixture.
            println!("machine_journal/{name}\t{}", hex::encode(&bytes));
            assert!(codec_fuzz::check(&bytes));
            if let Ok(terminal) = decode::<Completed>(&bytes) {
                for end in 0..bytes.len() {
                    assert!(
                        decode::<Completed>(&bytes[..end]).is_err(),
                        "partial original terminal at {end} was accepted"
                    );
                }
                match terminal.outcome {
                    MutationOutcome::Created(_) => created = true,
                    MutationOutcome::Forked { results, .. } => {
                        assert!(results.count > 1);
                        forked = true;
                    }
                    _ => {}
                }
            }
        }
        assert!(
            created,
            "the original create terminal must enter the corpus"
        );
        assert!(
            forked,
            "the original paged fork terminal must enter the corpus"
        );
        Ok(())
    }

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
