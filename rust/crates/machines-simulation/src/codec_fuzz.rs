//! Original simulator journal and result captures for the permanent corpus.

use super::*;
use acyclic_machines::{
    JournalCompleted, JournalIntent, intent_digest, journal_encode, results,
};

/// Capture the shipped simulator's public create/fork/recovery outcomes
/// through the production journal encoder. These are process-local
/// simulation records, not physical guest or deployed transport captures.
pub fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>, ProviderError> {
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
            ("simulator-v1-machine", acyclic_machines::journal_encode(machine)?),
            (
                "simulator-v1-result-observation",
                results::record(machine)?.value.to_vec(),
            ),
            (
                "simulator-v1-pending",
                journal_encode(&JournalIntent { digest })?,
            ),
            (
                "simulator-v1-settled",
                journal_encode(&JournalCompleted {
                    digest,
                    outcome: created,
                })?,
            ),
            (
                "simulator-v1-fork-pending",
                journal_encode(&JournalIntent {
                    digest: fork_digest,
                })?,
            ),
            (
                "simulator-v1-fork-settled",
                journal_encode(&JournalCompleted {
                    digest: fork_digest,
                    outcome: forked,
                })?,
            ),
        ])
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_machines::journal_decode;

    #[test]
    fn original_fork_terminals_enter_the_permanent_corpus() -> Result<(), Box<dyn std::error::Error>>
    {
        let seeds = seeds()?;
        let mut created = false;
        let mut forked = false;
        for (name, bytes) in seeds {
            // The capture tool can retain these original producer bytes without
            // a second execution or a hand-written wire fixture.
            println!("machine_journal/{name}\t{}", hex::encode(&bytes));
            assert!(acyclic_machines::codec_fuzz::check(&bytes));
            if let Ok(terminal) = journal_decode::<JournalCompleted>(&bytes) {
                for end in 0..bytes.len() {
                    assert!(
                        journal_decode::<JournalCompleted>(&bytes[..end]).is_err(),
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
}
