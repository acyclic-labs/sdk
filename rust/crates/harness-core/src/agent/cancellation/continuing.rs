//! Goal-bound peer inputs use the original terminal-fact stream and key index.
use super::{Error, InputFact, input_events};
use crate::agent::{AgentPath, AgentRecord as _};
use acyclic_stream::{ReadKeySeek, ReadRequest, StreamError, StreamProvider};
use futures::TryStreamExt as _;
use std::collections::BTreeSet;

pub(super) fn key(agent: &AgentPath, run: crate::OperationId) -> Result<Vec<u8>, Error> {
    Ok(
        crate::agent::intent_key("goal-peer-inputs", &[agent.as_str(), &run.to_string()])?
            .as_bytes()
            .to_vec(),
    )
}

/// One bounded index traversal, proportional to this goal's accepted peer
/// inputs. Continuation turns without a peer input create no matching records.
pub(super) async fn inputs<P: StreamProvider + ?Sized>(
    provider: &P,
    agent: &AgentPath,
    run: crate::OperationId,
) -> Result<BTreeSet<u64>, Error> {
    let path = input_events(agent)?;
    let key = key(agent, run)?;
    let mut inputs = BTreeSet::new();
    let mut from = 0;
    let mut before = u64::MAX;
    let mut at_least = acyclic_stream::RevisionCut::default();
    loop {
        let opened = provider
            .read(ReadRequest {
                path: path.clone(),
                from,
                limit: super::super::MAX_QUEUED as u32,
                key: Some(ReadKeySeek::exact(key.clone(), before)),
                at_least: at_least.clone(),
            })
            .await;
        let mut records = match opened {
            Ok(records) => records,
            Err(error) if error.category() == StreamError::NotFound => return Ok(inputs),
            Err(error) => return Err(Error::Stream(error)),
        };
        at_least
            .observe(records.snapshot.clone())
            .map_err(Error::Stream)?;
        before = records
            .selection
            .end
            .ok_or(Error::Stream(StreamError::InvalidResponse))?;
        let mut count = 0;
        while let Some(record) = records.try_next().await.map_err(Error::Stream)? {
            if record.origin != path
                || !matches!(InputFact::decode(&record.value)?, InputFact::Goal { run: actual } if actual == run)
            {
                return Err(Error::Record("goal input index differs"));
            }
            for candidate in record.headers.keys() {
                if candidate.as_ref() == key.as_slice() {
                    continue;
                }
                let bytes: [u8; 8] = candidate
                    .as_ref()
                    .try_into()
                    .map_err(|_| Error::Record("goal input key differs"))?;
                inputs.insert(u64::from_be_bytes(bytes));
            }
            from = record
                .sequence
                .checked_add(1)
                .ok_or(Error::Record("goal input cursor overflow"))?;
            count += 1;
        }
        if count < super::super::MAX_QUEUED || from >= before {
            return Ok(inputs);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_stream::{
        CommitCondition, CommitEffect, CommitMutation, CommitRequest, MemoryStream,
        StreamProviderExt as _,
    };

    #[tokio::test]
    async fn goal_inputs_page_only_their_original_root_and_run()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider = MemoryStream::default();
        let agent = AgentPath::root(&crate::agent::AgentId::from_bytes([7; 16]));
        let forked = AgentPath::root(&crate::agent::AgentId::from_bytes([8; 16]));
        let run = crate::OperationId::from_bytes([9; 16]);
        let other = crate::OperationId::from_bytes([10; 16]);
        let path = input_events(&agent)?;
        let mut records = Vec::new();
        let expected = (0..=super::super::super::MAX_QUEUED as u64).collect::<BTreeSet<_>>();
        for input in &expected {
            for run in [run, other] {
                let input = if run == other {
                    *input + expected.len() as u64
                } else {
                    *input
                };
                records.extend(super::super::completion_records(
                    &InputFact::Goal { run },
                    BTreeSet::from([input]),
                    Some(key(&agent, run)?),
                )?);
            }
        }
        for (index, records) in records.chunks(acyclic_stream::MAX_ITEMS).enumerate() {
            let outcome = crate::agent::submit(
                &provider,
                CommitRequest {
                    idempotency_key: crate::agent::intent_key(
                        "goal-input-page",
                        &[&index.to_string()],
                    )?,
                    deadline_unix_millis: None,
                    effects: vec![CommitEffect::hard(
                        Vec::new(),
                        vec![CommitMutation::Append {
                            path: path.clone(),
                            records: records.to_vec(),
                        }],
                    )],
                },
            )
            .await?;
            assert!(matches!(outcome, crate::agent::Submitted::Committed(_)));
        }
        let counted = acyclic_stream::testing::Counted::new(provider.clone());
        assert_eq!(inputs(&counted, &agent, run).await?, expected);
        assert_eq!(
            counted.requests().data(),
            2,
            "unrelated goals must not spend page credit"
        );
        assert_eq!(counted.requests().head(), 0);
        let copied = input_events(&forked)?;
        let outcome = crate::agent::submit(
            &provider,
            CommitRequest {
                idempotency_key: crate::agent::intent_key("fork-goal-inputs", &[])?,
                deadline_unix_millis: None,
                effects: vec![CommitEffect::hard(
                    vec![CommitCondition::Absent {
                        path: copied.clone(),
                    }],
                    vec![CommitMutation::Fork {
                        source: path.clone(),
                        destination: copied.clone(),
                        at_tail: provider.tail(path.clone()).await?,
                        exclude: Vec::new(),
                        records: Vec::new(),
                    }],
                )],
            },
        )
        .await?;
        assert!(matches!(outcome, crate::agent::Submitted::Committed(_)));
        let mut inherited = provider
            .read(ReadRequest {
                path: copied,
                from: 0,
                limit: 1,
                key: Some(ReadKeySeek::exact(key(&agent, run)?, u64::MAX)),
                at_least: Default::default(),
            })
            .await?;
        assert_eq!(
            inherited
                .try_next()
                .await?
                .ok_or("inherited goal index missing")?
                .origin,
            path,
            "negative control must retain the ancestor's original index"
        );
        assert!(
            inputs(&provider, &forked, run).await?.is_empty(),
            "a history fork must not inherit its ancestor's reply obligations"
        );
        Ok(())
    }
}
