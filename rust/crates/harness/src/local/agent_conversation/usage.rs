//! Authenticated, committed usage, shared by frontends: one visitor over the
//! tree's calls, and its JSONL projection.
use super::*;
use crate::executor::{ModelPriceRecord, ModelUsageRecord};

type PriceLookup<'p> = dyn Fn(&ModelUsageRecord) -> Option<&'p ModelPriceRecord> + 'p;
/// Receives each original call once, in commit order: its completion when one
/// was committed, else its admission with unknown counters.
pub(super) type Visit<'v> = dyn FnMut(&ModelUsageRecord) -> Result<()> + 'v;

pub(super) async fn visit_agent(
    connection: &Connection,
    agent: &AgentPath,
    emit: &mut Visit<'_>,
) -> Result<()> {
    let mut input = 0;
    let mut after = 0;
    let mut pending: [Option<ModelUsageRecord>; 2] = [None, None];
    loop {
        let AgentReply::Usage {
            page,
            input: admitted,
            ..
        } = connection
            .execute(&AgentCommand::Usage {
                agent: agent.clone(),
                input,
                after,
                wait: false,
                observed_children: Some(0),
            })
            .await?
        else {
            return Err(unexpected());
        };
        input = admitted;
        let Some(page) = page else {
            break;
        };
        for row in page.items {
            visit_call(&mut pending, row, emit)?;
        }
        if let Some(next) = page.next {
            after = next;
        } else {
            for pending in &mut pending {
                if let Some(original) = pending.take() {
                    emit(&original)?;
                }
            }
            input = input
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("usage input continuation overflows".into()))?;
            after = 0;
        }
    }
    Ok(())
}

fn visit_call(
    pending: &mut [Option<ModelUsageRecord>; 2],
    row: ModelUsageRecord,
    emit: &mut Visit<'_>,
) -> Result<()> {
    let pending = &mut pending[super::usage_query::purpose_slot(row.purpose)];
    if row.completed {
        if pending
            .as_ref()
            .is_none_or(|original| !original.same_admission_as(&row))
        {
            return Err(Error::Storage(
                "usage completion differs from its original admission".into(),
            ));
        }
        emit(&row)?;
        *pending = None;
    } else if let Some(original) = pending.replace(row) {
        emit(&original)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interleaved_worker_and_restarted_summary_export_each_call_once() -> Result<()> {
        let operation = crate::OperationId::new();
        let mut pending = [None, None];
        let mut totals = crate::executor::ModelUsageTotals::unpriced();
        let mut rows = Vec::new();
        let mut emit = |row: &ModelUsageRecord| {
            totals
                .push(&row.estimate(None).map_err(failed)?)
                .map_err(failed)?;
            rows.push((row.purpose, row.dispatch.step, row.completed));
            Ok(())
        };
        use crate::executor::ModelPurpose::{Response, Summary};
        for (purpose, step, completed) in [
            (Response, 0, false),
            (Summary, 0, false),
            (Response, 0, true),
            (Summary, 1, false),
            (Summary, 1, true),
        ] {
            visit_call(
                &mut pending,
                ModelUsageRecord {
                    version: ModelUsageRecord::VERSION,
                    completed,
                    dispatch: crate::model::ModelDispatch {
                        operation_id: purpose.attempt_operation(operation, None)?,
                        step,
                        request_digest: [7; 32],
                    },
                    purpose,
                    provider: "test".into(),
                    model: "model".into(),
                    agent_path: None,
                    sampling: None,
                    max_output_tokens: None,
                    tokens: if completed {
                        crate::executor::ModelTokenUsage {
                            input: Some(3),
                            output: Some(2),
                            ..Default::default()
                        }
                    } else {
                        Default::default()
                    },
                    started_at: None,
                    completed_at: None,
                    latency_ns: None,
                    provider_request_id: None,
                    system_fingerprint: None,
                    ttft_ns: None,
                    cache: None,
                },
                &mut emit,
            )?;
        }
        assert_eq!(
            rows,
            [(Response, 0, true), (Summary, 0, false), (Summary, 1, true)]
        );
        assert_eq!((totals.calls, totals.completed), (3, 2));
        assert_eq!(totals.input.known, 6);
        assert_eq!(
            totals.input.value(),
            None,
            "a stopped call acquired guessed token usage"
        );
        Ok(())
    }
}

/// Every call of `agent` and its descendants, except the subtrees `prune`
/// names, one bounded page at a time. Only the pending calls and the active
/// traversal path are retained.
pub(super) async fn visit_tree(
    connection: &Connection,
    agent: &AgentPath,
    prune: &dyn Fn(&AgentPath) -> bool,
    visit: &mut Visit<'_>,
) -> Result<()> {
    let mut tree = super::tree::TreeWalk::new(agent);
    while let Some(agent) = tree.next(connection).await? {
        if prune(&agent) {
            tree.skip();
            continue;
        }
        visit_agent(connection, &agent, visit).await?;
    }
    Ok(())
}

pub(super) async fn write_tree(
    connection: &Connection,
    agent: &AgentPath,
    output: &mut dyn std::io::Write,
    prices: &PriceLookup<'_>,
) -> Result<()> {
    visit_tree(connection, agent, &|_| false, &mut |row| {
        let projection = row.estimate(prices(row)).map_err(failed)?;
        serde_json::to_writer(&mut *output, &projection).map_err(failed)?;
        output.write_all(b"\n").map_err(failed)
    })
    .await
}
