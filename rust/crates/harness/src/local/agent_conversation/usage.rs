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
    let mut pending: Option<ModelUsageRecord> = None;
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
                pending = None;
            } else if let Some(original) = pending.replace(row) {
                emit(&original)?;
            }
        }
        if let Some(next) = page.next {
            after = next;
        } else {
            if let Some(original) = pending.take() {
                emit(&original)?;
            }
            input = input
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("usage input continuation overflows".into()))?;
            after = 0;
        }
    }
    Ok(())
}

/// Every call of `agent` and its descendants, except the subtrees `prune`
/// names, one bounded page at a time. Only the pending call and the active
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
