//! A JSONL projection of authenticated, committed usage, shared by frontends.
use super::*;
use crate::executor::{ModelPriceRecord, ModelUsageRecord};

type PriceLookup<'p> = dyn Fn(&ModelUsageRecord) -> Option<&'p ModelPriceRecord> + 'p;

fn emit(
    output: &mut dyn std::io::Write,
    row: &ModelUsageRecord,
    prices: &PriceLookup<'_>,
) -> Result<()> {
    let projection = row.estimate(prices(row)).map_err(failed)?;
    serde_json::to_writer(&mut *output, &projection).map_err(failed)?;
    output.write_all(b"\n").map_err(failed)
}

async fn write_agent(
    connection: &Connection,
    agent: &AgentPath,
    output: &mut dyn std::io::Write,
    prices: &PriceLookup<'_>,
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
                if pending.as_ref().is_none_or(|original| {
                    original.dispatch != row.dispatch
                        || original.purpose != row.purpose
                        || original.provider != row.provider
                        || original.model != row.model
                }) {
                    return Err(Error::Storage(
                        "usage completion differs from its original admission".into(),
                    ));
                }
                emit(output, &row, prices)?;
                pending = None;
            } else {
                if let Some(original) = pending.replace(row) {
                    emit(output, &original, prices)?;
                }
            }
        }
        if let Some(next) = page.next {
            after = next;
        } else {
            if let Some(original) = pending.take() {
                emit(output, &original, prices)?;
            }
            input = input
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("usage input continuation overflows".into()))?;
            after = 0;
        }
    }
    Ok(())
}

pub(super) async fn write_tree(
    connection: &Connection,
    agent: &AgentPath,
    output: &mut dyn std::io::Write,
    prices: &PriceLookup<'_>,
) -> Result<()> {
    let mut tree = super::tree::TreeWalk::new(agent);
    while let Some(agent) = tree.next(connection).await? {
        write_agent(connection, &agent, output, prices).await?;
    }
    Ok(())
}
