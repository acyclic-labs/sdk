//! A JSONL projection of authenticated, committed usage, shared by frontends.
use super::*;
use crate::executor::ModelUsageRecord;

fn emit(output: &mut dyn std::io::Write, row: &ModelUsageRecord) -> Result<()> {
    serde_json::to_writer(&mut *output, row).map_err(failed)?;
    output.write_all(b"\n").map_err(failed)
}

async fn write_agent(
    connection: &Connection,
    agent: &AgentPath,
    output: &mut dyn std::io::Write,
) -> Result<()> {
    let mut input = 0;
    let mut after = 0;
    let mut pending: Option<ModelUsageRecord> = None;
    loop {
        let AgentReply::Usage {
            page,
            input: admitted,
        } = connection
            .execute(&AgentCommand::Usage {
                agent: agent.clone(),
                input,
                after,
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
                emit(output, &row)?;
                pending = None;
            } else {
                if let Some(original) = pending.replace(row) {
                    emit(output, &original)?;
                }
            }
        }
        if let Some(next) = page.next {
            after = next;
        } else {
            if let Some(original) = pending.take() {
                emit(output, &original)?;
            }
            input = input
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("usage input continuation overflows".into()))?;
            after = 0;
        }
    }
    Ok(())
}

pub(super) fn write_tree<'a>(
    connection: &'a Connection,
    agent: &'a AgentPath,
    output: &'a mut dyn std::io::Write,
) -> futures::future::LocalBoxFuture<'a, Result<()>> {
    Box::pin(async move {
        write_agent(connection, agent, output).await?;
        let mut cursor = None;
        loop {
            let page = AgentPageRequest::new(32, 32 * 1024, cursor).map_err(failed)?;
            let AgentReply::Children { items, next } = connection
                .execute(&AgentCommand::Children {
                    agent: agent.clone(),
                    page,
                })
                .await?
            else {
                return Err(unexpected());
            };
            for child in items {
                write_tree(connection, &child.agent, output).await?;
            }
            let Some(next) = next else {
                return Ok(());
            };
            cursor = Some(next);
        }
    })
}
