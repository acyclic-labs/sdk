//! Rendering only: selection and inspection are shared harness queries.
use super::{Connection, OperationId, args, headless};
use acyclic_harness::{
    agent::AgentPath,
    local::agent_conversation::{Conversation, InspectQuery},
};
use futures::TryStreamExt as _;

pub(super) async fn run(
    connection: Connection,
    options: &args::Inspect,
    agent: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let session = acyclic_harness::local::sessions::select(&connection, &options.session).await?;
    let selected = if let Some(agent) = agent {
        let agent = AgentPath::try_from(agent.to_owned())?;
        if !session.root().contains(&agent) {
            return Err("inspect agent must belong to the selected session".into());
        }
        session.select(agent).await?
    } else {
        session
    };
    let task = OperationId::from_bytes(selected.root().id_bytes());
    let conversation = Conversation::open(selected);
    let mut output = headless::Output::new(acyclic_native_runtime::InheritedOutput::stdout()?);
    if options.children {
        let page = acyclic_harness::client::AgentPageRequest::new(
            options.page.page_size,
            options.page.page_bytes,
            options.page.cursor.clone(),
        )?;
        headless::write_event(&mut output, task, &conversation.children_page(page).await?).await?;
        return Ok(());
    }
    if options.tree {
        let queries = conversation.inspect_tree();
        futures::pin_mut!(queries);
        while let Some(mut query) = queries.try_next().await? {
            write_initial(&mut query, &mut output, task).await?;
        }
        return Ok(());
    }
    let mut query = conversation.inspect().await?;
    write_initial(&mut query, &mut output, task).await?;
    if options.follow {
        loop {
            if query.advance().await? {
                headless::write_event(&mut output, task, &query.snapshot()).await?;
            }
        }
    }
    Ok(())
}

async fn write_initial(
    query: &mut InspectQuery,
    output: &mut headless::Output,
    task: OperationId,
) -> Result<(), Box<dyn std::error::Error>> {
    while !query.snapshot().synchronized {
        query.advance().await?;
    }
    headless::write_event(output, task, &query.snapshot()).await?;
    Ok(())
}
