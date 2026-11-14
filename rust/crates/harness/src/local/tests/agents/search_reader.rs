use super::*;
use crate::test_support::ScriptedTurn;
use acyclic_harness_client::ReadTarget;
use acyclic_stream::search::Cursor;
use std::num::NonZeroU32;

#[tokio::test]
async fn linked_search_keeps_the_original_session_input_after_later_turns() -> Result<()> {
    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let provider = super::super::scripted([
        ScriptedTurn::Text("first receipt".into()),
        ScriptedTurn::Text("later receipt".into()),
    ]);
    let harness = Arc::new(
        super::super::open_scripted(
            volume_config(directory.path(), project, provider)?.without_default_tools(),
        )
        .await?,
    );
    let mut conversation = ObservedConversation::open(&harness).await?;
    let input = "original linked specimen c2-reader";
    conversation.send("first-search-source", input).await?;
    let connection = harness.connect().await?;
    let root = connection.root().clone();
    let bound = harness.bound()?;
    let reader = bound.content_reader(&root).await.map_err(agent)?;
    let matcher = crate::history_search::ContentMatcher::new(reader.as_ref(), None);
    let streams = harness
        .agent_stream
        .as_ref()
        .ok_or_else(|| Error::NotFound("session stream".into()))?;
    let budget = NonZeroU32::new(2).ok_or_else(|| Error::Invalid("search work chunk".into()))?;
    let first = Cursor::new(root.inbox().map_err(agent)?, input.as_bytes().to_vec())
        .page_with(streams.as_ref(), budget, &matcher)
        .await?;
    let mut matches = first.matches;
    let next = first
        .next
        .ok_or_else(|| Error::NotFound("bounded search continuation".into()))?;
    let encoded = serde_json::to_vec(&next).map_err(agent)?;
    conversation.send("later-search-source", input).await?;
    let mut cursor: Option<Cursor> = Some(serde_json::from_slice(&encoded).map_err(agent)?);
    while let Some(selected) = cursor {
        let page = selected
            .page_with(streams.as_ref(), budget, &matcher)
            .await?;
        matches.extend(page.matches);
        cursor = page.next;
    }
    assert_eq!(
        matches.len(),
        1,
        "later turns changed the selected input cut"
    );
    let found = matches
        .pop()
        .ok_or_else(|| Error::NotFound("linked input".into()))?;
    assert_eq!(found.path, root.inbox().map_err(agent)?);
    let file = found
        .source
        .ok_or_else(|| Error::NotFound("original input file reference".into()))?;
    assert_eq!(
        Conversation::read(
            &connection,
            &root,
            ReadTarget::File {
                file: (*file).clone()
            }
        )
        .await?,
        crate::agent::encode_user_input(&crate::model::ModelContent::Text(input.into()))?,
    );
    Ok(())
}
