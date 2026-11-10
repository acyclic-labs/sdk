use super::*;
use crate::reader::{Driver, Priority};
use crate::{
    AgentCommand, AgentId, AgentPath, AgentRecord, IntentId, ModelContent, PendingApproval, Phase,
    Reader, ReaderPolicy, StatusRecord,
};
use acyclic_harness_core::{
    OperationId,
    agent::{ConversationEntry, ForkAdopted, InboxRecord, TurnInput, TurnOutcome},
    conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
    executor::TurnOutput,
    interaction::ApprovalBinding,
    resources::ProviderRef,
};
use acyclic_stream::{
    AppendRequest, IdempotencyKey, MemoryStream, StreamPath, StreamProviderExt as _,
};
use futures::future::poll_fn;
use std::{collections::BTreeMap, sync::Arc, task::Poll};
type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

fn agent(n: u128) -> AgentPath {
    AgentPath::root(&AgentId::from_bytes(n.to_be_bytes()))
}
fn status(phase: Phase) -> StatusRecord {
    StatusRecord {
        child_settlement: None,
        child_wakes: None,
        phase,
        cursor: 1,
        conversation: 0,
    }
}
async fn append(memory: &MemoryStream, path: StreamPath, bytes: Vec<u8>) -> TestResult {
    memory
        .append(AppendRequest {
            path,
            records: vec![bytes.into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    Ok(())
}
fn file(
    owner: &AgentPath,
    path: &str,
    bytes: &[u8],
    media: &str,
) -> std::result::Result<FileRef, Box<dyn std::error::Error>> {
    Ok(FileRef::new(
        VolumeRef::new(
            ProviderRef::new("acyclic".into(), "filesystem".into(), "2".into(), &mut None)?,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(acyclic_harness_core::AgentId::from_bytes(
                owner.id().to_bytes(),
            )),
            &mut None,
        )?,
        path.into(),
        "v1".into(),
        FileDescriptor::from_bytes(bytes, media, &mut None)?,
        path.rsplit('/').next().unwrap_or(path).into(),
        &mut None,
    )?)
}
async fn wait(
    reader: &mut Reader,
    driver: &mut Driver<MemoryStream>,
    ready: impl Fn(&Reader) -> bool,
) {
    poll_fn(|cx| {
        driver.step(reader, 0, cx);
        if ready(reader) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
}
async fn idle(memory: &MemoryStream, path: &AgentPath) -> TestResult {
    let adoption = InboxRecord::ForkAdopted(Box::new(ForkAdopted {
        agent: path.id(),
        parent: None,
        profile: None,
        forked_at: 0,
        conversation_tail: 0,
        checkpoint: None,
        volumes: Vec::new(),
        brief: None,
        policy: None,
    }));
    append(memory, StreamPath::new(path.as_str())?, adoption.encode()?).await
}

#[tokio::test]
async fn ui_default_projection() -> TestResult {
    let memory = Arc::new(MemoryStream::default());
    let selected = agent(1);
    idle(&memory, &selected).await?;
    append(&memory, selected.status()?, status(Phase::Idle).encode()?).await?;
    let mut files = BTreeMap::new();
    for n in 0..5 {
        let output = serde_json::to_string(&TurnOutput::new(
            format!("Answer {n}"),
            serde_json::Value::Null,
            1,
        ))?;
        let reference = file(
            &selected,
            &format!("turns/{n}.json"),
            output.as_bytes(),
            "application/json",
        )?;
        files.insert(reference.path().to_owned(), output);
        let entry = ConversationEntry {
            inbox: n,
            input: TurnInput::Brief {
                brief: Box::new(reference.clone()),
            },
            output: reference,
            outcome: TurnOutcome::Completed,
            goal: None,
            targets: None,
            tool_results: None,
            steering: None,
            notices: None,
        };
        append(&memory, selected.conversation()?, entry.encode()?).await?;
    }
    for n in 10..16 {
        let child = selected.child(&agent(n).id())?;
        append(&memory, StreamPath::new(child.as_str())?, b"child".to_vec()).await?;
    }
    let view = View::new(2)?;
    let mut reader = Reader::new(ReaderPolicy {
        listing_limit: 2,
        ..ReaderPolicy::default()
    })?;
    let mut driver = Driver::new(memory);
    driver.attach(&mut reader, 0);
    let index = UiPath::new("index.md")?;
    reader.plan(
        view.wants(&selected, &index, None, 0)?
            .into_iter()
            .map(|w| (w, Priority::Visible)),
    );
    wait(&mut reader, &mut driver, |r| {
        r.cache()
            .record(&selected.conversation().expect("conversation"), 4)
            .is_some()
    })
    .await;
    let lookup = |f: &FileRef| files.get(f.path()).cloned();
    let cut = ReaderCut {
        reader: &reader,
        agent: &selected,
        file: &lookup,
    };
    assert_eq!(view.project(&cut, &index)?, include_str!("golden/index.md"));
    assert_eq!(view.project(&cut, &UiPath::new("status.md")?)?, "Idle\n");
    assert_eq!(
        view.bytes(&cut, &index, 2..17)?,
        view.project(&cut, &index)?
            .as_bytes()
            .get(2..17)
            .ok_or("range")?
    );
    let children = UiPath::new("children")?;
    reader.begin_frame(0);
    reader.plan(
        view.wants(&selected, &children, Some(5), 0)?
            .into_iter()
            .map(|w| (w, Priority::Visible)),
    );
    wait(&mut reader, &mut driver, |r| {
        r.cache()
            .listing(&selected.children().expect("children"), 0)
            .is_some()
    })
    .await;
    let cut = ReaderCut {
        reader: &reader,
        agent: &selected,
        file: &lookup,
    };
    assert_eq!(
        view.project(&cut, &children)?,
        include_str!("golden/children.md")
    );
    let first = view.nodes(&cut, &children, 0, 2)?;
    assert_eq!(first.nodes.len(), 2);
    let partial = view.nodes(&cut, &children, 0, 1)?;
    let within = partial.next.ok_or("in-page continuation")?;
    assert!(
        view.wants(&selected, &children, Some(5), within)?
            .iter()
            .any(|want| matches!(want, crate::reader::Want::Listing { index: 0, .. }))
    );
    assert_eq!(
        view.nodes(&cut, &children, within, 1)?.nodes,
        first.nodes[1..]
    );
    let cursor = first.next.ok_or("next page")?;
    let issued = reader.issued();
    reader.begin_frame(0);
    reader.plan(
        view.wants(&selected, &children, Some(5), cursor)?
            .into_iter()
            .map(|w| (w, Priority::Visible)),
    );
    wait(&mut reader, &mut driver, |r| {
        r.cache()
            .listing(&selected.children().expect("children"), 1)
            .is_some()
    })
    .await;
    assert_eq!(
        reader.issued() - issued,
        1,
        "page two must issue one children_page"
    );
    let cut = ReaderCut {
        reader: &reader,
        agent: &selected,
        file: &lookup,
    };
    assert_eq!(view.nodes(&cut, &children, cursor, 2)?.nodes.len(), 2);
    let page_path = UiPath::new("children/pages/1.md")?;
    let page = view.project(&cut, &page_path)?;
    let base = url::Url::parse("https://ui.invalid/children/pages/1.md")?;
    let links = pulldown_cmark::Parser::new(&page)
        .filter_map(|event| match event {
            pulldown_cmark::Event::Start(pulldown_cmark::Tag::Link { dest_url, .. }) => {
                Some(base.join(&dest_url))
            }
            _ => None,
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let nodes = view.nodes(&cut, &children, cursor, 2)?.nodes;
    for (link, node) in links.iter().zip(&nodes) {
        assert_eq!(link.path(), format!("/children/{}/", node.name));
    }
    let next = links.last().ok_or("continuation link")?;
    let next = UiPath::new(next.path().trim_start_matches('/'))?;
    assert!(
        view.wants(&selected, &next, Some(5), 0)?
            .iter()
            .any(|want| { matches!(want, crate::reader::Want::Listing { index: 2, .. }) })
    );
    assert_eq!(view.affected(Affected::Status), &["index.md", "status.md"]);
    Ok(())
}

#[test]
fn ui_agent_override() -> TestResult {
    let path = UiPath::new("index.md")?;
    let mut files = BTreeMap::new();
    files.insert(
        path.clone(),
        "# My plan\n\n~~~ui:default\n~~~\n\n[Plan](plan.md)\n".to_owned(),
    );
    files.insert(UiPath::new("plan.md")?, "Ship the change".to_owned());
    let mut calls = 0;
    let result = path.resolve(files.get(&path).map(String::as_str), || {
        calls += 1;
        Ok("Default".into())
    })?;
    assert_eq!(result, "# My plan\n\nDefault\n\n[Plan](plan.md)\n");
    assert_eq!(calls, 1);
    assert_eq!(
        UiPath::new("plan.md")?.resolve(
            files.get(&UiPath::new("plan.md")?).map(String::as_str),
            || Err(Error::Pending)
        )?,
        "Ship the change"
    );
    files.remove(&path);
    assert_eq!(
        path.resolve(files.get(&path).map(String::as_str), || Ok(
            "Default\n".into()
        ))?,
        "Default\n"
    );
    assert_eq!(
        path.resolve(Some("````markdown\n```ui:default\n```\n````\n"), || Err(
            Error::Pending
        ))?,
        "````markdown\n```ui:default\n```\n````\n"
    );
    assert!(UiPath::new("../index.md").is_err());
    assert!(UiPath::new("/index.md").is_err());
    Ok(())
}

#[test]
fn ui_reply_roundtrip_with_unknown_kind_and_approval() -> TestResult {
    for kind in ["choose", "future-widget"] {
        let blocks = blocks::parse(&format!(
            "```input {kind} id=approach recommended=b\n- a\n- b\n```\n\n~~~input text id=notes\nDetails?\n~~~\n"
        ))?;
        let answers = BTreeMap::from([
            ("approach".into(), vec!["b".into()]),
            ("notes".into(), vec!["Details".into()]),
        ]);
        let expected = ReplyPayload {
            ui: "ask.md".into(),
            generation: "cut-7".into(),
            answers: answers.clone(),
            text: "Also keep this".into(),
        };
        let command = Reply {
            path: UiPath::new("ask.md")?,
            generation: expected.generation.clone(),
            answers,
            text: expected.text.clone(),
        }
        .command(agent(1), IntentId::try_from("reply-1")?, &blocks)?;
        let AgentCommand::Prompt {
            agent: recipient,
            id,
            body: ModelContent::Text(body),
        } = command
        else {
            return Err("expected one prompt".into());
        };
        assert_eq!(recipient, agent(1));
        assert_eq!(id.as_str(), "reply-1");
        assert_eq!(serde_json::from_str::<ReplyPayload>(&body)?, expected);
        assert_eq!(blocks.first().ok_or("block")?.kind, kind);
    }
    let pending = PendingApproval {
        id: IntentId::try_from("approval-1")?,
        status: 7,
        binding: ApprovalBinding {
            operation_id: OperationId::from_bytes([3; 16]),
            action_digest: [9; 32],
        },
    };
    let blocks =
        blocks::parse("```input choose id=approval approval=approval-1\n- allow\n- deny\n```\n")?;
    let block = blocks.first().ok_or("approval block")?;
    for approved in [true, false] {
        let command = approval_reply(agent(1), &pending, block, approved, Some("human".into()))?;
        let AgentCommand::Decide {
            id,
            binding,
            approved: decision,
            ..
        } = command
        else {
            return Err("expected Decide".into());
        };
        assert_eq!(id, pending.id);
        assert_eq!(binding, pending.binding);
        assert_eq!(decision, approved);
    }
    assert!(
        Reply {
            path: UiPath::new("status.md")?,
            generation: "7".into(),
            answers: BTreeMap::new(),
            text: "allow".into()
        }
        .command(agent(1), IntentId::try_from("reply-2")?, &blocks)
        .is_err()
    );
    let wrong = PendingApproval {
        id: IntentId::try_from("another")?,
        ..pending
    };
    assert!(approval_reply(agent(1), &wrong, block, true, None).is_err());
    Ok(())
}

#[test]
fn ui_blocks_are_generic_commonmark_and_inert() -> TestResult {
    let source = "````markdown\n```input fake id=hidden\nno\n```\n````\n\n   ~~~input slider id=amount min=0 max=100\n42\n   ~~~\n\n```input pick-fork id=branch\n<script>bad()</script> [link](https://invalid/)\n```\n";
    let blocks = blocks::parse(source)?;
    assert_eq!(blocks.len(), 2);
    let slider = blocks.first().ok_or("slider")?;
    assert_eq!(slider.kind, "slider");
    assert_eq!(slider.body, "42\n");
    assert_eq!(
        slider.attributes.get("max").map(String::as_str),
        Some("100")
    );
    let future = blocks.last().ok_or("future")?;
    assert!(future.body.contains("<script>"));
    assert!(
        source
            .get(future.range.clone())
            .ok_or("source range")?
            .starts_with("```input")
    );
    assert!(blocks::parse("```input choose id=x id=y\n```\n").is_err());
    assert!(matches!(
        blocks::parse(&format!(
            "```input x {}\n```",
            "a".repeat(blocks::MAX_HEADER_BYTES)
        )),
        Err(Error::Capacity)
    ));
    Ok(())
}

#[tokio::test]
async fn ui_lazy_cost() -> TestResult {
    async fn cost(n: u128) -> std::result::Result<u64, Box<dyn std::error::Error>> {
        let memory = Arc::new(MemoryStream::default());
        for id in 1..=n {
            idle(&memory, &agent(id)).await?;
        }
        let selected = agent(1);
        append(&memory, selected.status()?, status(Phase::Idle).encode()?).await?;
        let output = serde_json::to_string(&TurnOutput::new(
            "Resident turn".into(),
            serde_json::Value::Null,
            1,
        ))?;
        let reference = file(
            &selected,
            "turns/0.json",
            output.as_bytes(),
            "application/json",
        )?;
        let entry = ConversationEntry {
            inbox: 0,
            input: TurnInput::Brief {
                brief: Box::new(reference.clone()),
            },
            output: reference.clone(),
            outcome: TurnOutcome::Completed,
            goal: None,
            targets: None,
            tool_results: None,
            steering: None,
            notices: None,
        };
        append(&memory, selected.conversation()?, entry.encode()?).await?;
        let view = View::new(16)?;
        let mut reader = Reader::new(ReaderPolicy::default())?;
        assert_eq!(reader.issued(), 0, "no viewer has no operations");
        let mut driver = Driver::new(memory.clone());
        driver.attach(&mut reader, 0);
        reader.plan(
            view.wants(&selected, &UiPath::new("index.md")?, None, 0)?
                .into_iter()
                .map(|w| (w, Priority::Visible)),
        );
        wait(&mut reader, &mut driver, |r| {
            r.cache()
                .record(&selected.status().expect("status"), 0)
                .is_some()
                && r.cache()
                    .record(&selected.conversation().expect("conversation"), 0)
                    .is_some()
        })
        .await;
        let lookup = |file: &FileRef| (file == &reference).then(|| output.clone());
        let cut = ReaderCut {
            reader: &reader,
            agent: &selected,
            file: &lookup,
        };
        assert!(
            view.project(&cut, &UiPath::new("index.md")?)?
                .contains("Resident turn")
        );
        let issued = reader.issued();
        let before = memory.head(selected.status()?).await?.tail;
        memory
            .append(AppendRequest {
                path: selected.status()?,
                records: vec![
                    status(Phase::WaitingUser {
                        path: "ask.md".into(),
                    })
                    .append_record()?,
                ],
                if_tail: Some(before),
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
        assert_eq!(memory.head(selected.status()?).await?.tail, before + 1);
        Ok(issued)
    }
    assert_eq!(cost(10).await?, cost(100_000).await?);
    Ok(())
}

#[test]
fn ui_attention_uses_followed_status_and_authored_queue() {
    let (a, b, c) = (agent(1), agent(2), agent(3));
    let (ask, idle, done) = (
        status(Phase::WaitingUser {
            path: "ask.md".into(),
        }),
        status(Phase::Idle),
        status(Phase::Done {
            outcome: acyclic_harness_core::agent::AgentOutcome::Succeeded { result: None },
        }),
    );
    let visible = [(&a, &ask), (&b, &idle), (&c, &done)];
    assert_eq!(attention(visible, None), vec![&a, &c]);
    assert_eq!(
        attention(
            visible,
            Some(&format!(
                "- [first]({})\n- [duplicate]({})",
                c.as_str(),
                c.as_str()
            ))
        ),
        vec![&c, &a]
    );
}
