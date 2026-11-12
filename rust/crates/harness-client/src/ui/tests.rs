use super::*;
use crate::reader::{Driver, Priority, Want};
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
        cancelled_input: None,
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
    wants: Vec<Want>,
    ready: impl Fn(&Reader) -> bool,
) {
    poll_fn(|cx| {
        let generation = reader.generation();
        reader.begin_frame(0);
        reader.plan(wants.iter().cloned().map(|want| (want, Priority::Visible)));
        driver.step(reader, 0, cx);
        if ready(reader) {
            Poll::Ready(())
        } else {
            if reader.generation() != generation {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        }
    })
    .await;
}
async fn idle(memory: &MemoryStream, path: &AgentPath) -> TestResult {
    append(memory, path.membership()?, b"self membership".to_vec()).await?;
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
async fn terminal_diagnostic_is_discoverable_and_lazily_readable_without_losing_utf8() -> TestResult
{
    use acyclic_harness_core::agent::{AgentOutcome, Diagnostic};
    let memory = Arc::new(MemoryStream::default());
    let selected = agent(301);
    let original = format!(
        "{}é{}complete suffix",
        "x".repeat(65535),
        "\u{0001}\"🙂".repeat(18000)
    );
    let diagnostic = Diagnostic {
        agent: selected.clone(),
        digest: *blake3::hash(original.as_bytes()).as_bytes(),
        bytes: original.len() as u64,
    };
    let output = serde_json::to_string(&TurnOutput::new(
        "saved reply".into(),
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
        observed_through: 0,
        inbox: 0,
        input: TurnInput::Brief {
            brief: Box::new(reference.clone()),
        },
        output: reference,
        outcome: TurnOutcome::Completed,
        goal: None,
        announcements: Vec::new(),
        tool_results: None,
        steering: None,
        notices: None,
    };
    append(&memory, selected.conversation()?, entry.encode()?).await?;
    for chunk in original.as_bytes().chunks(acyclic_stream::MAX_RECORD_BYTES) {
        append(&memory, diagnostic.path()?, chunk.to_vec()).await?;
    }
    append(
        &memory,
        selected.status()?,
        status(Phase::Done {
            outcome: AgentOutcome::FailedDiagnostic {
                diagnostic: diagnostic.clone(),
            },
        })
        .encode()?,
    )
    .await?;
    let view = View::new(2)?;
    let mut reader = Reader::new(ReaderPolicy::default())?;
    let mut driver = Driver::new(memory);
    driver.attach(&mut reader, 0);
    let digest = blake3::Hash::from_bytes(diagnostic.digest)
        .to_hex()
        .to_string();
    // Opening the directory asks only for published status, never raw payload.
    let dir = UiPath::new(&format!("diagnostics/{digest}"))?;
    assert_eq!(view.wants(&selected, &dir, None, 0)?.len(), 1);
    let index = UiPath::new("index.md")?;
    wait(
        &mut reader,
        &mut driver,
        view.wants(&selected, &index, None, 0)?,
        |reader| {
            reader
                .cache()
                .record(&selected.conversation().expect("conversation"), 0)
                .is_some()
        },
    )
    .await;
    let lookup = |_: &FileRef| Some(output.clone());
    let mut complete = String::new();
    while complete.len() < original.len() {
        let from = complete.len() as u64;
        let path = UiPath::new(&format!("diagnostics/{digest}/{from}.md"))?;
        let wants = view.wants(&selected, &path, None, 0)?;
        assert!(matches!(&wants[1], Want::Range { from: start, to, .. } if to-start == 2));
        wait(&mut reader, &mut driver, wants, |reader| {
            let cut = ReaderCut {
                reader,
                agent: &selected,
                file: &lookup,
            };
            cut.status().is_ok() && cut.diagnostic(&diagnostic, from).is_ok()
        })
        .await;
        let cut = ReaderCut {
            reader: &reader,
            agent: &selected,
            file: &lookup,
        };
        let page = cut.diagnostic(&diagnostic, from)?;
        assert!(!page.text.is_empty());
        assert!(page.text.len() <= acyclic_stream::MAX_RECORD_BYTES);
        complete.push_str(&page.text);
        assert_eq!(page.end, complete.len() as u64);
        let markdown = view.project(&cut, &path)?;
        if page.end < page.total {
            assert!(markdown.contains(&format!("[Next diagnostic page]({}.md)", page.end)));
        }
        assert!(
            view.project(&cut, &UiPath::new("status.md")?)?
                .contains(&format!("(diagnostics/{digest}/0.md)"))
        );
        assert!(
            view.project(&cut, &index)?
                .contains(&format!("(diagnostics/{digest}/0.md)"))
        );
        assert!(
            view.project(&cut, &UiPath::new("turns/0.md")?)?
                .contains("[Current status](../status.md)")
        );
        let forged = UiPath::new(&format!("diagnostics/{}/0.md", "0".repeat(64)))?;
        assert!(view.project(&cut, &forged).is_err());
    }
    assert_eq!(complete, original);
    Ok(())
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
            observed_through: 0,
            inbox: n,
            input: TurnInput::Brief {
                brief: Box::new(reference.clone()),
            },
            output: reference,
            outcome: TurnOutcome::Completed,
            goal: None,
            announcements: Vec::new(),
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
    let mut driver = Driver::new(Arc::clone(&memory));
    driver.attach(&mut reader, 0);
    let index = UiPath::new("index.md")?;
    wait(
        &mut reader,
        &mut driver,
        view.wants(&selected, &index, None, 0)?,
        |r| {
            r.cache()
                .record(&selected.conversation().expect("conversation"), 4)
                .is_some()
        },
    )
    .await;
    let lookup = |f: &FileRef| files.get(f.path()).cloned();
    let cut = ReaderCut {
        reader: &reader,
        agent: &selected,
        file: &lookup,
    };
    assert_eq!(view.project(&cut, &index)?, include_str!("golden/index.md"));
    assert_eq!(view.project(&cut, &UiPath::new("status.md")?)?, "Idle\n");
    // Rows resolve exactly the authored UI cut over the same default files;
    // absence never invents a model, currency or creation clock.
    let row = view.row(&cut, |_| None)?;
    assert_eq!(
        (&*row.glyph, &*row.title, &*row.model, &*row.cost, &*row.age),
        ("○", selected.id().as_str(), "—", "—", "—")
    );
    let authored = BTreeMap::from([
        ("title", "Parser worker"),
        ("model", "selected-model"),
        ("cost", "0.12 EUR"),
        ("age", "2m"),
    ]);
    let row = view.row(&cut, |path| authored.get(path).copied())?;
    assert_eq!(
        (&*row.title, &*row.model, &*row.cost, &*row.age),
        ("Parser worker", "selected-model", "0.12 EUR", "2m")
    );
    assert!(title_matches(&row.title, "pWK"));
    assert!(!title_matches(&row.title, "wp"));
    assert_eq!(
        view.bytes(&cut, &index, 2..17)?,
        view.project(&cut, &index)?
            .as_bytes()
            .get(2..17)
            .ok_or("range")?
    );
    let children = UiPath::new("children")?;
    reader.begin_frame(0);
    wait(
        &mut reader,
        &mut driver,
        view.wants(&selected, &children, Some(5), 0)?,
        |r| {
            r.cache()
                .listing(&selected.children().expect("children"), 0)
                .is_some()
        },
    )
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
    wait(
        &mut reader,
        &mut driver,
        view.wants(&selected, &children, Some(5), cursor)?,
        |r| {
            r.cache()
                .listing(&selected.children().expect("children"), 1)
                .is_some()
        },
    )
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
    assert_eq!(
        view.affected(Affected::Status),
        &["index.md", "status.md", "glyph"]
    );
    // A real followed record supplies multibyte text. Byte ranges may split a
    // scalar, cross formatting writes, be reversed, or extend beyond EOF.
    reader.set_follow(selected.status()?, true);
    append(
        &memory,
        selected.status()?,
        status(Phase::WaitingUser {
            path: "réponse.md".into(),
        })
        .encode()?,
    )
    .await?;
    let tail = memory.head(selected.status()?).await?.tail;
    wait(&mut reader, &mut driver, Vec::new(), |r| {
        let stream = selected.status().expect("status");
        r.cache().tail(&stream) == Some(tail) && r.cache().record(&stream, tail - 1).is_some()
    })
    .await;
    let cut = ReaderCut {
        reader: &reader,
        agent: &selected,
        file: &lookup,
    };
    let path = UiPath::new("status.md")?;
    let expected = "Waiting for your reply: [réponse.md](réponse.md)\n";
    assert_eq!(view.project(&cut, &path)?, expected);
    for from in 0..expected.len() + 2 {
        for to in 0..expected.len() + 2 {
            let start = from.min(expected.len());
            let end = to.min(expected.len()).max(start);
            assert_eq!(
                view.bytes(&cut, &path, from..to)?,
                &expected.as_bytes()[start..end]
            );
        }
    }
    let mut storage = [0; 1];
    let error = view
        .write(
            &cut,
            &path,
            &mut std::io::Cursor::new(storage.as_mut_slice()),
        )
        .expect_err("caller output capacity must be respected");
    assert!(matches!(error, Error::Output(error) if error.kind() == std::io::ErrorKind::WriteZero));
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
            peer: None,
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
    for verdict in [
        acyclic_harness_core::interaction::ApprovalVerdict::Approve,
        acyclic_harness_core::interaction::ApprovalVerdict::ApproveRule,
        acyclic_harness_core::interaction::ApprovalVerdict::Deny,
    ] {
        let command = approval_reply(agent(1), &pending, block, verdict, Some("human".into()))?;
        let AgentCommand::Decide {
            id,
            binding,
            verdict: decision,
            ..
        } = command
        else {
            return Err("expected Decide".into());
        };
        assert_eq!(id, pending.id);
        assert_eq!(binding, pending.binding);
        assert_eq!(decision, verdict);
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
    assert!(approval_reply(agent(1), &wrong, block, true.into(), None).is_err());
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
            observed_through: 0,
            inbox: 0,
            input: TurnInput::Brief {
                brief: Box::new(reference.clone()),
            },
            output: reference.clone(),
            outcome: TurnOutcome::Completed,
            goal: None,
            announcements: Vec::new(),
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
        wait(
            &mut reader,
            &mut driver,
            view.wants(&selected, &UiPath::new("index.md")?, None, 0)?,
            |r| {
                r.cache()
                    .record(&selected.status().expect("status"), 0)
                    .is_some()
                    && r.cache()
                        .record(&selected.conversation().expect("conversation"), 0)
                        .is_some()
            },
        )
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
fn ui_attention_uses_followed_status() {
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
    assert_eq!(attention(visible), vec![&a, &c]);
}
#[tokio::test]
async fn ui_role_and_input_target_use_each_agents_own_fork_adoption() -> TestResult {
    use acyclic_harness_core::agent::SpawnContext;
    let memory = Arc::new(MemoryStream::default());
    let root = agent(100);
    let lead = root.child(&agent(101).id())?;
    let worker = lead.child(&agent(102).id())?;
    let adoption =
        |path: &AgentPath, parent: Option<AgentPath>, at, role: &str| -> TestResultBytes {
            let mut profile = acyclic_harness_core::templates::root()?;
            if parent.is_some() {
                profile.context = SpawnContext::Fresh;
                profile.template = Some(file(
                    path,
                    &format!("templates/{role}/AGENT.md"),
                    b"role",
                    "text/markdown",
                )?);
            }
            Ok(InboxRecord::ForkAdopted(Box::new(ForkAdopted {
                agent: path.id(),
                parent,
                profile: Some(profile),
                forked_at: at,
                conversation_tail: 0,
                checkpoint: None,
                volumes: Vec::new(),
                brief: None,
                policy: None,
            }))
            .encode()?)
        };
    append(
        &memory,
        root.inbox()?,
        adoption(&root, None, 0, "orchestrator")?,
    )
    .await?;
    for (parent, child, at, role) in [
        (&root, &lead, 1, "orchestrator"),
        (&lead, &worker, 2, "worker"),
    ] {
        memory
            .fork(acyclic_stream::ForkRequest {
                source: parent.inbox()?,
                destination: child.inbox()?,
                at_tail: Some(at),
                exclude: Vec::new(),
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
        append(
            &memory,
            child.inbox()?,
            adoption(child, Some(parent.clone()), at, role)?,
        )
        .await?;
    }
    let mut reader = Reader::new(ReaderPolicy::default())?;
    let mut driver = Driver::new(memory);
    driver.attach(&mut reader, 0);
    let agents = [&root, &lead, &worker];
    let wants = agents
        .iter()
        .map(|path| {
            Ok(Want::Last {
                stream: path.inbox()?,
                count: 1,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    wait(&mut reader, &mut driver, wants, |reader| {
        agents
            .iter()
            .all(|agent| agent_role(reader, agent).is_some())
    })
    .await;
    assert_eq!(agent_role(&reader, &worker).as_deref(), Some("worker"));
    assert_eq!(agent_role(&reader, &lead).as_deref(), Some("orchestrator"));
    assert_eq!(input_target(&reader, &worker), lead);
    assert_eq!(input_target(&reader, &root), root);
    Ok(())
}

type TestResultBytes = std::result::Result<Vec<u8>, Box<dyn std::error::Error>>;

#[tokio::test]
async fn role_filtered_empty_window_can_page_to_an_orchestrator_and_expand_contributors()
-> TestResult {
    use crate::explorer::{Explorer, Row};
    let memory = Arc::new(MemoryStream::default());
    let root = agent(200);
    idle(&memory, &root).await?;
    let children = (201..204)
        .map(|n| root.child(&agent(n).id()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    for (index, child) in children.iter().enumerate() {
        let mut profile = acyclic_harness_core::templates::root()?;
        let role = if index == 2 { "orchestrator" } else { "worker" };
        profile.template = Some(file(
            child,
            &format!("templates/{role}/AGENT.md"),
            b"role",
            "text/markdown",
        )?);
        let adoption = InboxRecord::ForkAdopted(Box::new(ForkAdopted {
            agent: child.id(),
            parent: Some(root.clone()),
            profile: Some(profile),
            forked_at: 0,
            conversation_tail: 0,
            checkpoint: None,
            volumes: Vec::new(),
            brief: None,
            policy: None,
        }));
        append(&memory, child.inbox()?, adoption.encode()?).await?;
        append(&memory, child.status()?, status(Phase::Idle).encode()?).await?;
    }
    let mut explorer = Explorer::children(ReaderPolicy::default(), &root)?;
    explorer.orchestrator_view(true);
    explorer.set_viewport(2, 0, 16);
    let mut driver = Driver::new(memory);
    driver.attach(explorer.reader(), 0);
    poll_fn(|cx| {
        let before = explorer.reader_ref().generation();
        explorer.frame(0);
        driver.step(explorer.reader(), 0, cx);
        if children
            .iter()
            .take(2)
            .all(|path| agent_role(explorer.reader_ref(), path).is_some())
        {
            explorer.frame(0);
            Poll::Ready(())
        } else {
            if explorer.reader_ref().generation() != before {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        }
    })
    .await;
    assert!(explorer.rows().is_empty());
    assert!(matches!(explorer.next_page(), Poll::Ready(true)));
    poll_fn(|cx| {
        let before = explorer.reader_ref().generation();
        explorer.frame(0);
        driver.step(explorer.reader(), 0, cx);
        if agent_role(explorer.reader_ref(), &children[2]).as_deref() == Some("orchestrator") {
            explorer.frame(0);
            Poll::Ready(())
        } else {
            if explorer.reader_ref().generation() != before {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        }
    })
    .await;
    assert!(matches!(&explorer.rows()[0].1, Row::Agent { path, .. } if path == &children[2]));
    explorer.page(false);
    explorer.frame(0);
    assert!(explorer.rows().is_empty());
    explorer.expand(&root);
    explorer.home();
    explorer.frame(0);
    assert_eq!(explorer.rows().len(), 2);
    Ok(())
}
