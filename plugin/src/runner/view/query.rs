//! Visible query selection over the connection's original authenticated pool.
use super::{Conversation, Failure, Line, diff};
use acyclic_harness::{
    agent::{AgentPath, IntentId},
    interaction::ApprovalBinding,
};
use acyclic_harness::{
    client::AgentPageRequest,
    tui::{QueryPage, display_text, display_text_prefix},
};
use std::sync::Arc;

#[derive(Clone)]
pub(super) enum Source {
    Diff(Option<String>),
    History(Option<String>),
    AgentDiff(AgentPath, Option<String>),
    Wait(AgentPath),
    Merge(AgentPath, String),
    Discard(AgentPath, String),
    Approval {
        request: IntentId,
        binding: ApprovalBinding,
        status: Option<u64>,
        offset: u64,
    },
}

impl Source {
    pub(super) fn title(&self) -> &'static str {
        match self {
            Self::Diff(_) | Self::AgentDiff(..) => "Diff",
            Self::Wait(_) => "Wait",
            Self::Merge(..) => "Merge",
            Self::Discard(..) => "Discard",
            Self::History(_) => "History",
            Self::Approval { .. } => "Approval request",
        }
    }

    pub(super) fn newest(&self) -> Self {
        match self {
            Self::Diff(_) => Self::Diff(None),
            Self::AgentDiff(agent, _) => Self::AgentDiff(agent.clone(), None),
            Self::Wait(agent) => Self::Wait(agent.clone()),
            Self::Merge(agent, operation) => Self::Merge(agent.clone(), operation.clone()),
            Self::Discard(agent, operation) => Self::Discard(agent.clone(), operation.clone()),
            Self::History(_) => Self::History(None),
            Self::Approval {
                request,
                binding,
                status,
                ..
            } => Self::Approval {
                request: request.clone(),
                binding: binding.clone(),
                status: *status,
                offset: 0,
            },
        }
    }

    pub(super) fn follows_tail(&self) -> bool {
        matches!(
            self,
            Self::Diff(None) | Self::AgentDiff(_, None) | Self::History(None)
        )
    }
}

#[derive(Default)]
pub(super) struct Context {
    pub approval_status: Option<u64>,
    history: Vec<u64>,
}

pub(super) type Panel = QueryPage<Source, Context>;

impl Context {
    /// Select by the immutable cut printed on screen, never by a moving row.
    pub(super) fn history_turn(&self, cut: u64) -> Option<u64> {
        if !self.history.contains(&cut) {
            return None;
        }
        cut.checked_sub(1)
    }
}

pub(super) async fn load(
    conversation: Arc<Conversation>,
    root: Arc<Conversation>,
    rows: u32,
    source: Source,
) -> Result<Panel, Failure> {
    match source {
        Source::Diff(cursor) => Ok(diff::query(conversation, rows, cursor, None)
            .await?
            .with_context(Context::default())),
        Source::AgentDiff(agent, cursor) => {
            let selected = root.select(agent.clone()).await?;
            Ok(diff::query(selected, rows, cursor, Some(agent))
                .await?
                .with_context(Context::default()))
        }
        Source::Discard(agent, operation) => {
            let selected = root.select(agent).await?;
            acyclic_harness::local::sessions::archive(selected.connection(), &operation).await?;
            selected.wait_current_input().await?;
            Ok(QueryPage::new(
                vec![
                    Line::raw("Discarded · agent archived without merging"),
                    Line::raw("Esc: close"),
                ],
                None,
                false,
            )
            .with_context(Context::default()))
        }
        Source::Merge(agent, operation) => {
            let selected = root.select(agent).await?;
            let commit =
                acyclic_harness::local::sessions::merge(selected.connection(), &operation).await?;
            Ok(QueryPage::new(
                vec![
                    Line::raw(match commit {
                        Some(commit) => format!("Merged · {commit}"),
                        None => "No changes to merge".into(),
                    }),
                    Line::raw("Esc: close"),
                ],
                None,
                false,
            )
            .with_context(Context::default()))
        }
        Source::Wait(agent) => {
            let selected = root.select(agent).await?;
            let phase = selected.wait_current_input().await?;
            Ok(QueryPage::new(
                vec![
                    Line::raw(format!("Captured inputs settled · {phase:?}")),
                    Line::raw("Esc: close"),
                ],
                None,
                false,
            )
            .with_context(Context::default()))
        }
        Source::History(cursor) => {
            let continued = cursor.is_some();
            let history = conversation
                .history_page(AgentPageRequest::new(rows, 64 * 1024, cursor)?)
                .await?;
            let mut lines = Vec::with_capacity(history.items.len() + 2);
            if history.items.is_empty() {
                lines.push(Line::raw("No settled history"));
            }
            for item in &history.items {
                lines.push(Line::raw(format!(
                    "{} · {} project volumes · context through {}",
                    item.conversation, item.volume_count, item.context_tail,
                )));
            }
            let selected = history.items.iter().map(|item| item.conversation).collect();
            lines.push(Line::raw(
                "Enter a displayed cut to select · PgDn: older · PgUp: newest · Esc: close",
            ));
            Ok(QueryPage::new(
                lines,
                history.next.map(|cursor| Source::History(Some(cursor))),
                continued,
            )
            .with_context(Context {
                approval_status: None,
                history: selected,
            }))
        }
        Source::Approval {
            request,
            binding,
            status,
            offset,
        } => {
            let status = match status {
                Some(status) => status,
                None => {
                    let pending = conversation
                        .pending_approval(&request)
                        .await?
                        .filter(|pending| pending.binding == binding)
                        .ok_or("The selected approval is no longer pending.")?;
                    pending.status
                }
            };
            let (bytes, length) = conversation.approval_request(status, offset).await?;
            let range_end = offset
                .checked_add(bytes.len() as u64)
                .ok_or("Approval range overflows.")?;
            // A byte page can end inside a character. Leave that suffix at the
            // original offset for the next range; never replace or lose bytes.
            let text = display_text_prefix(&bytes, range_end == length)?;
            let end = offset
                .checked_add(text.len() as u64)
                .ok_or("Approval range overflows.")?;
            if end == offset && end < length {
                return Err("Approval range made no progress.".into());
            }
            let mut lines = Vec::new();
            lines.push(Line::raw(format!(
                "Request {} · bytes {offset}–{end} of {length}",
                display_text(request.as_str())
            )));
            lines.extend(
                text.lines()
                    .map(|line| Line::raw(display_text(line).into_owned())),
            );
            lines.push(Line::raw(
                "PgDn: more · PgUp: start · y: approve · n: decline · Esc: close",
            ));
            Ok(QueryPage::new(
                lines,
                (end < length).then_some(Source::Approval {
                    request,
                    binding,
                    status: Some(status),
                    offset: end,
                }),
                offset != 0,
            )
            .with_context(Context {
                approval_status: Some(status),
                history: Vec::new(),
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_harness::tui::QueryController;
    use std::{cell::Cell, task::Poll};
    use tokio::sync::oneshot;

    // A real channel supplies reply readiness, cancellation and producer loss.
    // This async wrapper also makes the original read !Unpin, like load().
    async fn read(receiver: oneshot::Receiver<Panel>) -> Result<Panel, oneshot::error::RecvError> {
        receiver.await
    }

    fn page(source: &Source) -> Panel {
        let next = match source {
            Source::Diff(_) => Source::Diff(Some("original-diff-cut".into())),
            Source::History(_) => Source::History(Some("original-history-cut".into())),
            Source::AgentDiff(agent, _) => {
                Source::AgentDiff(agent.clone(), Some("original-diff-cut".into()))
            }
            Source::Wait(_) | Source::Merge(..) | Source::Discard(..) => {
                return QueryPage::new(vec![], None, false).with_context(Context::default());
            }
            Source::Approval { .. } => source.clone(),
        };
        QueryPage::new(vec![Line::raw("original source row")], Some(next), false)
            .with_context(Context::default())
    }

    #[tokio::test]
    async fn query_controller_retains_reads_coalesces_turns_and_ends_on_original_events()
    -> Result<(), Failure> {
        for source in [Source::Diff(None), Source::History(None)] {
            let mut controller = QueryController::default();
            let mut pending = std::pin::pin!(None);
            let starts = Cell::new(0);
            controller.select(source.clone(), pending.as_mut());
            controller.start(0, pending.as_mut(), |_, _| {
                panic!("collapsed query started")
            });
            let (producer, receiver) = oneshot::channel();
            controller.start(8, pending.as_mut(), |_, _| {
                starts.set(starts.get() + 1);
                read(receiver)
            });
            let original = std::ptr::from_ref(pending.as_ref().get_ref().as_ref().unwrap());
            assert!(matches!(
                futures::poll!(pending.as_mut().as_pin_mut().unwrap()),
                Poll::Pending
            ));

            // Several commits and resizes while the original reply is pending
            // must share one refresh and preserve that original future in place.
            for rows in [1, 40, 0, 8] {
                controller.invalidate(Source::follows_tail);
                controller.start(rows, pending.as_mut(), |_, _| {
                    panic!("pending read replaced")
                });
                assert_eq!(
                    std::ptr::from_ref(pending.as_ref().get_ref().as_ref().unwrap()),
                    original
                );
                assert!(!producer.is_closed());
            }
            producer
                .send(page(&source))
                .map_err(|_| "original consumer dropped")?;
            let result = pending.as_mut().as_pin_mut().unwrap().await;
            controller.finish(result, pending.as_mut())?;
            let (producer, receiver) = oneshot::channel();
            controller.start(8, pending.as_mut(), |_, _| {
                starts.set(starts.get() + 1);
                read(receiver)
            });
            assert_eq!(starts.get(), 2, "commits coalesce into one refresh");
            producer
                .send(page(&source))
                .map_err(|_| "refresh consumer dropped")?;
            let result = pending.as_mut().as_pin_mut().unwrap().await;
            controller.finish(result, pending.as_mut())?;
            for columns in [12, 40, 0, 8] {
                controller.page.as_mut().unwrap().wrap(columns);
                controller.start(columns.into(), pending.as_mut(), |_, _| {
                    panic!("resize refetched source")
                });
            }

            // Selecting an older continuation changes the source exactly once.
            // Commits cannot turn that frozen selection into a tail query.
            let older = controller.page.as_mut().unwrap().next.take().unwrap();
            assert!(!older.follows_tail());
            controller.select(older.clone(), pending.as_mut());
            let (producer, receiver) = oneshot::channel();
            controller.start(8, pending.as_mut(), |_, _| read(receiver));
            controller.invalidate(Source::follows_tail);
            producer
                .send(page(&older))
                .map_err(|_| "older consumer dropped")?;
            let result = pending.as_mut().as_pin_mut().unwrap().await;
            controller.finish(result, pending.as_mut())?;
            controller.start(8, pending.as_mut(), |_, _| panic!("frozen cut refreshed"));

            // Escape cancels the same original channel whether its reply is
            // suspended or already ready. A ready result cannot reopen the page.
            for ready in [false, true] {
                controller.select(source.clone(), pending.as_mut());
                let (mut producer, receiver) = oneshot::channel();
                controller.start(8, pending.as_mut(), |_, _| read(receiver));
                if ready {
                    producer
                        .send(page(&source))
                        .map_err(|_| "ready consumer dropped")?;
                    controller.close(pending.as_mut());
                } else {
                    let closed = producer.closed();
                    let mut closed = std::pin::pin!(closed);
                    // Negative control: without consumer cancellation the
                    // original producer's closure event must remain pending.
                    assert!(matches!(futures::poll!(closed.as_mut()), Poll::Pending));
                    controller.close(pending.as_mut());
                    closed.await;
                }
                assert!(pending.is_none());
                assert!(controller.source().is_none());
                assert!(controller.page.is_none());
            }

            // Dropping the producer, rather than sending a reply, wakes the
            // same read with its typed error and prevents an implicit retry.
            controller.select(source.clone(), pending.as_mut());
            let (producer, receiver) = oneshot::channel();
            controller.start(8, pending.as_mut(), |_, _| read(receiver));
            assert!(matches!(
                futures::poll!(pending.as_mut().as_pin_mut().unwrap()),
                Poll::Pending
            ));
            drop(producer);
            let result = pending.as_mut().as_pin_mut().unwrap().await;
            assert!(controller.finish(result, pending.as_mut()).is_err());
            controller.start(8, pending.as_mut(), |_, _| panic!("failed source retried"));
            assert!(controller.source().is_none());
        }
        Ok(())
    }
}
