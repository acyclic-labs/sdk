//! One visible page on the selected conversation's authenticated connection.
use super::{Conversation, Failure, Line, diff};
use acyclic_harness::{client::AgentPageRequest, tui::QueryPage};
use std::sync::Arc;

#[derive(Clone)]
pub(super) enum Source {
    Diff(Option<String>),
    History(Option<String>),
}

impl Source {
    pub(super) fn title(&self) -> &'static str {
        match self {
            Self::Diff(_) => "Diff",
            Self::History(_) => "History",
        }
    }

    pub(super) fn newest(&self) -> Self {
        match self {
            Self::Diff(_) => Self::Diff(None),
            Self::History(_) => Self::History(None),
        }
    }

    pub(super) fn follows_tail(&self) -> bool {
        matches!(self, Self::Diff(None) | Self::History(None))
    }
}

pub(super) type Panel = QueryPage<Source>;

/// The existing session owns the wait. Closing or replacing the selection
/// drops that consumer; input and redraw keep the same pending future.
pub(super) async fn load(
    conversation: Arc<Conversation>,
    rows: u32,
    source: Source,
) -> Result<Panel, Failure> {
    match source {
        Source::Diff(cursor) => diff::query(conversation, rows, cursor).await,
        Source::History(cursor) => {
            let continued = cursor.is_some();
            let history = conversation
                .history_page(AgentPageRequest::new(rows, 64 * 1024, cursor)?)
                .await?;
            let mut lines = Vec::with_capacity(history.items.len() + 2);
            if history.items.is_empty() {
                lines.push(Line::raw("No settled history"));
            }
            for item in history.items {
                lines.push(Line::raw(format!(
                    "Cut {} · {} project volumes · context through {}",
                    item.conversation, item.volume_count, item.context_tail,
                )));
            }
            lines.push(Line::raw("PgDn: older · PgUp: newest · Esc: close"));
            Ok(Panel::new(
                lines,
                history.next.map(|cursor| Source::History(Some(cursor))),
                continued,
            ))
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
        };
        Panel::new(vec![Line::raw("original source row")], Some(next), false)
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
