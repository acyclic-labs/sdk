//! Navigation owns only the original socket and prompts the user has requested.
use super::{Conversation, Failure};
use acyclic_harness::local::sessions::PromptHistory;
use futures::future::BoxFuture;
use std::{
    collections::VecDeque,
    sync::Arc,
    task::{Context, Poll},
};

type Loaded = Result<(PromptHistory, Option<String>), Failure>;

pub(super) struct Reply {
    pub request: u64,
    pub expected: String,
    pub text: Option<String>,
    pub previous: bool,
    before: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_harness::{
        LocalHarness,
        local::WorkspaceMounting,
        model::Model,
        test_support::{ScriptedModelProvider, ScriptedTurn, local_harness_config},
    };

    #[tokio::test]
    async fn queued_recall_preserves_original_text_and_the_unsent_draft() -> Result<(), Failure> {
        let directory = tempfile::tempdir()?;
        let workspace = directory.path().join("workspace");
        std::fs::create_dir(&workspace)?;
        let model = Arc::new(ScriptedModelProvider::new([
            ScriptedTurn::Text("answer".into()),
            ScriptedTurn::Text("none".into()),
        ]));
        let harness = Arc::new(
            LocalHarness::open(
                local_harness_config(
                    &workspace,
                    (
                        Model::new("scripted".into(), "test".into(), "1".into())?,
                        model,
                    ),
                )?
                .without_default_tools()
                .feedback_plugin(false)
                .without_user_settings()
                .state_directory(directory.path().join("state"))
                .workspace_mounting(WorkspaceMounting::Physical),
            )
            .await?,
        );
        let owner = harness.connect_sessions().await?;
        let session = acyclic_harness::local::sessions::create(&owner, "recall", None).await?;
        let conversation = Conversation::open(session);
        let original = "first\r\nsecond\rlast";
        conversation.send("original", original).await?;
        conversation.wait_current_input().await?;
        let mut history = History::new(conversation);
        history.request(1, "draft".into(), true, None, true);
        history.request(2, "draft".into(), true, None, false);
        let first = futures::future::poll_fn(|cx| history.poll(cx)).await?;
        history.resolve(&first, false);
        let second = futures::future::poll_fn(|cx| history.poll(cx)).await?;
        assert_eq!(first.text.as_deref(), Some(original));
        assert_eq!(second.request, 2);
        assert_eq!(second.text.as_deref(), Some(original));
        history.resolve(&second, true);
        let editor = super::super::Editor::new(original);
        history.request(3, editor.text().into(), false, None, false);
        let restored = futures::future::poll_fn(|cx| history.poll(cx)).await?;
        assert_eq!(restored.text.as_deref(), Some("draft"));
        history.resolve(&restored, true);
        history.request(4, "draft".into(), true, None, false);
        let unseen = futures::future::poll_fn(|cx| history.poll(cx)).await?;
        history.resolve(&unseen, false);
        history.request(5, "draft".into(), true, None, false);
        let recalled = futures::future::poll_fn(|cx| history.poll(cx)).await?;
        assert_eq!(recalled.text.as_deref(), Some(original));
        history.resolve(&recalled, true);
        // A failed older-page load cannot take custody of the saved draft.
        history.failure = Some("original history source closed".into());
        history.request(6, editor.text().into(), false, None, false);
        let restored = futures::future::poll_fn(|cx| history.poll(cx)).await?;
        assert_eq!(restored.text.as_deref(), Some("draft"));
        history.resolve(&restored, true);
        assert!(history.failure.is_none());
        for (index, accepted) in [false, true].into_iter().enumerate() {
            let request = 10 + index as u64 * 3;
            history.request(request, "draft".into(), true, None, false);
            let selected = futures::future::poll_fn(|cx| history.poll(cx)).await?;
            history.resolve(&selected, true);
            history.request(
                request + 1,
                editor.text().into(),
                true,
                Some(String::new()),
                true,
            );
            let searched = futures::future::poll_fn(|cx| history.poll(cx)).await?;
            assert_eq!(searched.text.as_deref(), Some(original));
            history.resolve(&searched, true);
            history.finish_search(accepted);
            history.request(request + 2, editor.text().into(), false, None, false);
            let restored = futures::future::poll_fn(|cx| history.poll(cx)).await?;
            assert_eq!(restored.text.as_deref(), Some("draft"));
            history.resolve(&restored, true);
        }
        // Search can be the first navigation from an unrecorded editor draft.
        for accepted in [false, true] {
            let mut fresh = History::new(Arc::clone(&history.conversation));
            fresh.request(20, "unsent draft".into(), true, Some(String::new()), true);
            let selected = futures::future::poll_fn(|cx| fresh.poll(cx)).await?;
            assert_eq!(selected.text.as_deref(), Some(original));
            fresh.resolve(&selected, true);
            fresh.finish_search(accepted);
            let expected = if accepted {
                editor.text()
            } else {
                "unsent draft"
            };
            fresh.request(21, expected.into(), false, None, false);
            let restored = futures::future::poll_fn(|cx| fresh.poll(cx)).await?;
            assert_eq!(restored.text.as_deref(), Some("unsent draft"));
            fresh.resolve(&restored, true);
        }
        Ok(())
    }
}

struct Navigation {
    request: u64,
    expected: String,
    previous: bool,
}

pub(super) struct History {
    conversation: Arc<Conversation>,
    saved: Option<Box<Self>>,
    source: Option<PromptHistory>,
    loading: Option<BoxFuture<'static, Loaded>>,
    messages: Vec<String>,
    cursor: Option<usize>,
    draft: String,
    shown: Option<String>,
    search: Option<String>,
    pending: VecDeque<Navigation>,
    ended: bool,
    failure: Option<String>,
    stale: bool,
}

impl History {
    pub fn new(conversation: Arc<Conversation>) -> Self {
        Self {
            conversation,
            saved: None,
            source: None,
            loading: None,
            messages: Vec::new(),
            cursor: None,
            draft: String::new(),
            shown: None,
            search: None,
            pending: VecDeque::new(),
            ended: false,
            failure: None,
            stale: false,
        }
    }

    pub fn invalidate(&mut self) {
        if let Some(original) = &mut self.saved {
            original.invalidate();
        }
        self.stale = true;
        if self.cursor.is_none() {
            self.refresh();
        }
    }

    fn refresh(&mut self) {
        self.source = None;
        self.loading = None;
        self.messages.clear();
        self.ended = false;
        self.failure = None;
        self.stale = false;
    }

    pub fn request(
        &mut self,
        request: u64,
        expected: String,
        previous: bool,
        search: Option<String>,
        restart: bool,
    ) {
        if search.is_some() && self.saved.is_none() {
            let conversation = Arc::clone(&self.conversation);
            let mut original = std::mem::replace(self, Self::new(conversation));
            original.pending.clear();
            if original.cursor.is_none() {
                original.draft = expected.clone();
            }
            self.saved = Some(Box::new(original));
        }
        // Keep an active navigation pinned to its cut. Once it returns to the
        // draft, or a new search starts, open the event-invalidated fresh cut.
        if restart || (self.stale && self.cursor.is_none()) {
            self.refresh();
        }
        if restart
            || self.search != search
            || (self.shown.as_deref() != Some(expected.as_str()) && self.draft != expected)
        {
            self.cursor = None;
            self.draft = expected.clone();
            self.pending.clear();
            self.search = search;
        }
        self.pending.push_back(Navigation {
            request,
            expected,
            previous,
        });
    }

    pub fn finish_search(&mut self, accepted: bool) {
        let Some(original) = self.saved.take() else {
            return;
        };
        if accepted {
            self.draft = original.draft;
            self.search = None;
            self.pending.clear();
        } else {
            *self = *original;
        }
    }

    pub fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<Reply, String>> {
        loop {
            let Some(next) = self.pending.front() else {
                return Poll::Pending;
            };
            let selected = if next.previous {
                let start = self.cursor.map_or(0, |cursor| cursor + 1);
                (start..self.messages.len()).find(|index| self.matches(*index))
            } else {
                self.cursor
                    .and_then(|cursor| (0..cursor).rev().find(|index| self.matches(*index)))
            };
            if selected.is_some() || !next.previous || self.ended {
                let next = self.pending.pop_front().expect("selected request");
                let before = self.cursor;
                let text = match selected {
                    Some(index) => {
                        self.cursor = Some(index);
                        Some(self.messages[index].clone())
                    }
                    None if !next.previous => {
                        self.cursor = None;
                        Some(self.draft.clone())
                    }
                    None => self.cursor.map(|index| self.messages[index].clone()),
                };
                if let Some(text) = &text {
                    self.shown = Some(super::Editor::new(text).text().to_owned());
                }
                return Poll::Ready(Ok(Reply {
                    request: next.request,
                    expected: next.expected,
                    text,
                    previous: next.previous,
                    before,
                }));
            }
            if let Some(error) = &self.failure {
                self.pending.clear();
                return Poll::Ready(Err(error.clone()));
            }
            if self.loading.is_none() {
                let source = self.source.take();
                let conversation = Arc::clone(&self.conversation);
                self.loading = Some(Box::pin(async move {
                    let mut source = source
                        .unwrap_or_else(|| PromptHistory::new(conversation.connection().clone()));
                    let text = source.next().await?;
                    Ok((source, text))
                }));
            }
            match self
                .loading
                .as_mut()
                .expect("query started")
                .as_mut()
                .poll(cx)
            {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => {
                    self.loading = None;
                    match result {
                        Ok((source, text)) => {
                            self.source = Some(source);
                            if let Some(text) = text {
                                self.messages.push(text);
                            } else {
                                self.ended = true;
                            }
                        }
                        Err(error) => {
                            self.failure = Some(error.to_string());
                        }
                    }
                }
            }
        }
    }

    pub fn resolve(&mut self, reply: &Reply, accepted: bool) {
        if accepted {
            if self.cursor.is_none() && reply.text.is_some() {
                self.refresh();
            }
        } else if self.pending.is_empty() {
            self.cursor = reply.before;
            let text = self
                .cursor
                .map_or(self.draft.as_str(), |index| self.messages[index].as_str());
            self.shown = Some(super::Editor::new(text).text().to_owned());
        }
    }

    fn matches(&self, index: usize) -> bool {
        self.search
            .as_ref()
            .is_none_or(|query| self.messages[index].contains(query))
    }
}
