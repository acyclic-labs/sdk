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
        let conversation = Arc::new(Conversation::open(harness.connect().await?));
        let original = "first\r\nsecond\rlast";
        conversation.send("original", original).await?;
        conversation.wait_current_input().await?;
        let mut history = History::new(conversation);
        history.request(1, "draft".into(), true, None, true);
        history.request(2, "draft".into(), true, None, false);
        let first = futures::future::poll_fn(|cx| history.poll(cx)).await?;
        let second = futures::future::poll_fn(|cx| history.poll(cx)).await?;
        assert_eq!(first.text.as_deref(), Some(original));
        assert_eq!(second.request, 2);
        assert_eq!(second.text.as_deref(), Some(original));
        let editor = super::super::Editor::new(original);
        history.request(3, editor.text().into(), false, None, false);
        let restored = futures::future::poll_fn(|cx| history.poll(cx)).await?;
        assert_eq!(restored.text.as_deref(), Some("draft"));
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

    pub fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<Reply, String>> {
        loop {
            let Some(next) = self.pending.front() else {
                return Poll::Pending;
            };
            if let Some(error) = &self.failure {
                self.pending.clear();
                return Poll::Ready(Err(error.clone()));
            }
            let selected = if next.previous {
                let start = self.cursor.map_or(0, |cursor| cursor + 1);
                (start..self.messages.len()).find(|index| self.matches(*index))
            } else {
                self.cursor
                    .and_then(|cursor| (0..cursor).rev().find(|index| self.matches(*index)))
            };
            if selected.is_some() || !next.previous || self.ended {
                let next = self.pending.pop_front().expect("selected request");
                let text = match selected {
                    Some(index) => {
                        self.cursor = Some(index);
                        Some(self.messages[index].clone())
                    }
                    None if !next.previous => {
                        self.cursor = None;
                        self.refresh();
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
                }));
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

    fn matches(&self, index: usize) -> bool {
        self.search
            .as_ref()
            .is_none_or(|query| self.messages[index].contains(query))
    }
}
