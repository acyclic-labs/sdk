//! Navigation owns only the original socket and prompts the user has requested.
use super::{Conversation, Failure};
use acyclic_harness::{
    client::{AgentPageRequest, InputPreviewStep, MAX_READ_BYTES},
    local::agent_conversation::ConversationInput,
};
use futures::{Future, future::BoxFuture};
use std::{
    collections::VecDeque,
    sync::Arc,
    task::{Context, Poll},
};

type Loaded = Result<(ConversationInput, Option<String>), Failure>;

pub(super) struct Reply {
    pub request: u64,
    pub expected: String,
    pub text: Option<String>,
    pub previous: bool,
}

struct Navigation {
    request: u64,
    expected: String,
    previous: bool,
}

pub(super) struct History {
    conversation: Arc<Conversation>,
    source: Option<ConversationInput>,
    loading: Option<BoxFuture<'static, Loaded>>,
    messages: Vec<String>,
    cursor: Option<usize>,
    draft: String,
    shown: Option<String>,
    search: Option<String>,
    pending: VecDeque<Navigation>,
    ended: bool,
    failure: Option<String>,
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
        }
    }

    pub fn request(
        &mut self,
        request: u64,
        expected: String,
        previous: bool,
        search: Option<String>,
    ) {
        if self.search != search
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
                        Some(self.draft.clone())
                    }
                    None => None,
                };
                if let Some(text) = &text {
                    self.shown = Some(text.clone());
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
                    let mut source = match source {
                        Some(source) => source,
                        None => {
                            conversation
                                .prompt_history_query(AgentPageRequest::new(
                                    1,
                                    MAX_READ_BYTES as u32,
                                    None,
                                )?)
                                .await?
                        }
                    };
                    let mut text = String::new();
                    let mut original = None;
                    loop {
                        match source.next().await? {
                            Some(InputPreviewStep::Progress) => {}
                            Some(InputPreviewStep::MessageRange {
                                inbox,
                                offset,
                                preview,
                            }) => {
                                if offset != text.len() as u64
                                    || original.is_some_and(|selected| selected != inbox)
                                {
                                    return Err(
                                        "Prompt history range lost its original input.".into()
                                    );
                                }
                                original = Some(inbox);
                                text.push_str(&preview.text);
                                if text.len() as u64 == preview.bytes {
                                    return Ok((source, Some(text)));
                                }
                            }
                            Some(InputPreviewStep::Selected(None)) => return Ok((source, None)),
                            _ => {
                                return Err(
                                    "Prompt history ended without its original completion.".into(),
                                );
                            }
                        }
                    }
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
