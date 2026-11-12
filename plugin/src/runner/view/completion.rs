//! One visible completion page over the selected conversation's query owner.
use super::{Conversation, Failure};
use acyclic_harness::{
    client::{
        AgentPageRequest, MAX_READ_BYTES,
        commands::{PROMPT_DIRECTORIES, validate_name},
    },
    conversation::PrivateDirectoryEntryKind,
    local::agent_conversation::ProjectDirectory,
    tui::{CompletionKind, PromptInput, QueryController, QueryPage},
};
use std::{collections::VecDeque, pin::Pin, sync::Arc};
use tokio::sync::Mutex;

const PROJECTION: &str = "queries/completion";
type Names = Vec<(String, bool)>;
type Identity = (CompletionKind, String);
pub(super) type Page = QueryPage<Source, Names>;

#[derive(Clone)]
pub(super) struct Source {
    identity: Identity,
    reader: Option<Arc<Mutex<Reader>>>,
}

impl Source {
    fn first(identity: Identity) -> Self {
        Self {
            identity,
            reader: None,
        }
    }
}

#[derive(Default)]
pub(super) struct State {
    identity: Option<Identity>,
    pub controller: QueryController<Source, Names>,
}

impl State {
    pub fn refresh<F>(
        &mut self,
        input: &mut PromptInput,
        enabled: bool,
        pending: Pin<&mut Option<F>>,
    ) {
        input.refresh_completions();
        let request = input
            .completion
            .request()
            .filter(|request| enabled && request.kind != CompletionKind::Theme);
        if match (self.identity.as_ref(), request) {
            (Some((kind, query)), Some(request)) => {
                *kind == request.kind && query == &request.query
            }
            (None, None) => true,
            _ => false,
        } {
            return;
        }
        let identity = request.map(|request| (request.kind, request.query.clone()));
        self.identity = identity.clone();
        input.completion.directory_page = None;
        input
            .completion
            .set_names(PROJECTION, CompletionKind::Mention, Vec::new());
        match identity {
            Some(identity) => self.controller.select(Source::first(identity), pending),
            None => self.controller.close(pending),
        }
    }

    pub fn finish<F>(
        &mut self,
        input: &mut PromptInput,
        result: Result<Page, Failure>,
        mut pending: Pin<&mut Option<F>>,
    ) -> Result<(), Failure> {
        self.controller.finish(result, pending.as_mut())?;
        let page = self
            .controller
            .page
            .as_mut()
            .ok_or("completion page is absent")?;
        input.completion.directory_page = Some((page.next.is_some(), page.continued));
        let names = std::mem::take(&mut page.context);
        if names.is_empty()
            && let Some(next) = page.next.take()
        {
            // This is original source progress, not a readiness retry. The
            // next native page remains cancellable by the existing event loop.
            self.controller.select(next, pending);
        } else if let Some((kind, _)) = self.identity.as_ref() {
            input.completion.set_names(PROJECTION, *kind, names);
        }
        Ok(())
    }

    pub fn page<F>(
        &mut self,
        input: &mut PromptInput,
        previous: bool,
        pending: Pin<&mut Option<F>>,
    ) -> bool {
        let mut top = 0;
        let selected = self.controller.scroll(
            &mut top,
            if previous { 1 } else { -1 },
            1,
            pending,
            |source| Source::first(source.identity.clone()),
        );
        if selected {
            input.completion.directory_page = None;
            input
                .completion
                .set_names(PROJECTION, CompletionKind::Mention, Vec::new());
        }
        selected
    }
}

struct Reader {
    directories: VecDeque<String>,
    current: Option<ProjectDirectory>,
    prefix: String,
}

impl Reader {
    fn new((kind, query): &Identity) -> Result<Self, Failure> {
        let (prefix, directories) = match kind {
            CompletionKind::Mention => {
                let (parent, _) = query.rsplit_once('/').unwrap_or(("", query));
                let prefix = if parent.is_empty() {
                    String::new()
                } else {
                    format!("{parent}/")
                };
                (prefix, VecDeque::from([parent.to_owned()]))
            }
            CompletionKind::Command => {
                let (parent, _) = query.rsplit_once(':').unwrap_or(("", query));
                if !parent.is_empty() {
                    validate_name(parent)?;
                }
                let prefix = if parent.is_empty() {
                    String::new()
                } else {
                    format!("{parent}:")
                };
                let parent = parent.replace(':', "/");
                let roots = PROMPT_DIRECTORIES
                    .into_iter()
                    .map(|root| {
                        if parent.is_empty() {
                            root.to_owned()
                        } else {
                            format!("{root}/{parent}")
                        }
                    })
                    .collect();
                (prefix, roots)
            }
            CompletionKind::Theme => return Err("themes use the followed command catalogue".into()),
        };
        Ok(Self {
            directories,
            current: None,
            prefix,
        })
    }
}

pub(super) async fn load(
    conversation: Arc<Conversation>,
    mut source: Source,
    rows: u32,
) -> Result<Page, Failure> {
    let continued = source.reader.is_some();
    let reader = match &source.reader {
        Some(reader) => Arc::clone(reader),
        None => Arc::new(Mutex::new(Reader::new(&source.identity)?)),
    };
    let mut selected = reader.lock().await;
    if selected.current.is_none()
        && let Some(path) = selected.directories.pop_front()
    {
        selected.current = Some(
            conversation
                .project_directory(
                    path,
                    AgentPageRequest::new(rows, MAX_READ_BYTES.try_into()?, None)?,
                )
                .await?,
        );
    }
    let entries = match selected.current.as_mut() {
        Some(query) => query.next().await?,
        None => None,
    };
    let mut names = Vec::new();
    if let Some(page) = entries {
        if !page.has_more {
            selected.current = None;
        }
        for entry in page.entries {
            let directory = entry.kind == PrivateDirectoryEntryKind::Directory;
            let name = match source.identity.0 {
                CompletionKind::Mention => format!(
                    "{}{}{}",
                    selected.prefix,
                    entry.name,
                    if directory { "/" } else { "" }
                ),
                CompletionKind::Command => {
                    let Some(name) = (if directory {
                        Some(entry.name.as_str())
                    } else {
                        entry.name.strip_suffix(".md")
                    }) else {
                        continue;
                    };
                    let name = format!("{}{name}", selected.prefix);
                    if validate_name(&name).is_err() {
                        continue;
                    }
                    format!("{name}{}", if directory { ":" } else { "" })
                }
                CompletionKind::Theme => {
                    return Err("theme source cannot select project names".into());
                }
            };
            if source.identity.0.score(&source.identity.1, &name).is_some() {
                names.push((name, directory));
            }
        }
    } else {
        selected.current = None;
    }
    let more = selected.current.is_some() || !selected.directories.is_empty();
    drop(selected);
    source.reader = Some(reader);
    Ok(QueryPage::new(Vec::new(), more.then_some(source), continued).with_context(names))
}
