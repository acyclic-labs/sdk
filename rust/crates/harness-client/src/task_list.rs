//! Disposable Markdown task-list interpretation of an observed file.

use pulldown_cmark::{CowStr, Event, Options, Parser, Tag, TagEnd, TextMergeWithOffset};
use std::borrow::Cow;

/// A Markdown checkbox state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskState {
    /// Work has not started.
    Pending,
    /// Work is underway.
    InProgress,
    /// Work has finished.
    Completed,
}

impl TaskState {
    /// Canonical checkbox marker, including brackets.
    pub fn marker(self) -> &'static str {
        match self {
            Self::Pending => "[ ]",
            Self::InProgress => "[~]",
            Self::Completed => "[x]",
        }
    }
}

/// One checkbox with its decoded inline description.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskItem<'a> {
    /// Checkbox state, without imposing an execution policy on the file.
    pub state: TaskState,
    /// Text borrows the original input when Markdown decoding permits it.
    pub text: Cow<'a, str>,
}

/// Disposable interpretation; file contents remain authoritative.
#[derive(Clone, Debug)]
pub struct TaskList<'a> {
    items: Vec<TaskItem<'a>>,
}

impl<'a> TaskList<'a> {
    /// Checkboxes in source order, including nested and ordered list items.
    pub fn items(self) -> impl Iterator<Item = TaskItem<'a>> {
        self.items.into_iter()
    }
}

#[derive(Default)]
struct Item<'a> {
    state: Option<TaskState>,
    text: Cow<'a, str>,
    first_text: bool,
}

fn text(value: CowStr<'_>) -> Cow<'_, str> {
    match value {
        CowStr::Borrowed(value) => Cow::Borrowed(value),
        value => Cow::Owned(value.into_string()),
    }
}

/// Reads Markdown task lists; `None` requests raw-text rendering of the file.
pub fn parse_task_list(markdown: &str) -> Option<TaskList<'_>> {
    let mut items = Vec::<Item<'_>>::new();
    let mut stack = Vec::new();
    let mut code = false;
    for (event, span) in TextMergeWithOffset::new(
        Parser::new_ext(markdown, Options::ENABLE_TASKLISTS).into_offset_iter(),
    ) {
        match event {
            Event::Start(Tag::Item) => {
                stack.push(items.len());
                items.push(Item {
                    first_text: true,
                    ..Default::default()
                });
            }
            Event::End(TagEnd::Item) => {
                stack.pop();
            }
            Event::Start(Tag::CodeBlock(_)) => {
                code = true;
                if stack
                    .last()
                    .and_then(|index| items.get(*index))
                    .is_some_and(|item| item.state.is_some())
                {
                    return None;
                }
            }
            Event::End(TagEnd::CodeBlock) => code = false,
            event if !code => {
                let Some(index) = stack.last() else { continue };
                let item = items.get_mut(*index)?;
                let value = match event {
                    Event::Start(Tag::Paragraph) if !item.text.is_empty() => Cow::Borrowed("\n\n"),
                    Event::TaskListMarker(done) => {
                        item.state = Some(if done {
                            TaskState::Completed
                        } else {
                            TaskState::Pending
                        });
                        continue;
                    }
                    Event::Text(value) => {
                        let mut value = text(value);
                        if item.first_text && item.state.is_none() {
                            if let Some(tail) = value.strip_prefix("[~]") {
                                let source = markdown.get(span.clone())?;
                                if !source.starts_with("[~]")
                                    || markdown.get(..span.start)?.ends_with('\\')
                                {
                                    return None;
                                }
                                if !tail.starts_with(char::is_whitespace) {
                                    return None;
                                }
                                let offset = value.len() - tail.trim_start().len();
                                value = match value {
                                    Cow::Borrowed(value) => Cow::Borrowed(value.get(offset..)?),
                                    Cow::Owned(mut value) => {
                                        value.drain(..offset);
                                        Cow::Owned(value)
                                    }
                                };
                                item.state = Some(TaskState::InProgress);
                            } else if value.starts_with('[') {
                                return None;
                            }
                        }
                        item.first_text = false;
                        value
                    }
                    Event::Code(value) => {
                        item.first_text = false;
                        text(value)
                    }
                    Event::SoftBreak | Event::HardBreak => Cow::Borrowed("\n"),
                    _ => continue,
                };
                if item.text.is_empty() {
                    item.text = value;
                } else {
                    item.text.to_mut().push_str(&value);
                }
            }
            _ => {}
        }
    }
    let mut tasks = Vec::new();
    for item in items {
        if let Some(state) = item.state {
            if item.text.trim().is_empty() {
                return None;
            }
            tasks.push(TaskItem {
                state,
                text: item.text,
            });
        }
    }
    (!tasks.is_empty()).then_some(TaskList { items: tasks })
}
