//! Borrowed Markdown task-list rendering; the file remains authoritative.

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

/// One checkbox borrowing its description from a Markdown file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaskItem<'a> {
    /// Checkbox state, without imposing an execution policy on the file.
    pub state: TaskState,
    /// Description on the same line, preserving its whitespace.
    pub text: &'a str,
}

/// Disposable interpretation of a file; no separate plan state is retained.
#[derive(Clone, Copy, Debug)]
pub struct TaskList<'a> {
    markdown: &'a str,
}

impl<'a> TaskList<'a> {
    /// Checkboxes in source order, excluding fenced code blocks.
    pub fn items(self) -> impl Iterator<Item = TaskItem<'a>> {
        scan(self.markdown).filter_map(Result::ok)
    }
}

/// Reads conventional Markdown checkboxes with constant auxiliary memory.
/// `None` means render the original file as raw text, including malformed lists.
pub fn parse_task_list(markdown: &str) -> Option<TaskList<'_>> {
    let mut found = false;
    for item in scan(markdown) {
        item.ok()?;
        found = true;
    }
    found.then_some(TaskList { markdown })
}

fn scan(markdown: &str) -> impl Iterator<Item = Result<TaskItem<'_>, ()>> {
    let mut fence = None;
    markdown.lines().filter_map(move |line| {
        let line = line.trim_start_matches([' ', '\t']);
        let delimiter = line.as_bytes().first().copied();
        let run = line
            .bytes()
            .take_while(|byte| Some(*byte) == delimiter)
            .count();
        if let Some((byte, length)) = fence {
            if delimiter == Some(byte) && run >= length && line.get(run..)?.trim().is_empty() {
                fence = None;
            }
            return None;
        }
        if matches!(delimiter, Some(b'`' | b'~')) && run >= 3 {
            fence = Some((delimiter?, run));
            return None;
        }
        let body = line.strip_prefix("- ")?;
        if !body.starts_with('[') {
            return None;
        }
        let state = match body.get(..3) {
            Some("[ ]") => TaskState::Pending,
            Some("[~]") => TaskState::InProgress,
            Some("[x]" | "[X]") => TaskState::Completed,
            _ => return Some(Err(())),
        };
        let Some(text) = body.get(3..).and_then(|tail| tail.strip_prefix(' ')) else {
            return Some(Err(()));
        };
        Some(if text.trim().is_empty() {
            Err(())
        } else {
            Ok(TaskItem { state, text })
        })
    })
}
