//! Drafts are plain UTF-8 files, one injected private-volume path per item.
//! History is a JSON array in the user's volume. Live-query consumers fold
//! snapshots through `Editor::set_text`, `parse_history` and `InputKeymap::parse`.

use crate::{Editor, PromptHistory};
use std::{
    io::{self, Write},
    path::Path,
};

/// Decode a history-file snapshot, normalizing prompts and adjacent duplicates.
pub fn parse_history(json: &str) -> io::Result<PromptHistory> {
    let entries: Vec<String> = serde_json::from_str(json)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut history = PromptHistory::default();
    for entry in entries {
        history.push(entry);
    }
    let mut loaded = PromptHistory::default();
    loaded.replace(history);
    Ok(loaded)
}

/// Encode the history file; navigation and search cursors remain ephemeral.
pub fn format_history(history: &PromptHistory) -> io::Result<String> {
    serde_json::to_string(history.entries()).map_err(io::Error::other)
}

/// Load one task/item draft. Missing files are empty drafts.
pub fn load_draft(path: &Path) -> io::Result<Editor> {
    Ok(Editor::new(&read_or_empty(path)?))
}

/// Write one draft to its injected path; no owner IDs become filesystem paths.
pub fn save_draft(path: &Path, editor: &Editor) -> io::Result<()> {
    write_atomic(path, editor.text().as_bytes())
}

/// Load saved input history. A missing file starts empty.
pub fn load_history(path: &Path) -> io::Result<PromptHistory> {
    let json = read_or_empty(path)?;
    if json.is_empty() {
        return parse_history("[]");
    }
    parse_history(&json)
}

/// Write saved input history to the caller's config-volume path.
pub fn save_history(path: &Path, history: &PromptHistory) -> io::Result<()> {
    write_atomic(path, format_history(history)?.as_bytes())
}

fn read_or_empty(path: &Path) -> io::Result<String> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(text),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(String::new()),
        Err(error) => Err(error),
    }
}

pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    // Private permissions on Unix and atomic replacement on Windows too.
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}
