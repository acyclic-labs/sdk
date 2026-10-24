use super::{Completion, CompletionKind, CompletionMenu};
use std::{io, rc::Rc};

#[derive(Clone, Debug)]
pub(super) struct Entry {
    pub kind: CompletionKind,
    pub item: Rc<Completion>,
    pub theme: bool,
    pub query: Option<String>,
}

impl CompletionMenu {
    /// Fold one command or path-index file from a snapshot/follow change.
    /// Paths are logical volume paths: commands/*.json, commands/**/*.md,
    /// search/results.json. UI workspace files are never mention candidates.
    pub fn set_file(&mut self, path: &str, bytes: Option<&[u8]>) -> io::Result<()> {
        let entries = bytes
            .map(|bytes| parse_file(path, bytes))
            .transpose()?
            .unwrap_or_default();
        let progress = progress(path, bytes)?;
        if let Some(progress) = progress {
            self.search_progress = progress;
        }
        self.set_entries(path, bytes.is_some(), entries);
        Ok(())
    }

    /// Fold an atomic file publication, parsing every replacement before changing
    /// any candidate. Unchanged files and their ranked entries are retained.
    pub fn set_files<'a>(
        &mut self,
        files: impl IntoIterator<Item = (&'a str, Option<&'a [u8]>)>,
    ) -> io::Result<()> {
        let decoded = files
            .into_iter()
            .map(|(path, bytes)| {
                let entries = bytes
                    .map(|bytes| parse_file(path, bytes))
                    .transpose()?
                    .unwrap_or_default();
                Ok((path, bytes.is_some(), entries, progress(path, bytes)?))
            })
            .collect::<io::Result<Vec<_>>>()?;
        for (path, present, entries, progress) in decoded {
            if let Some(progress) = progress {
                self.search_progress = progress;
            }
            self.set_entries(path, present, entries);
        }
        Ok(())
    }

    fn set_entries(&mut self, path: &str, present: bool, entries: Vec<Entry>) {
        let order = self.files.get(path).map_or_else(
            || {
                let order = self.next_file;
                self.next_file += 1;
                order
            },
            |(order, _)| *order,
        );
        if let Some((_, old)) = self.files.remove(path) {
            for entry in old.iter().filter(|entry| entry.theme) {
                let name = entry.item.value.to_ascii_lowercase();
                if let Some(count) = self.theme_commands.get_mut(&name) {
                    *count -= 1;
                    if *count == 0 {
                        self.theme_commands.remove(&name);
                        self.version = None;
                    }
                }
            }
            for (index, entry) in old.iter().enumerate() {
                if let Some(request) = &self.request {
                    if request.kind != entry.kind {
                        continue;
                    }
                    self.work.candidates += 1;
                    let score = match request.kind {
                        CompletionKind::Command => {
                            super::prefix(&entry.item.value, &request.query).then_some(0)
                        }
                        CompletionKind::Theme => {
                            super::theme_prefix(&entry.item.value, &request.query).then_some(0)
                        }
                        CompletionKind::Mention => {
                            super::fuzzy_score(&request.query, &entry.item.value)
                        }
                    };
                    if let Some(score) = score {
                        self.items.remove(&(!entry.item.tool, score, order, index));
                    }
                }
            }
        }
        for (index, entry) in entries.iter().enumerate() {
            if entry.theme {
                *self
                    .theme_commands
                    .entry(entry.item.value.to_ascii_lowercase())
                    .or_default() += 1;
                self.version = None;
            }
            self.rank_entry(order, index, entry);
        }
        if present {
            self.files.insert(path.to_owned(), (order, entries));
        }
        self.selected = self.selected.min(self.items.len().saturating_sub(1));
    }

    /// Replace a lost query's disposable fold before applying its new snapshot.
    pub fn clear_files(&mut self) {
        self.files.clear();
        self.theme_commands.clear();
        self.version = None;
        self.next_file = 0;
        self.items.clear();
        self.selected = 0;
        self.search_progress = None;
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
pub(super) struct Progress {
    pub query: String,
    pub pending_dirs: usize,
    pub complete: bool,
    pub unavailable_entries: usize,
}

fn progress(path: &str, bytes: Option<&[u8]>) -> io::Result<Option<Option<Progress>>> {
    if path != "search/progress.json" {
        return Ok(None);
    }
    let Some(bytes) = bytes else {
        return Ok(Some(None));
    };
    let value: Progress = serde_json::from_slice(bytes)?;
    Ok(Some(Some(value)))
}

fn make(
    kind: CompletionKind,
    value: String,
    description: String,
    tool: bool,
    append_space: bool,
) -> Entry {
    Entry {
        kind,
        theme: false,
        query: None,
        item: Rc::new(Completion {
            label: if kind == CompletionKind::Command {
                format!("/{value}")
            } else {
                value.clone()
            },
            value,
            description,
            tool,
            append_space,
        }),
    }
}

fn parse_file(path: &str, bytes: &[u8]) -> io::Result<Vec<Entry>> {
    if path == "search/results.json" {
        #[derive(serde::Deserialize)]
        struct Results {
            query: String,
            paths: Vec<String>,
        }
        let results: Results = serde_json::from_slice(bytes)?;
        if results.paths.len() > super::PATH_RESULT_COUNT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "search publication exceeds its top-k contract",
            ));
        }
        return Ok(results
            .paths
            .into_iter()
            .map(|path| {
                let mut entry = make(CompletionKind::Mention, path, String::new(), false, true);
                entry.query = Some(results.query.clone());
                entry
            })
            .collect());
    }
    let Some(path) = path.strip_prefix("commands/") else {
        return Ok(vec![]);
    };
    if let Some(name) = path.strip_suffix(".md") {
        let body = std::str::from_utf8(bytes).map_err(io::Error::other)?;
        return Ok(vec![make(
            CompletionKind::Command,
            name.replace('/', ":"),
            body.lines().next().unwrap_or_default().into(),
            false,
            true,
        )]);
    }
    if !path.ends_with(".json") {
        return Ok(vec![]);
    }
    if path == "themes.json" {
        let themes: Vec<String> = serde_json::from_slice(bytes)?;
        return themes
            .into_iter()
            .map(|name| {
                if name.trim() != name || name.is_empty() || name.chars().any(char::is_control) {
                    return Err(io::Error::other("invalid theme name"));
                }
                Ok(make(
                    CompletionKind::Theme,
                    name,
                    String::new(),
                    false,
                    false,
                ))
            })
            .collect();
    }
    command_definitions(bytes)
}

/// Command entries, with their aliases, from a `commands/*.json` file.
fn command_definitions(bytes: &[u8]) -> io::Result<Vec<Entry>> {
    let definitions: Vec<serde_json::Value> = serde_json::from_slice(bytes)?;
    definitions
        .into_iter()
        .map(|definition| {
            let name = definition
                .get("name")
                .and_then(|v| v.as_str())
                .filter(|s| {
                    !s.is_empty() && !s.chars().any(|c| c.is_whitespace() || c.is_control())
                })
                .ok_or_else(|| io::Error::other("invalid command name"))?;
            let description = definition
                .get("description")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let action = definition.pointer("/action/kind").and_then(|v| v.as_str());
            let tool = action == Some("tool")
                || definition
                    .get("tool")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
            let append = tool
                || action == Some("theme")
                || definition
                    .get("append_space")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
            let aliases: Vec<String> = definition
                .get("aliases")
                .map(|v| serde_json::from_value(v.clone()))
                .transpose()?
                .unwrap_or_default();
            let mut names = vec![name.to_owned()];
            names.extend(aliases);
            names
                .into_iter()
                .map(|name| {
                    if name.is_empty() || name.chars().any(|c| c.is_whitespace() || c.is_control())
                    {
                        return Err(io::Error::other("invalid command alias"));
                    }
                    let mut entry = make(
                        CompletionKind::Command,
                        name,
                        description.into(),
                        tool,
                        append,
                    );
                    entry.theme = action == Some("theme")
                        || action.is_none() && entry.item.value.eq_ignore_ascii_case("theme");
                    Ok(entry)
                })
                .collect::<io::Result<Vec<_>>>()
        })
        .collect::<io::Result<Vec<_>>>()
        .map(|groups| groups.into_iter().flatten().collect())
}
