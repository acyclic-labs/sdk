use super::{Completion, CompletionKind, CompletionMenu};
use std::{io, rc::Rc};

#[derive(Clone, Debug)]
pub(super) struct Entry {
    pub kind: CompletionKind,
    pub item: Rc<Completion>,
}

impl CompletionMenu {
    /// Fold one command/workspace file from a snapshot or cursor-follow change.
    /// Paths are logical volume paths: commands/*.json, commands/**/*.md,
    /// workspace/**. `None` removes a file. A malformed replacement is atomic.
    pub fn set_file(&mut self, path: &str, bytes: Option<&[u8]>) -> io::Result<()> {
        let entries = bytes
            .map(|bytes| parse_file(path, bytes))
            .transpose()?
            .unwrap_or_default();
        let order = self
            .files
            .get(path)
            .map(|(order, _)| *order)
            .unwrap_or_else(|| {
                let order = self.next_file;
                self.next_file += 1;
                order
            });
        if let Some((_, old)) = self.files.remove(path) {
            for (index, entry) in old.iter().enumerate() {
                if let Some(request) = &self.request {
                    if request.kind != entry.kind {
                        continue;
                    }
                    self.work.candidates += 1;
                    let score = match request.kind {
                        CompletionKind::Command => {
                            entry.item.value.starts_with(&request.query).then_some(0)
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
            self.rank_entry(order, index, entry);
        }
        if bytes.is_some() {
            self.files.insert(path.to_owned(), (order, entries));
        }
        self.selected = self.selected.min(self.items.len().saturating_sub(1));
        Ok(())
    }

    /// Replace a lost query's disposable fold before applying its new snapshot.
    pub fn clear_files(&mut self) {
        self.files.clear();
        self.next_file = 0;
        self.items.clear();
        self.selected = 0;
    }
}

fn parse_file(path: &str, bytes: &[u8]) -> io::Result<Vec<Entry>> {
    let make = |kind, value: String, description, tool, append_space| Entry {
        kind,
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
    };
    if let Some(path) = path.strip_prefix("workspace/") {
        return Ok(vec![make(
            CompletionKind::Mention,
            path.into(),
            String::new(),
            false,
            true,
        )]);
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
            let tool = definition
                .get("tool")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let append = definition
                .get("append_space")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            Ok(make(
                CompletionKind::Command,
                name.into(),
                description.into(),
                tool,
                append,
            ))
        })
        .collect()
}
