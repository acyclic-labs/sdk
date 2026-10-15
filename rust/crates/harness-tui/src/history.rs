use crate::buffer::Buffer;

#[derive(Clone, Debug)]
pub(crate) struct Edit {
    pub before: Buffer,
    pub after: Buffer,
    pub at: u64,
    pub kind: u8,
    pub size: usize,
    pub boundary: bool,
}

/// Submitted prompts, with a saved in-progress draft and reverse search.
#[derive(Clone, Debug, Default)]
pub struct PromptHistory {
    entries: Vec<String>,
    index: Option<usize>,
    draft: String,
    search_before: Option<usize>,
}

impl PromptHistory {
    /// Record a nonblank prompt, suppressing adjacent duplicates.
    pub fn push(&mut self, text: String) {
        if !text.trim().is_empty() && self.entries.last() != Some(&text) {
            self.entries.push(text);
        }
        self.reset();
    }

    /// Return the submitted prompts in chronological order.
    pub fn entries(&self) -> &[String] {
        &self.entries
    }

    /// Begin a new history navigation session.
    pub fn reset(&mut self) {
        self.index = None;
        self.search_before = None;
    }

    /// Go backwards, saving the live draft on the first invocation.
    pub fn previous(&mut self, draft: &str) -> Option<String> {
        if self.entries.is_empty() {
            return None;
        }
        if self.index.is_none() {
            self.draft = draft.to_owned();
        }
        let index = self.index.unwrap_or(self.entries.len()).saturating_sub(1);
        self.index = Some(index);
        self.entries.get(index).cloned()
    }

    /// Go forwards, restoring the draft after the newest entry.
    pub fn next_entry(&mut self) -> Option<String> {
        let index = self.index? + 1;
        if index >= self.entries.len() {
            self.index = None;
            Some(self.draft.clone())
        } else {
            self.index = Some(index);
            self.entries.get(index).cloned()
        }
    }

    /// Find the previous substring match; reset navigation to restart a query.
    pub fn reverse_search(&mut self, query: &str) -> Option<String> {
        let end = self.search_before.unwrap_or(self.entries.len());
        let found = self
            .entries
            .iter()
            .take(end)
            .enumerate()
            .rev()
            .find(|(_, entry)| entry.contains(query));
        if let Some((index, entry)) = found {
            self.search_before = Some(index);
            Some(entry.clone())
        } else {
            None
        }
    }
}
