use caseless::Caseless;
use std::{collections::VecDeque, ops::Range};
pub(crate) mod index;
pub(crate) mod view;
pub use view::TranscriptSearch;

/// Unicode case-folded matches, expressed as byte ranges in the original text.
/// Fold expansions highlight their whole source character; duplicate ranges
/// from the same expansion are returned once. Empty queries have no matches.
pub fn transcript_matches(text: &str, query: &str) -> Vec<Range<usize>> {
    matches(text, query).0
}

fn matches(text: &str, query: &str) -> (Vec<Range<usize>>, usize) {
    let needle: Vec<_> = query.chars().default_case_fold().collect();
    if needle.is_empty() {
        return (Vec::new(), 0);
    }
    let mut work = needle.len();
    let mut prefix = vec![0; needle.len()];
    for end in 1..needle.len() {
        let mut length = prefix.get(end - 1).copied().unwrap_or(0);
        while length != 0 && needle.get(length) != needle.get(end) {
            work += 1;
            length = prefix.get(length - 1).copied().unwrap_or(0);
        }
        work += 1;
        if let Some(slot) = prefix.get_mut(end) {
            *slot = length + usize::from(needle.get(length) == needle.get(end));
        }
    }
    let mut origins = VecDeque::new();
    let mut found = Vec::new();
    let mut length = 0;
    for (start, character) in text.char_indices() {
        let end = start + character.len_utf8();
        for folded in std::iter::once(character).default_case_fold() {
            if origins.len() == needle.len() {
                origins.pop_front();
            }
            origins.push_back(start);
            while length != 0 && needle.get(length) != Some(&folded) {
                work += 1;
                length = prefix.get(length - 1).copied().unwrap_or(0);
            }
            work += 1;
            length += usize::from(needle.get(length) == Some(&folded));
            if length == needle.len() {
                let Some(&start) = origins.front() else {
                    continue;
                };
                let range = start..end;
                if found.last() != Some(&range) {
                    found.push(range);
                }
                length = prefix.get(length - 1).copied().unwrap_or(0);
            }
        }
    }
    (found, work)
}

#[cfg(test)]
#[path = "transcript_search/tests.rs"]
mod tests;
