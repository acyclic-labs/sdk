//! Text preservation in the turn oracle: the exact fragments in order, with
//! only the deletions turn repair is specified to make.
use super::turn_field;
use crate::Adapter;

/// All text in order. Repair inserts whole `(continue)` turns at either end
/// and joins fragments without changing their text; Anthropic may also drop
/// whitespace-only fragments, and nothing else.
pub(super) fn text_matches(
    adapter: Adapter,
    input: &[serde_json::Value],
    output: &[serde_json::Value],
) -> bool {
    let fragments = |messages: &[serde_json::Value]| -> Vec<String> {
        messages
            .iter()
            .filter_map(|message| message[turn_field(adapter)].as_array())
            .flatten()
            .filter_map(|part| part["text"].as_str().map(str::to_owned))
            .collect()
    };
    let continuation = |message: Option<&serde_json::Value>| {
        message.is_some_and(|message| {
            message["role"] == "user"
                && message[turn_field(adapter)]
                    .as_array()
                    .is_some_and(|parts| parts.len() == 1 && parts[0]["text"] == "(continue)")
        })
    };
    let expected = fragments(input);
    let first = usize::from(continuation(output.first()));
    let last = usize::from(output.len() > first && continuation(output.last()));
    [(0, 0), (first, 0), (0, last), (first, last)]
        .into_iter()
        .any(|(head, tail)| {
            let text = fragments(&output[head..output.len() - tail]).concat();
            spells(adapter, &expected, &text)
        })
}

/// Whether `text` is the input fragments in order, each kept exactly or, for
/// Anthropic only, a whitespace-only fragment deleted whole. Tracks every
/// reachable output offset, so a blank fragment never licenses any other loss.
fn spells(adapter: Adapter, fragments: &[String], text: &str) -> bool {
    let mut reachable = vec![false; text.len() + 1];
    reachable[0] = true;
    for fragment in fragments {
        let droppable = adapter == Adapter::Anthropic && fragment.trim().is_empty();
        let mut next = vec![false; text.len() + 1];
        for offset in (0..=text.len()).filter(|&offset| reachable[offset]) {
            next[offset] |= droppable;
            if text[offset..].starts_with(fragment.as_str()) {
                next[offset + fragment.len()] = true;
            }
        }
        reachable = next;
    }
    reachable[text.len()]
}
