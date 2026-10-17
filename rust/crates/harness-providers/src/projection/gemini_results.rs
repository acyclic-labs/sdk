//! GEM-TOOL-05/06: results follow their parallel calls in call order.
use super::wire::{Item, Message};
use crate::protocol::sequence::{Exchange, GeminiPart, answered, exchange};
use std::collections::HashMap;

pub(super) fn order(messages: &mut [Message<'_>]) {
    for index in 1..messages.len() {
        let (previous, next) = messages.split_at_mut(index);
        if let (
            Message::Gemini {
                role: "model",
                parts: calls,
            },
            Message::Gemini {
                role: "user",
                parts: results,
            },
        ) = (&previous[index - 1], &mut next[0])
        {
            order_results(calls, results);
        }
    }
}

fn response<'a>(item: &'a Item<'_>) -> Option<Exchange<'a>> {
    match item {
        Item::Json(value) => match GeminiPart::of(value) {
            GeminiPart::Response(response) => Some(exchange(response)),
            _ => None,
        },
        Item::Native(_) | Item::Call(_) => None,
    }
}

fn order_results(calls: &[Item<'_>], results: &mut [Item<'_>]) {
    let calls = calls.iter().filter_map(|item| match item {
        Item::Json(value) => match GeminiPart::of(value) {
            GeminiPart::Call(call) => Some(exchange(call)),
            _ => None,
        },
        Item::Call(call) => Some((Some(call.name.as_ref()), Some(call.id.as_str()))),
        Item::Native(_) => None,
    });
    // The answered path borrows both sequences and allocates nothing.
    if answered(calls.clone(), results.iter().filter_map(response)) {
        return;
    }
    let count = calls.clone().count();
    if count == 0 || count > results.len() {
        return;
    }
    let positions: HashMap<_, _> = calls
        .enumerate()
        .map(|(index, call)| (call, index))
        .collect();
    if positions.len() != count {
        return;
    }
    let mut ranks = Vec::with_capacity(results.len());
    let mut seen = vec![false; count];
    let mut extra = count;
    for result in results.iter() {
        let position = response(result).and_then(|key| positions.get(&key).copied());
        ranks.push(if let Some(position) = position {
            if std::mem::replace(&mut seen[position], true) {
                return;
            }
            position
        } else {
            let position = extra;
            extra += 1;
            position
        });
    }
    // Incomplete or malformed exchanges remain visible to the validator. Only a
    // complete bijection is permuted; no response or signed native part is lost.
    if !seen.into_iter().all(|seen| seen) {
        return;
    }
    for index in 0..ranks.len() {
        while ranks[index] != index {
            let destination = ranks[index];
            ranks.swap(index, destination);
            results.swap(index, destination);
        }
    }
}
