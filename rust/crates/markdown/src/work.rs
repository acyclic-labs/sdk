use std::cell::Cell;
thread_local! {
    pub(crate) static CACHE_JOINS: Cell<bool> = const { Cell::new(false) };
    pub(crate) static QUADRUPLE_TOKEN_PAGE: Cell<bool> = const { Cell::new(false) };
    pub(crate) static EAGER_RENDER_PAGE: Cell<bool> = const { Cell::new(false) };
    static COUNTERS: Counters = const { Counters {
        parts: [const { Cell::new(0) }; 4],
        source_growth: Cell::new(0),
        rendered: Cell::new(0),
        rendering_parts: Cell::new([0; 4]),
        rendering: Cell::new(false),
    } };
}

// One TLS lookup per charge, including its optional rendering breakdown.
// The measured operations and weights are unchanged.
struct Counters {
    parts: [Cell<usize>; 4],
    source_growth: Cell<usize>,
    rendered: Cell<usize>,
    rendering_parts: Cell<[usize; 4]>,
    rendering: Cell<bool>,
}

#[inline(always)]
fn charge(part: usize, count: usize, rendering_part: usize, growth: bool) {
    COUNTERS.with(|counters| {
        if let Some(total) = counters.parts.get(part) {
            total.set(total.get() + count);
        }
        if growth {
            counters
                .source_growth
                .set(counters.source_growth.get() + count);
        }
        if counters.rendering.get() {
            counters.rendered.set(counters.rendered.get() + count);
            let mut parts = counters.rendering_parts.get();
            if let Some(value) = parts.get_mut(rendering_part) {
                *value += count;
            }
            counters.rendering_parts.set(parts);
        }
    });
}

pub(crate) fn copy(bytes: usize) {
    charge(1, bytes, 0, false);
}
pub(crate) fn visibility(count: usize) {
    charge(2, count, 1, false);
}
pub(crate) fn visibility_current() -> usize {
    COUNTERS.with(|counters| counters.parts.get(2).map_or(0, Cell::get))
}
pub(crate) fn parts() -> [usize; 5] {
    COUNTERS.with(|counters| {
        let [scan, copy, visibility, feed] = counters.parts.each_ref().map(Cell::get);
        [
            scan,
            copy,
            visibility,
            feed,
            crate::references::PARSED_BYTES.get(),
        ]
    })
}
pub(crate) fn current() -> usize {
    parts().into_iter().sum()
}
pub(crate) fn render_feed(bytes: usize) {
    charge(3, bytes, 2, false);
}
pub(crate) fn rendered_bytes() -> usize {
    COUNTERS.with(|counters| counters.parts.get(3).map_or(0, Cell::get))
}
pub(crate) fn rendering(active: bool) -> bool {
    COUNTERS.with(|counters| counters.rendering.replace(active))
}
pub(crate) fn rendering_parts() -> [usize; 4] {
    COUNTERS.with(|counters| counters.rendering_parts.get())
}
pub(crate) fn rendering_work() -> usize {
    COUNTERS.with(|counters| counters.rendered.get())
}

pub(crate) fn event(event: &pulldown_cmark::Event<'_>) {
    use pulldown_cmark::{CodeBlockKind, Event, Tag};
    copy(match event {
        Event::Text(text)
        | Event::Code(text)
        | Event::Html(text)
        | Event::InlineHtml(text)
        | Event::InlineMath(text)
        | Event::DisplayMath(text)
        | Event::FootnoteReference(text) => text.len(),
        Event::Start(
            Tag::Link {
                dest_url,
                title,
                id,
                ..
            }
            | Tag::Image {
                dest_url,
                title,
                id,
                ..
            },
        ) => dest_url.len() + title.len() + id.len(),
        Event::Start(Tag::CodeBlock(CodeBlockKind::Fenced(info))) => info.len(),
        _ => 0,
    });
}

pub(crate) fn source_growth() -> usize {
    COUNTERS.with(|counters| counters.source_growth.get())
}
pub(crate) fn grow_source(bytes: usize) {
    charge(1, bytes, 0, true);
}
pub(crate) fn scan(bytes: usize) {
    charge(0, bytes, 3, false);
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn charges_match_the_independent_accounting_model(
            actions in prop::collection::vec((0u8..6, 0usize..128), 0..128),
        ) {
            let old_active = rendering(false);
            let mut expected = parts();
            let mut growth = source_growth();
            let mut rendered = rendering_work();
            let mut breakdown = rendering_parts();
            let mut active = false;
            for (action, count) in actions {
                let (part, rendering_part) = match action {
                    0 => { scan(count); (0, 3) }
                    1 => { copy(count); (1, 0) }
                    2 => { visibility(count); (2, 1) }
                    3 => { render_feed(count); (3, 2) }
                    4 => { grow_source(count); growth += count; (1, 0) }
                    _ => {
                        let previous = active;
                        active = count % 2 == 1;
                        prop_assert_eq!(rendering(active), previous);
                        continue;
                    }
                };
                *expected.get_mut(part).unwrap() += count;
                if active {
                    rendered += count;
                    *breakdown.get_mut(rendering_part).unwrap() += count;
                }
                prop_assert_eq!(parts(), expected);
                prop_assert_eq!(current(), expected.into_iter().sum::<usize>());
                prop_assert_eq!(source_growth(), growth);
                prop_assert_eq!(rendering_work(), rendered);
                prop_assert_eq!(rendering_parts(), breakdown);
                prop_assert_eq!(visibility_current(), *expected.get(2).unwrap());
                prop_assert_eq!(rendered_bytes(), *expected.get(3).unwrap());
            }
            rendering(old_active);
        }
    }

    #[test]
    fn accounting_model_detects_an_omitted_charge() {
        let omitted = parts();
        visibility(7);
        assert!(std::panic::catch_unwind(|| assert_eq!(parts(), omitted)).is_err());
    }
}
