use std::cell::Cell;
thread_local! {
    static SOURCE_GROWTH: Cell<usize> = const { Cell::new(0) };
    static SCAN: Cell<usize> = const { Cell::new(0) };
    static COPY: Cell<usize> = const { Cell::new(0) };
    static VISIBILITY: Cell<usize> = const { Cell::new(0) };
}
pub(crate) fn copy(bytes: usize) {
    COPY.set(COPY.get() + bytes);
}
pub(crate) fn visibility(count: usize) {
    VISIBILITY.set(VISIBILITY.get() + count);
}
pub(crate) fn current() -> usize {
    SCAN.get() + COPY.get() + VISIBILITY.get() + crate::references::PARSED_BYTES.get()
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
    SOURCE_GROWTH.get()
}
pub(crate) fn grow_source(bytes: usize) {
    copy(bytes);
    SOURCE_GROWTH.set(SOURCE_GROWTH.get() + bytes);
}

pub(crate) fn scan(bytes: usize) {
    SCAN.set(SCAN.get() + bytes);
}

/// Bytes appended to a geometrically growing buffer. Reallocation at least
/// doubles capacity, so by the time `n` bytes have been appended the moves
/// total less than `2n`. Prepaying that on append keeps measured work a
/// smooth upper bound instead of a step at each reallocation, whose size
/// depends on where a length falls between powers of two.
pub(crate) fn grow(bytes: usize) {
    copy(2 * bytes);
}
