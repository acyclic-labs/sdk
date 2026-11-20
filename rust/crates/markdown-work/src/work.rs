//! Test-only work accounting for the incremental Markdown parser: bytes
//! scanned, copied, made visible and parsed, plus source growth. It lives
//! beside the `record!` counters so a split crate keeps the same accounting
//! across the crate boundary.

use std::cell::Cell;

thread_local! {
    static SOURCE_GROWTH: Cell<usize> = const { Cell::new(0) };
    static SCAN: Cell<usize> = const { Cell::new(0) };
    static COPY: Cell<usize> = const { Cell::new(0) };
    static VISIBILITY: Cell<usize> = const { Cell::new(0) };
    static PARSED_BYTES: Cell<usize> = const { Cell::new(0) };
}

/// Account for bytes copied into retained state.
pub fn copy(bytes: usize) {
    COPY.set(COPY.get() + bytes);
}

/// Account for one visibility update.
pub fn visibility(count: usize) {
    VISIBILITY.set(VISIBILITY.get() + count);
}

/// Total work on this thread: scanned, copied, made visible and parsed bytes.
pub fn current() -> usize {
    SCAN.get() + COPY.get() + VISIBILITY.get() + PARSED_BYTES.get()
}

/// Account for bytes handed to a parser pass.
pub fn parsed(bytes: usize) {
    PARSED_BYTES.set(PARSED_BYTES.get() + bytes);
}

/// Bytes handed to parser passes on this thread.
pub fn parsed_bytes() -> usize {
    PARSED_BYTES.get()
}

/// Bytes appended to the source on this thread.
pub fn source_growth() -> usize {
    SOURCE_GROWTH.get()
}

/// Account for bytes appended to the source.
pub fn grow_source(bytes: usize) {
    copy(bytes);
    SOURCE_GROWTH.set(SOURCE_GROWTH.get() + bytes);
}

/// Account for bytes scanned.
pub fn scan(bytes: usize) {
    SCAN.set(SCAN.get() + bytes);
}

/// Bytes appended to a geometrically growing buffer. Reallocation at least
/// doubles capacity, so by the time `n` bytes have been appended the moves
/// total less than `2n`. Prepaying that on append keeps measured work a
/// smooth upper bound instead of a step at each reallocation, whose size
/// depends on where a length falls between powers of two.
pub fn grow(bytes: usize) {
    copy(2 * bytes);
}
