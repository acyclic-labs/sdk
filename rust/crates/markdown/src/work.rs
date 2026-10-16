use std::cell::Cell;
thread_local! {
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
    COPY.get() + VISIBILITY.get() + crate::references::PARSED_BYTES.get()
}
