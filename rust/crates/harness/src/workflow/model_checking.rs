#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Fault {
    None,
    SkipCursor,
    SplitCommit,
}

loom::thread_local! {
    static FAULT: std::cell::Cell<Fault> = std::cell::Cell::new(Fault::None);
}

pub(super) fn fault_is(fault: Fault) -> bool {
    FAULT.with(|current| current.get() == fault)
}

#[cfg(test)]
mod tests;
