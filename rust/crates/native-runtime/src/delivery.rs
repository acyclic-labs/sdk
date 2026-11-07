use std::panic::{AssertUnwindSafe, catch_unwind};

// The normal path invokes callbacks directly. If one unwinds, the same owned
// iterator retains every remaining delivery. A later consumer cannot unwind
// through that cleanup and abort the original producer's terminal fanout.
pub(crate) fn deliver<I: Iterator, F: FnMut(I::Item)>(items: I, invoke: F) {
    let mut delivery = Delivery { items, invoke };
    if std::thread::panicking() {
        delivery.finish();
    } else {
        for item in delivery.items.by_ref() {
            (delivery.invoke)(item);
        }
    }
}

struct Delivery<I: Iterator, F: FnMut(I::Item)> {
    items: I,
    invoke: F,
}

impl<I: Iterator, F: FnMut(I::Item)> Delivery<I, F> {
    fn finish(&mut self) {
        for item in self.items.by_ref() {
            let _ = catch_unwind(AssertUnwindSafe(|| (self.invoke)(item)));
        }
    }
}

impl<I: Iterator, F: FnMut(I::Item)> Drop for Delivery<I, F> {
    fn drop(&mut self) {
        self.finish();
    }
}

#[cfg(test)]
pub(crate) fn isolated() -> std::io::Result<bool> {
    const CHILD: &str = "ACYCLIC_TEST_NATIVE_DELIVERY";
    let current = std::thread::current();
    let name = current
        .name()
        .ok_or_else(|| std::io::Error::other("native delivery control has no test identity"))?;
    if std::env::var_os(CHILD).as_deref() == Some(std::ffi::OsStr::new(name)) {
        return Ok(false);
    }
    let status = std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", name, "--nocapture"])
        .env(CHILD, name)
        .status()?;
    if !status.success() {
        return Err(std::io::Error::other(format!(
            "native terminal delivery lost remaining consumers: {status}"
        )));
    }
    Ok(true)
}
