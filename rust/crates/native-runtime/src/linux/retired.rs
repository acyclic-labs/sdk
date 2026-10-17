use super::*;

pub(super) enum RetiredKey {
    Read(Key<OwnedReadOp>),
    Write(Key<OwnedWriteOp>),
}

pub(super) struct RetiredProactor {
    driver: Proactor,
    key: Option<RetiredKey>,
}
impl Drop for RetiredProactor {
    fn drop(&mut self) {
        if self.key.is_some() {
            std::process::abort();
        }
    }
}
impl RetiredProactor {
    pub(super) fn new(driver: Proactor, key: RetiredKey) -> Self {
        Self {
            driver,
            key: Some(key),
        }
    }

    pub(super) fn poll_terminal(&mut self) -> bool {
        let _ = self.driver.poll(Some(Duration::ZERO));
        self.key = match self.key.take() {
            Some(RetiredKey::Read(key)) => match self.driver.pop(key) {
                PushEntry::Ready(_) => None,
                PushEntry::Pending(key) => Some(RetiredKey::Read(key)),
            },
            Some(RetiredKey::Write(key)) => match self.driver.pop(key) {
                PushEntry::Ready(_) => None,
                PushEntry::Pending(key) => Some(RetiredKey::Write(key)),
            },
            None => None,
        };
        self.key.is_none()
    }

    /// Blocks on the ring until the retained request is terminal, without a wall-clock bound.
    #[cfg(test)]
    pub(super) fn wait_terminal(&mut self) -> io::Result<()> {
        while !self.poll_terminal() {
            self.driver.poll(None)?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "retired_tests.rs"]
mod tests;
