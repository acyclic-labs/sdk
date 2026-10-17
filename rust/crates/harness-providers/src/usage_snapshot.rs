use crate::sync::Mutex;

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub(crate) struct Usage {
    pub(crate) digest: [u8; 32],
    pub(crate) messages: usize,
    pub(crate) bytes: u64,
    pub(crate) tokens: u64,
}

/// Standalone lock: never acquire another lock or perform I/O while holding it.
#[derive(Default)]
pub(crate) struct Snapshot(Mutex<Option<Usage>>);
impl Snapshot {
    pub(crate) fn get(&self) -> Result<Option<Usage>, ()> {
        self.0.lock().map(|snapshot| *snapshot).map_err(|_| ())
    }
    pub(crate) fn set(&self, usage: Usage) -> Result<(), ()> {
        *self.0.lock().map_err(|_| ())? = Some(usage);
        Ok(())
    }
}
