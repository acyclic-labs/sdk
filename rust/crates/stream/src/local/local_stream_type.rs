/// Exclusive-process durable local provider backed by a snapshot and a checksummed command
#[derive(Clone)]
#[doc = include_str!("format.md")]
pub struct LocalStream {
    inner: Arc<LocalInner>,
    work: Option<Arc<crate::memory::Work>>,
}
