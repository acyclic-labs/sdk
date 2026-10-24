/// Exclusive-process durable local provider backed by a snapshot and a checksummed command
#[derive(Clone)]
pub struct LocalStream {
    inner: Arc<LocalInner>,
    work: Option<Arc<crate::memory::Work>>,
}
