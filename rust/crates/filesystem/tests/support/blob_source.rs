use acyclic_fs::CancellationToken;
use acyclic_fs::kernel::AsyncBlobSource;
use acyclic_fs_work::performance::WorkBudget;
use acyclic_stream::OwnedBytes;

pub(super) struct ScriptedSource {
    pub(super) bytes: OwnedBytes,
    pub(super) position: usize,
    pub(super) maximum_per_read: usize,
    pub(super) fail_after: Option<usize>,
    pub(super) cancel_at: Option<usize>,
}

impl AsyncBlobSource for ScriptedSource {
    async fn read<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        _budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        if self.fail_after.is_some_and(|limit| self.position >= limit) {
            return Err(std::io::Error::other("scripted source failure"));
        }
        if self.position == self.bytes.len() {
            return Ok(0);
        }
        let count = self
            .maximum_per_read
            .min(destination.len())
            .min(self.bytes.len() - self.position);
        let source = self
            .bytes
            .get(self.position..self.position + count)
            .ok_or(std::io::ErrorKind::InvalidData)?;
        destination
            .get_mut(..count)
            .ok_or(std::io::ErrorKind::InvalidData)?
            .copy_from_slice(source);
        self.position += count;
        if self.cancel_at.is_some_and(|offset| self.position >= offset) {
            cancellation.cancel();
        }
        Ok(count)
    }
}
