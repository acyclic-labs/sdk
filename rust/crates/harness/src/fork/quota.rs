//! quota fork protocol.
use super::*;
pub(crate) use crate::wire_codec::CodecQuota;
impl<'a, 'b> CodecQuota<'a, 'b> {
    pub(crate) fn observed(work: &'a mut Option<&'b mut dyn crate::store::history::HistoryIo>) -> Result<Self> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        Ok(Self::from_retained(work, 0))
    }

    pub(crate) fn invalid(&mut self, reason: &'static str) -> Error {
        boundary_error(self.work(), crate::wire::ErrorCode::Invalid, reason)
    }

    pub(crate) fn checkpoint(&mut self) -> Result<()> {
        if let Some(observer) = self.work().as_deref_mut() {
            observer.check_cancellation()?;
            observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        }
        Ok(())
    }

    pub(crate) fn retain(&mut self, bytes: u64) -> Result<()> {
        if self.work().is_none() { return Ok(()); }
        let retained = self.retained().checked_add(bytes)
            .ok_or_else(|| self.invalid("fork validation storage overflows"))?;
        self.replace(retained)
    }

    pub(crate) fn release(&mut self, bytes: u64) -> Result<()> {
        if self.work().is_none() { return Ok(()); }
        self.retained().checked_sub(bytes)
            .ok_or_else(|| self.invalid("fork validation storage underflows"))?;
        self.relinquish(bytes)
    }

    pub(crate) fn slots<T>(&mut self, count: usize) -> Result<()> {
        let bytes = (count as u64).checked_mul(std::mem::size_of::<T>() as u64)
            .ok_or_else(|| self.invalid("fork validation slot count overflows"))?;
        self.retain(bytes)
    }

    pub(crate) fn canonical<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<Vec<u8>> {
        let bytes = crate::store::history::canonical_bytes_with_work(
            value, crate::store::default_projection_read_limits().maximum_bytes, self.work(),
        )?;
        self.adopt(bytes.capacity() as u64)?;
        Ok(bytes)
    }

    pub(super) fn file_digest(&mut self, file: &FileRef) -> Result<[u8; 32]> {
        file.validate(self.work())?;
        crate::contract::canonical_json_digest_with_work(file, crate::store::default_projection_read_limits().maximum_bytes, self.work())
    }

    pub(in crate::fork) fn stream_path(&mut self, authority: &Authority) -> Result<String> {
        let path = authority.stream_path(self.work())?;
        self.adopt(path.capacity() as u64)?;
        Ok(path)
    }
}
