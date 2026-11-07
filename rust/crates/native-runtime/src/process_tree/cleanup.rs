//! Prepaid physical cleanup custody shared by native process resources.
use super::*;

pub(in crate::process_tree) struct Cleanup<'a> {
    meter: Option<&'a dyn crate::NativeReadMeter>,
    token: Option<u64>,
}

impl<'a> Cleanup<'a> {
    pub(in crate::process_tree) fn new(
        meter: Option<&'a dyn crate::NativeReadMeter>,
    ) -> io::Result<Self> {
        Ok(Self {
            meter,
            token: meter
                .map(crate::NativeReadMeter::reserve_cleanup_write)
                .transpose()?,
        })
    }

    pub(in crate::process_tree) fn record(&mut self) -> io::Result<()> {
        self.token.take().map_or(Ok(()), |token| {
            self.meter
                .ok_or(io::ErrorKind::InvalidInput)?
                .record_cleanup_write(token)
        })
    }
}

impl Drop for Cleanup<'_> {
    fn drop(&mut self) {
        if let (Some(meter), Some(token)) = (self.meter, self.token.take()) {
            meter.cancel_cleanup_write(token);
        }
    }
}
