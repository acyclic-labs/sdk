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

    /// Adoption fixes drop order once for every resource: the native owner is
    /// released before its original accounting callback can run or unwind.
    pub(in crate::process_tree) fn adopt<T>(self, resource: T) -> Resource<'a, T> {
        Resource {
            resource,
            _released: Released(self),
        }
    }

    fn record(&mut self) -> io::Result<()> {
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

/// Fields are dropped in declaration order, including during unwinding.
/// No custom destructor may publish completion ahead of the native owner.
pub(in crate::process_tree) struct Resource<'a, T> {
    resource: T,
    _released: Released<'a>,
}

impl<T> std::ops::Deref for Resource<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.resource
    }
}

struct Released<'a>(Cleanup<'a>);

impl Drop for Released<'_> {
    fn drop(&mut self) {
        // Preserve an existing unwind while completing this original callback.
        // Normal callback failure still unwinds after the native owner released.
        crate::delivery::deliver(std::iter::once(()), |()| {
            let _ = self.0.record();
        });
    }
}
