use super::{ProcessResources, sample_process_resources};
use std::{io, sync::mpsc, thread, time::Duration};

/// A sampling thread whose lifetime is owned by its caller, including unwind
/// and cancellation. Dropping the sender wakes it; joining retires its work.
pub struct ResourceSampler {
    stop: Option<mpsc::Sender<()>>,
    observer: Option<thread::JoinHandle<io::Result<ProcessResources>>>,
}

impl ResourceSampler {
    /// Samples real process counters until explicitly finished or dropped.
    pub fn start(baseline: ProcessResources, interval: Duration) -> io::Result<Self> {
        Self::start_with(baseline, interval, sample_process_resources)
    }

    fn start_with(
        mut peak: ProcessResources,
        interval: Duration,
        mut sample: impl FnMut() -> io::Result<ProcessResources> + Send + 'static,
    ) -> io::Result<Self> {
        if interval.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "resource sampling requires a nonzero interval",
            ));
        }
        let (stop, receiver) = mpsc::channel();
        let observer = thread::Builder::new()
            .name("resource-sampler".into())
            .spawn(move || {
                loop {
                    let now = sample()?;
                    peak.memory_bytes = peak.memory_bytes.max(now.memory_bytes);
                    peak.threads = peak.threads.max(now.threads);
                    peak.descriptors = peak.descriptors.max(now.descriptors);
                    peak.mounts = peak.mounts.max(now.mounts);
                    peak.watches = peak.watches.max(now.watches);
                    peak.cpu_centiseconds = peak.cpu_centiseconds.max(now.cpu_centiseconds);
                    match receiver.recv_timeout(interval) {
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                        Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(peak),
                    }
                }
            })?;
        Ok(Self {
            stop: Some(stop),
            observer: Some(observer),
        })
    }

    fn stop(&mut self) -> Option<io::Result<ProcessResources>> {
        drop(self.stop.take());
        self.observer.take().map(|observer| {
            observer
                .join()
                .map_err(|_| io::Error::other("resource sampler panicked"))?
        })
    }

    /// Stops and joins the observer, propagating its sampling or panic error.
    pub fn finish(mut self) -> io::Result<ProcessResources> {
        self.stop()
            .ok_or_else(|| io::Error::other("resource sampler observer already retired"))?
    }
}

impl Drop for ResourceSampler {
    fn drop(&mut self) {
        // A cancelled future has no receiver for measurement errors, but its
        // observer must still retire before the future's remaining state does.
        let _ = self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn dropping_the_sampler_joins_its_real_observer() -> io::Result<()> {
        let lifetime = Arc::new(());
        let weak = Arc::downgrade(&lifetime);
        let (ready, started) = mpsc::channel();
        let sampler = ResourceSampler::start_with(
            super::super::process_resources()?,
            // Retirement must wake the observer, rather than waiting for the
            // next sampling period. The owner still has the normal test budget.
            Duration::from_secs(3_600),
            move || {
                let _lifetime = &lifetime;
                let sampled = sample_process_resources()?;
                let _ = ready.send(());
                Ok(sampled)
            },
        )?;
        started.recv().map_err(io::Error::other)?;
        assert!(weak.upgrade().is_some());
        drop(sampler);
        assert!(weak.upgrade().is_none(), "observer outlived its owner");
        Ok(())
    }

    #[test]
    fn finishing_returns_real_high_water_counters() -> io::Result<()> {
        let baseline = super::super::process_resources()?;
        let peak = ResourceSampler::start(baseline, Duration::from_millis(100))?.finish()?;
        assert!(peak.memory_bytes >= baseline.memory_bytes);
        assert!(peak.threads >= baseline.threads);
        assert!(peak.descriptors >= baseline.descriptors);
        assert!(peak.mounts >= baseline.mounts);
        Ok(())
    }
}
