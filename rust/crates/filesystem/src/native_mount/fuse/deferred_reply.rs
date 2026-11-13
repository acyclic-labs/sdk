//! A reply whose prerequisite cache notification must physically complete first.

use super::host_session::PendingReply;
use super::invalidation::KernelCacheItem;
use super::packet::PacketRequest;
use super::packet_dispatch::PacketCompletion;
use super::packet_response::PacketResponse;
use super::packet_session::PacketReply;
use super::*;
use acyclic_native_runtime::PacketDevicePublication;

pub(super) struct DeferredReply {
    pending: Option<PendingReply>,
    completion: Option<PacketCompletion>,
    request: FuseOwned<Vec<u8>>,
    response: FuseOwned<Vec<u8>>,
    core: Arc<ProjectionCore>,
    #[cfg(test)]
    capture: Option<(Arc<super::tests::packets::Capture>, u64)>,
    _allocation: crate::NativeAllocation,
}

impl std::fmt::Debug for DeferredReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DeferredReply")
    }
}

impl DeferredReply {
    pub(super) fn enqueue(
        reply: &mut PacketReply<'_>,
        request: &[u8],
        response: &[u8],
        pending: PendingReply,
        #[cfg(test)] capture: Option<(Arc<super::tests::packets::Capture>, u64)>,
    ) -> Result<bool, NativeMountError> {
        let core = Arc::clone(reply.core().ok_or(libc::EPROTO)?);
        let work = reply.completion().work().ok_or(libc::EPROTO)?.clone();
        let request = fuse_copy_bytes(request, &work)?;
        let length = reply.length().ok_or(libc::EPROTO)?;
        let response = fuse_copy_bytes(response.get(..length).ok_or(libc::EPROTO)?, &work)?;
        let allocation = work.claim(
            std::mem::size_of::<Self>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        let mut state = core.state()?;
        if state.invalidation.denied || core.handler_failed.load(Ordering::Acquire) {
            return Err(libc::ENODEV.into());
        }
        state.reserve_invalidations(1, &work)?;
        // Stage the dependency before transferring the original obligation.
        // A pass already publishing its attributes finishes before this pass;
        // later notifications cannot extend this finite publication cut.
        state.queue_superseded_attributes();
        let completion = reply.take_completion()?;
        state
            .invalidation
            .deferred
            .push(KernelCacheItem::Reply(Box::new(Self {
                pending: Some(pending),
                completion: Some(completion),
                request,
                response,
                core: Arc::clone(&core),
                #[cfg(test)]
                capture,
                _allocation: allocation,
            })));
        #[cfg(all(test, not(loom)))]
        if let Some(queued) = core
            .publication_test
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .queued
            .take()
        {
            let _ = queued.send(());
        }
        Ok(true)
    }

    pub(super) fn deliver(mut self: Box<Self>) -> Result<(), NativeMountError> {
        let completion = self.completion.as_ref().ok_or(libc::EPROTO)?;
        let response = PacketResponse::new(
            &mut self.response,
            completion.length,
            completion
                .payload
                .as_ref()
                .map(|payload| payload.bytes.as_ref()),
        )?;
        let outcome = self
            .pending
            .as_mut()
            .ok_or(libc::EPROTO)?
            .publish(&response.slices().ok_or(libc::EPROTO)?)?;
        #[cfg(test)]
        if outcome == PacketDevicePublication::Published
            && let Some((capture, sequence)) = &self.capture
        {
            capture.completed(*sequence, &self.request, response.contiguous());
        }
        match outcome {
            PacketDevicePublication::Published => {}
            PacketDevicePublication::Rejected(libc::ENOENT) => {
                let request = PacketRequest::parse(&self.request)?;
                completion.discard_rejected(&self.core, &request, &self.response)?;
            }
            PacketDevicePublication::Rejected(errno) => return Err(errno.into()),
        }
        // Physical completion, including certified kernel rejection, releases
        // the original view, handle, work and packet storage together.
        self.completion.take();
        Ok(())
    }
}

impl Drop for DeferredReply {
    fn drop(&mut self) {
        if self.pending.as_ref().is_some_and(PendingReply::owes_reply) {
            // Finish this exact kernel request before releasing its view lease.
            // No successful OPEN was attempted, so its handle is unobserved.
            drop(self.pending.take());
            if let Some(completion) = &self.completion
                && let Ok(request) = PacketRequest::parse(&self.request)
            {
                let _ = completion.discard_rejected(&self.core, &request, &self.response);
            }
        }
    }
}
