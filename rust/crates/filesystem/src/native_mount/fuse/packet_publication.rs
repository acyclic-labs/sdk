//! One physical completion boundary for every packet transport.

use super::NativeMountError;
use super::packet::{OUTPUT_HEADER, PacketOutput, PacketRequest};
use super::packet_dispatch::PacketFailure;
use super::packet_session::PacketSession;

impl PacketSession {
    /// The publisher must complete the transport's descriptor/device operation
    /// before returning success. `None` completes a no-reply operation without
    /// fabricating a FUSE response. It is still a physical completion boundary.
    pub(super) fn publish(
        &self,
        request: &PacketRequest<'_>,
        response: &mut [u8],
        publish: impl FnOnce(Option<&[u8]>) -> Result<(), NativeMountError>,
    ) -> Result<bool, PacketFailure> {
        match self.dispatch(request, response) {
            Ok(reply) => {
                let bytes = reply
                    .length()
                    .map(|length| response.get(..length).ok_or(libc::EPROTO))
                    .transpose()?;
                // On failure the pending reply closes the session before its
                // original work and projection leases can be released.
                publish(bytes)?;
                reply.delivered();
                Ok(request.opcode != 38)
            }
            Err(error) => {
                self.reject(request, response, error.errno(), publish)?;
                if error.is_terminal() || request.opcode == 26 {
                    self.close();
                    Err(error)
                } else {
                    Ok(true)
                }
            }
        }
    }

    /// Complete a refused request through the same transport boundary.
    pub(super) fn reject(
        &self,
        request: &PacketRequest<'_>,
        response: &mut [u8],
        errno: i32,
        publish: impl FnOnce(Option<&[u8]>) -> Result<(), NativeMountError>,
    ) -> Result<(), PacketFailure> {
        let result = (|| {
            let bytes = if matches!(request.opcode, 2 | 42) {
                None
            } else {
                PacketOutput::new(response, 0)?.finish(request.unique, Some(errno))?;
                Some(response.get(..OUTPUT_HEADER).ok_or(libc::EPROTO)?)
            };
            publish(bytes)?;
            Ok(())
        })();
        if result.is_err() {
            // A negative reply can also have uncertain delivery. The caller
            // must retire this session rather than reuse it for another try.
            self.close();
        }
        result
    }
}
