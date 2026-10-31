//! One physical completion boundary for every packet transport.

use super::NativeMountError;
use super::packet::{OUTPUT_HEADER, PacketOutput, PacketRequest};
use super::packet_dispatch::PacketFailure;
use super::packet_response::PacketResponse;
use super::packet_session::PacketSession;

impl PacketSession {
    /// The publisher must complete the transport's descriptor/device operation
    /// before returning success. A response without slices completes a no-reply
    /// operation without fabricating a FUSE response. It remains a physical
    /// completion boundary, retaining original funded payload and view custody.
    pub(super) fn publish(
        &self,
        request: &PacketRequest<'_>,
        response: &mut [u8],
        publish: impl FnOnce(PacketResponse<'_>) -> Result<(), NativeMountError>,
    ) -> Result<bool, PacketFailure> {
        match self.dispatch(request, response) {
            Ok(reply) => {
                // On failure the pending reply closes the session before its
                // original work and projection leases can be released.
                publish(PacketResponse::new(
                    response,
                    reply.length(),
                    reply.payload(),
                )?)?;
                reply.delivered();
                Ok(request.opcode != 38)
            }
            Err(error) => {
                self.reject(request, response, error.errno(), publish)?;
                let failed_view =
                    matches!(error, PacketFailure::Source { .. }) && self.single_core().is_none();
                if (error.is_terminal() && !failed_view) || request.opcode == 26 {
                    // Keep the original view and work through the physical
                    // negative reply before starting terminal retirement.
                    self.retire_failure(&error);
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
        publish: impl FnOnce(PacketResponse<'_>) -> Result<(), NativeMountError>,
    ) -> Result<(), PacketFailure> {
        let result = (|| {
            let length = if matches!(request.opcode, 2 | 42) {
                None
            } else {
                PacketOutput::new(response, 0)?.finish(request.unique, Some(errno))?;
                Some(OUTPUT_HEADER)
            };
            publish(PacketResponse::new(response, length, None)?)?;
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
