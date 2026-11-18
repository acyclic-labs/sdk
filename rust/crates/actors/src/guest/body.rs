//! Project the already-admitted invocation fields into the portable HTTP body.
use super::{GuestBody, GuestStreamingRequest};
use acyclic_actors_guest_wire::streaming_request;

macro_rules! invocation_fetch {
    ($($header:ident),+ $(,)?) => {$(
        impl crate::domain::$header {
            /// Attach the original body after the serving host admits this
            /// header's target, ordering, bindings and caller provenance.
            ///
            /// Consumes HTTP metadata through the same conversion as buffered
            /// guest requests. It neither polls nor replaces the body. The
            /// original invocation owner must retain authority and context;
            /// this projection performs no admission or handler dispatch.
            pub fn into_streaming(self, body: GuestBody) -> Result<GuestStreamingRequest, http::Error> {
                let (method, url, headers) = self.into_http_parts();
                streaming_request(
                    &method,
                    url,
                    headers.into_iter().map(|header| {
                        let header: crate::wire::Header = header.into();
                        (header.name, header.value)
                    }),
                    body,
                )
            }
        }
    )+};
}
invocation_fetch!(InvokeActorHeader, InvokeWorkerHeader);
