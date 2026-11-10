use super::*;

#[cfg(feature = "filesystem")]
#[derive(Clone, Copy)]
#[doc(hidden)]
pub enum JournalWrite {
    Fresh,
    Model {
        attempt_id: OperationId,
        step: u32,
        request_digest: [u8; 32],
    },
    Settlement,
}
