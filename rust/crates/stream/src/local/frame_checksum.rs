/// The journal's frame, in the shared local log layout: a four-byte length,
/// the commit time and command, and their checksum.
const JOURNAL_FRAME: acyclic_log_frame::LogFrame = acyclic_log_frame::LogFrame {
    domain: b"acyclic-stream-local-frame-v1\0",
    wide: false,
    minimum_body: FRAME_TIME_BYTES as u64 + 1,
    maximum_body: (FRAME_TIME_BYTES + MAX_COMMAND_BYTES) as u64,
};
