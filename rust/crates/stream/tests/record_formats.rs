//! External hosted voters consume the canonical format contract at the crate root.
use acyclic_stream_wire::changes::{READER_FORMATS, WRITER_FORMAT};

const _: &[&[u8]] = READER_FORMATS;
const _: &[u8] = WRITER_FORMAT;
