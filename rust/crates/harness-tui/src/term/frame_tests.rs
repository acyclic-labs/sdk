use super::*;
use crate::term::{Capabilities, ModeState, RestoreGuard};

const CHUNK: usize = 256 * 1024;
const BEGIN: &[u8] = b"\x1b[?2026h";
const END: &[u8] = b"\x1b[?2026l";

fn synchronized() -> Capabilities {
    let mut caps = Capabilities::default();
    caps.modes.insert(2026, ModeState::Reset);
    caps
}

/// Present `len` bytes in calls of at most `chunk`; return each call's size
/// and the device bytes.
fn parts(len: usize, chunk: usize) -> (Vec<usize>, Vec<u8>) {
    let frame: Vec<u8> = (0..len).map(|index| b'a' + (index % 26) as u8).collect();
    let mut device = Vec::new();
    let mut sizes = Vec::new();
    {
        let mut guard = RestoreGuard::new(&mut device, &synchronized());
        present_parts(&frame, chunk, |part| {
            sizes.push(part.bytes.len());
            part.apply(&mut guard, false)
        })
        .unwrap_or_else(|error| panic!("{error}"));
        assert!(
            !guard.frame_open(),
            "the update stays open after the last part"
        );
    }
    assert_eq!(device, [BEGIN, &frame, END].concat());
    (sizes, device)
}

fn assert_bounded(sizes: &[usize], len: usize) {
    assert!(
        sizes.iter().all(|size| *size <= CHUNK),
        "bounded console call: {sizes:?}"
    );
    assert_eq!(sizes.len(), len.div_ceil(CHUNK), "console calls");
}

// Property: any frame, up to 16 MiB, reaches the device as ceil(len/chunk)
// calls of at most one chunk each, inside one synchronized update, with the
// exact bytes and nothing left journaled.
#[test]
fn large_frames_are_bounded_calls_in_one_update() {
    for len in [
        1,
        CHUNK - 1,
        CHUNK,
        CHUNK + 1,
        3 * CHUNK,
        16 * 1024 * 1024 + 7,
    ] {
        let (sizes, _) = parts(len, CHUNK);
        assert_bounded(&sizes, len);
    }
}

#[test]
#[should_panic(expected = "bounded console call")]
fn negative_control_one_unbounded_call_is_detected() {
    let len = 16 * 1024 * 1024;
    let (sizes, _) = parts(len, usize::MAX);
    assert_bounded(&sizes, len);
}

/// A device whose `fail`th write call times out.
struct FailingCall {
    bytes: Vec<u8>,
    calls: usize,
    fail: usize,
}
impl Write for FailingCall {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.calls += 1;
        if self.calls == self.fail {
            return Err(io::ErrorKind::TimedOut.into());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// Property: a call failing at any part leaves the update journaled, and the
// bounded abort closes it at once.
#[test]
fn a_failed_part_is_closed_by_the_abort() {
    let frame = vec![b'x'; 4 * CHUNK];
    for fail in 1..=4 {
        let device = FailingCall {
            bytes: Vec::new(),
            calls: 0,
            fail,
        };
        let mut guard = RestoreGuard::new(device, &synchronized());
        let result = present_parts(&frame, CHUNK, |part| part.apply(&mut guard, true));
        assert!(result.is_err());
        assert!(
            guard.frame_open(),
            "part {fail}: the failed update is journaled"
        );
        guard
            .abort_frame()
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(!guard.frame_open());
        assert!(guard.writer().bytes.ends_with(b"\x18\x1b[?2026l"));
    }
}

/// Frames whose character straddles the part limit at every offset, for
/// every UTF-8 width.
fn straddling() -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    for character in ["\u{e9}", "\u{754c}", "\u{1f600}"] {
        for before in CHUNK - character.len()..=CHUNK {
            frames.push(
                [
                    vec![b'a'; before],
                    character.as_bytes().to_vec(),
                    vec![b'b'; 9],
                ]
                .concat(),
            );
        }
    }
    frames
}

fn assert_whole_characters(frame: &[u8], calls: &[Vec<u8>]) {
    assert_eq!(calls.concat(), frame, "exact bytes");
    for call in calls {
        assert!(call.len() <= CHUNK, "bounded console call");
        assert!(
            std::str::from_utf8(call).is_ok(),
            "a console call split a character"
        );
    }
}

// Property: no console call splits a character, every call stays bounded,
// and a straddling frame still costs exactly two calls.
#[test]
fn parts_end_on_character_boundaries() {
    for frame in straddling() {
        let mut calls = Vec::new();
        present_parts(&frame, CHUNK, |part| {
            calls.push(part.bytes);
            Ok(())
        })
        .unwrap_or_else(|error| panic!("{error}"));
        assert_whole_characters(&frame, &calls);
        assert_eq!(calls.len(), 2);
    }
    assert_eq!(char_prefix(b"ab", 0), 0);
    assert!(present_parts(b"ab", 0, |_| Ok(())).is_err());
}

#[test]
#[should_panic(expected = "split a character")]
fn negative_control_byte_chunks_split_a_character() {
    for frame in straddling() {
        let calls: Vec<Vec<u8>> = frame.chunks(CHUNK).map(<[u8]>::to_vec).collect();
        assert_whole_characters(&frame, &calls);
    }
}
