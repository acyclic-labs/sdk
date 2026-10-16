use std::collections::VecDeque;
use std::io;
use std::time::{Duration, Instant};

use acyclic_harness_tui::key_event::{InputEvent, KeyCode, KeyEvent, KeyModifiers};
use acyclic_harness_tui::term::{
    Decoder, Environment, Multiplexer, PROBE_LIMIT, PROBE_TIMEOUT, Probe, ProbeIo, detect,
};

struct Wire {
    packets: VecDeque<Vec<u8>>,
    offset: usize,
    read_max: usize,
    fail_at: Option<usize>,
    sends: usize,
    outer_seen: bool,
    gap: bool,
    broken: u8,
    sent: Vec<u8>,
}

impl ProbeIo for Wire {
    fn send(&mut self, bytes: &[u8], deadline: Instant) -> io::Result<()> {
        let limit = if self.sends == 0 {
            PROBE_TIMEOUT
        } else {
            Duration::from_millis(1)
        };
        assert!(deadline.saturating_duration_since(Instant::now()) <= limit);
        self.sends += 1;
        if self.sends == 1
            && let Some(at) = self.fail_at
        {
            self.sent.extend_from_slice(&bytes[..at.min(bytes.len())]);
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        if self.broken == 2 && self.sends > 1 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        self.sent.extend_from_slice(bytes);
        Ok(())
    }

    fn receive(&mut self, buffer: &mut [u8], _deadline: Instant) -> io::Result<usize> {
        if buffer.is_empty() || (self.broken == 1 && self.outer_seen) {
            return Ok(0);
        }
        if std::mem::take(&mut self.gap) {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let Some(packet) = self.packets.front() else {
            return Ok(0);
        };
        let native = self.read_max.min(packet.len() - self.offset);
        let copied = native.min(buffer.len());
        buffer[..copied].copy_from_slice(&packet[self.offset..self.offset + copied]);
        // Negative control models consuming a large native read then silently
        // discarding bytes that do not fit the caller's remaining capacity.
        self.offset += if self.broken == 3 { native } else { copied };
        if self.offset == packet.len() {
            self.outer_seen |= packet == b"\x1b[?62;1c";
            self.packets.pop_front();
            self.offset = 0;
            self.gap = !self.packets.is_empty();
        }
        Ok(copied)
    }
}

fn environment(mux: Multiplexer) -> Environment {
    let key = match mux {
        Multiplexer::Tmux => "TMUX",
        Multiplexer::Screen => "STY",
        Multiplexer::None => "TERM",
    };
    Environment([(key.into(), "session".into())].into())
}

pub fn run(
    mux: Multiplexer,
    ordering: &[u8],
    read_max: usize,
    fail_at: Option<usize>,
    broken: u8,
) -> Vec<u8> {
    let env = environment(mux);
    let identity = if mux == Multiplexer::Tmux {
        "tmux"
    } else {
        "screen"
    };
    let mut packets = vec![
        b"\x1b[?62;1c".to_vec(),
        format!("\x1bP>|{identity}\x1b\\").into_bytes(),
        b"\x1b[?2026;2$y".to_vec(),
        b"\x1b[?1004;2$y".to_vec(),
        b"\x1b]11;rgb:1010/2020/3030\x07".to_vec(),
    ];
    if mux == Multiplexer::Tmux {
        packets.extend([
            b"\x1bP1+r524742\x1b\\".to_vec(),
            b"\x1bP1+r536d756c78\x1b\\".to_vec(),
            b"\x1b_Gi=314159;OK\x1b\\".to_vec(),
        ]);
    }
    let mut reference = Decoder::default();
    for packet in &packets {
        assert!(reference.feed(packet).is_ok());
    }
    let expected = reference.replies().capabilities(&env);
    let reply_size: usize = packets.iter().map(Vec::len).sum();
    let mut ranks: Vec<_> = packets.drain(..).enumerate().collect();
    ranks.sort_by_key(|(index, _)| (ordering.get(*index).copied().unwrap_or_default(), *index));
    let mut packets: VecDeque<_> = ranks.into_iter().map(|(_, bytes)| bytes).collect();
    // Make the final startup read exactly 24 bytes while 100 remain in the
    // native packet. Remaining input must survive the startup handoff.
    let padding = PROBE_LIMIT - 24 - reply_size;
    packets.push_back(vec![b'a'; padding]);
    packets.push_back(vec![b'b'; 100]);
    let mut wire = Wire {
        packets,
        offset: 0,
        read_max: read_max.max(1),
        fail_at,
        sends: 0,
        outer_seen: false,
        gap: false,
        broken,
        sent: Vec::new(),
    };
    assert_eq!(wire.receive(&mut [], Instant::now()).ok(), Some(0));
    let result = detect(&mut wire, &env);
    if fail_at.is_some() {
        assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::BrokenPipe));
        let mut cancel = vec![0x18];
        if mux != Multiplexer::None {
            cancel.extend_from_slice(&mux.wrap(b"\x18"));
        }
        assert!(
            wire.sent.ends_with(&cancel),
            "failed query was not cancelled"
        );
    } else {
        let (caps, mut decoder) = result.unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(caps, expected, "outer completion raced an inner reply");
        let mut events = decoder.take_events();
        let mut buffer = [0; 24];
        loop {
            let count = match wire.receive(&mut buffer, Instant::now() + PROBE_TIMEOUT) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                result => result.unwrap_or_else(|error| panic!("{error}")),
            };
            if count == 0 {
                break;
            }
            assert!(decoder.feed(&buffer[..count]).is_ok());
            events.extend(decoder.take_events());
        }
        let expected_events: Vec<_> = std::iter::repeat_n('a', padding)
            .chain(std::iter::repeat_n('b', 100))
            .map(|c| InputEvent::Key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE)))
            .collect();
        assert!(
            events == expected_events,
            "native read lost bytes: received={} expected={}",
            events.len(),
            expected_events.len()
        );
    }
    wire.sent
}

pub fn batch_len(mux: Multiplexer) -> usize {
    Probe::batch(mux).len()
}
