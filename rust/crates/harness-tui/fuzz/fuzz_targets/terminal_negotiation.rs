#![no_main]

use acyclic_harness_tui::term::Multiplexer;
use libfuzzer_sys::fuzz_target;

#[path = "../../tests/support/negotiation.rs"]
mod negotiation;

fuzz_target!(|data: &[u8]| {
    let mux = if data.first().copied().unwrap_or_default() & 1 == 0 {
        Multiplexer::Tmux
    } else {
        Multiplexer::Screen
    };
    let read_max = 1 + usize::from(u16::from_le_bytes([
        data.get(1).copied().unwrap_or_default(),
        data.get(2).copied().unwrap_or_default(),
    ])) % 2048;
    let failure = data.first().filter(|byte| **byte & 2 != 0).map(|_| {
        usize::from(u16::from_le_bytes([
            data.get(3).copied().unwrap_or_default(),
            data.get(4).copied().unwrap_or_default(),
        ])) % (negotiation::batch_len(mux) + 1)
    });
    negotiation::run(mux, data.get(5..).unwrap_or_default(), read_max, failure, 0);
});
