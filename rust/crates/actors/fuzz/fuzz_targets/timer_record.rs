#![no_main]

use acyclic_actors::timers::TimerRecord;
pub use acyclic_fuzz_support as fuzz_support;
#[path = "../../tests/support/timer_oracle.rs"]
mod oracle;

fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    if let Some(record) = fuzz_support::FuzzInput::parse(fuzz_input, TimerRecord::decode) {
        oracle::check(&record);
        let _ = fuzz_support::round_trip(
            &(record.clone()),
            TimerRecord::decode(&record.encode().expect("encode")),
        );
        assert!(record.stream_path().is_ok());
        assert!(record.fire_key().expect("fire").as_bytes().len() > 32);
    }
});
