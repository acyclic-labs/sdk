#![no_main]
use acyclic_actors::timers::TimerRecord;
use libfuzzer_sys::fuzz_target;
#[path = "../../tests/support/timer_oracle.rs"]
mod oracle;

fuzz_target!(|bytes: &[u8]| {
    if let Ok(record) = TimerRecord::decode(bytes) {
        oracle::check(&record);
        assert_eq!(
            TimerRecord::decode(&record.encode().expect("encode")),
            Ok(record.clone())
        );
        assert!(record.stream_path().is_ok());
        assert!(record.fire_key().expect("fire").as_bytes().len() > 32);
    }
});
