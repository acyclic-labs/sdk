#![no_main]
use acyclic_actors::timers::TimerRecord;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    if let Ok(record) = TimerRecord::decode(bytes) {
        assert_eq!(
            TimerRecord::decode(&record.encode().expect("encode")),
            Ok(record.clone())
        );
        assert!(record.stream_path().is_ok());
        assert_ne!(
            record.retention_key().expect("retain"),
            record.fire_key().expect("fire")
        );
    }
});
