use loom::sync::Arc;
use loom::sync::atomic::{AtomicU64, Ordering};

// Compile the production atomic operations against Loom, without a second algorithm.
#[path = "retained.rs"]
mod retained;

#[test]
fn concurrent_capture_retirement_and_transfer_conserve_every_byte() {
    loom::model(|| {
        let bytes = Arc::new(AtomicU64::new(0));
        let release = Arc::clone(&bytes);
        let worker = loom::thread::spawn(move || {
            retained::claim(&release, 3);
            retained::release(&release, 3);
        });
        retained::claim(&bytes, 5);
        assert!(retained::transfer(&bytes, 2).is_ok());
        assert!(worker.join().is_ok());
        // Capture teardown drains only after every worker relinquishes custody.
        assert_eq!(Arc::strong_count(&bytes), 1);
        assert_eq!(retained::take(&bytes), 3);
        assert_eq!(retained::take(&bytes), 0);
        assert!(retained::transfer(&bytes, 1).is_err());
    });
}
