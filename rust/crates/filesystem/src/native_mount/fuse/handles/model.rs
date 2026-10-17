use super::*;
use loom::{sync::Arc, thread};

fn concurrent_open<const RANK: u16>() {
    loom::model(|| {
        let file = Arc::new(RankedMutex::<_, RANK>::new(None));
        let source = Arc::new(RankedMutex::<_, VIEW>::new(0));
        let readers = [0, 1].map(|_| {
            let file = Arc::clone(&file);
            let source = Arc::clone(&source);
            thread::spawn(move || {
                initialize(&file, || {
                    let mut opens = source.lock().unwrap();
                    *opens += 1;
                    Ok(7)
                })
                .unwrap()
            })
        });
        for reader in readers {
            assert_eq!(reader.join().unwrap(), 7);
        }
        assert_eq!(*source.lock().unwrap(), 1);
        assert_eq!(*file.lock().unwrap(), Some(7));
    });
}

#[test]
fn deferred_open_initializes_once_before_acquiring_source_view_locks() {
    concurrent_open::<DEFERRED_OPEN>();
}

#[test]
#[should_panic(expected = "lock order")]
fn deferred_open_detects_the_former_file_to_view_lock_inversion() {
    concurrent_open::<{ crate::lock_order::FILE }>();
}
