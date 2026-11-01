#[cfg(all(test, loom))]
mod tests {
    fn model(broken: bool) {
        loom::model(move || {
            // Terminal snapshot lock. Each callback releases it before another
            // callback or join; no lock is held across either observation.
            let state = loom::sync::Arc::new(crate::sync::Mutex::new((0u8, 3u64)));
            let writer = loom::thread::spawn({
                let state = state.clone();
                move || *state.lock().unwrap() = (1, 17)
            });
            let early = broken.then(|| state.lock().unwrap().1);
            // Invalidation precedes fact selection; a later stamp could label
            // facts from an earlier candidate as current.
            let stamp = state.lock().unwrap().0;
            let size = early.unwrap_or_else(|| state.lock().unwrap().1);
            writer.join().unwrap();
            let current = state.lock().unwrap();
            if stamp == current.0 {
                assert_eq!(size, current.1, "stale facts labeled current");
            }
        });
    }

    #[test]
    fn concurrent_mutations_never_label_stale_sizes_current() {
        model(false);
    }
    #[test]
    #[should_panic(expected = "stale facts labeled current")]
    fn late_stamp_negative_control() {
        model(true);
    }
}
