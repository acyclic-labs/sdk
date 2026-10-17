use super::super::Arc;
use super::*;

/// One application thread races one placement fence over the production lock.
/// No Tokio synchronization or transport state is substituted for this cut.
pub fn verify_latest_progress(broken: bool) {
    loom::model(move || {
        let progress = Arc::new(Progress::new(1));
        let applying = Arc::clone(&progress);
        let consumer = loom::thread::spawn(move || {
            if broken {
                // Mutant reads generation outside the lock that advances the cut.
                let old = applying.state.lock().unwrap().generation;
                loom::thread::yield_now();
                if old == 1 {
                    applying.state.lock().unwrap().position.next = 8;
                    true
                } else {
                    false
                }
            } else {
                applying.consume_latest(1, 7).unwrap()
            }
        });
        // The cursor captured at fencing must remain the resume cut if generation
        // 1 is rejected. If consumed first, the fence must see the complete cut.
        assert_eq!(progress.fence().unwrap(), 2);
        let resume = progress.next().unwrap();
        let applied = consumer.join().unwrap();
        let actual = progress.next().unwrap();
        assert_eq!(
            actual, resume,
            "a stale delivery cannot advance the fenced resume cut"
        );
        assert_eq!(actual, if applied { 8 } else { 1 });
    });
}
