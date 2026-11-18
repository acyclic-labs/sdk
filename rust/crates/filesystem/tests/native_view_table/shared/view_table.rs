mod tests {
    use crate::sync::native::Arc;
    use crate::table::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Core {
        drops: Arc<AtomicUsize>,
        id: u32,
    }

    impl Drop for Core {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn core(id: u32, drops: &Arc<AtomicUsize>) -> Core {
        Core {
            drops: Arc::clone(drops),
            id,
        }
    }

    #[test]
    fn serials_are_issued_once_and_retired_serials_stay_gone() {
        let drops = Arc::new(AtomicUsize::new(0));
        let table = Arc::new(ViewTable::new(u64::MAX));
        let first = ViewTable::insert(&table, core(1, &drops)).unwrap();
        let first_serial = first.serial();
        assert_eq!(table.enter(first_serial.get()).unwrap().id, 1);
        drop(first);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(table.enter(first_serial.get()).is_none());
        let second = ViewTable::insert(&table, core(2, &drops)).unwrap();
        assert!(second.serial() > first_serial);
        assert!(table.enter(first_serial.get()).is_none());
        assert_eq!(table.enter(second.serial().get()).unwrap().id, 2);
        assert!(table.enter(0).is_none());
        assert!(table.enter(u64::MAX).is_none());
        assert_eq!(table.serials(), vec![second.serial()]);
        // A reservation that is abandoned is skipped, not reissued.
        let abandoned = ViewTable::reserve(&table).unwrap().get();
        let third = ViewTable::insert(&table, core(3, &drops)).unwrap();
        assert!(third.serial().get() > abandoned);
        assert!(table.enter(abandoned).is_none());
    }

    #[test]
    fn views_are_independent() {
        let drops = Arc::new(AtomicUsize::new(0));
        let table = Arc::new(ViewTable::new(u64::MAX));
        let a = ViewTable::insert(&table, core(1, &drops)).unwrap();
        let b = ViewTable::insert(&table, core(2, &drops)).unwrap();
        let lease_b = table.enter(b.serial().get()).unwrap();
        assert!(matches!(a.begin_retirement(|| {}).finish(), Ok(core) if core.id == 1));
        assert_eq!(lease_b.id, 2);
        assert_eq!(table.len(), 1);
        drop(lease_b);
        drop(b);
        assert_eq!(table.len(), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn exhausted_serials_are_refused_not_reused() {
        let drops = Arc::new(AtomicUsize::new(0));
        let table = Arc::new(ViewTable::new(2));
        let a = ViewTable::insert(&table, core(1, &drops)).unwrap();
        drop(ViewTable::insert(&table, core(2, &drops)).unwrap());
        assert_eq!(
            ViewTable::insert(&table, core(3, &drops)).err(),
            Some(ViewSerialsExhausted)
        );
        assert_eq!(table.serials(), vec![a.serial()]);
    }

    #[test]
    fn reservation_cannot_enter_another_table_with_the_same_counter() {
        let first = Arc::new(ViewTable::<u32>::new(2));
        let second = Arc::new(ViewTable::<u32>::new(2));
        let reservation = ViewTable::reserve(&first).unwrap();
        drop(ViewTable::reserve(&second).unwrap());
        assert!(
            std::panic::catch_unwind(|| { ViewTable::insert_reserved(&second, reservation, 7) })
                .is_err(),
            "a reservation must not cross instance identity"
        );
        assert_eq!(first.len(), 0);
        assert_eq!(second.len(), 0);
    }
}
