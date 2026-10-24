use super::*;

#[test]
fn revalidation_has_no_reply_delivery_timer() {
    use acyclic_native_runtime::deadline::simulation::Clock;
    for restore_host in [false, true] {
        let clock = Clock::new();
        if restore_host {
            clock.restore_host_clock_after(1);
        }
        let (_source, context, work) = clock
            .enter(|| checkout_context(MountPublication::Manual))
            .unwrap();
        context.forget_view();
        let (changed, unchanged) = std::thread::spawn(move || {
            (
                context.revalidate(&work).unwrap(),
                context.revalidate(&work).unwrap(),
            )
        })
        .join()
        .unwrap();
        assert_eq!((changed, unchanged), (true, false));
        assert!(
            clock.advances().is_empty(),
            "uncached Darwin replies need no timer"
        );
    }
}
