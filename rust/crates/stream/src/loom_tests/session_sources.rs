//! The session source scheduler models, run beside the scheduler they drive.
#[test]
fn a_saturated_channel_cannot_block_an_independent_follow() {
    saturated(false);
}
#[test]
#[should_panic(expected = "the reserved slot must enable the healthy channel")]
fn saturation_negative_control_applies_one_global_gate() {
    saturated(true);
}
fn saturated(broken: bool) {
    crate::session::verify_source_saturation(broken);
}

#[test]
fn disabling_an_idle_source_preserves_its_next_record() {
    disabling(false);
}
#[test]
#[should_panic(expected = "a disabled source must not consume a record")]
fn disabling_negative_control_keeps_polling_the_provider() {
    disabling(true);
}
fn disabling(broken: bool) {
    crate::session::verify_source_disabled(broken);
}
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(128))]
    // Reserving a maximum-record slot keeps one independently credited channel
    // admissible while every uncredited/latest channel is backpressured.
    #[test]
    fn reserved_capacity_is_available_only_to_credited_channels(
        frames in 2_usize..1024, largest in 1_usize..65536,
    ) {
        crate::session::verify_reserved_capacity(frames, largest);
    }
}
