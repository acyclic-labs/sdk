#[test]
fn persisted_window_records_preserve_fields_or_typed_corruption() {
    for input in [
        include_bytes!("../corpus/window_state/active").as_slice(),
        include_bytes!("../corpus/window_state/active-edit").as_slice(),
        include_bytes!("../corpus/window_state/blank-owner").as_slice(),
        include_bytes!("../corpus/window_state/byte-edit").as_slice(),
        include_bytes!("../corpus/window_state/empty-leases").as_slice(),
        include_bytes!("../corpus/window_state/empty-parent").as_slice(),
        include_bytes!("../corpus/window_state/foreign").as_slice(),
        include_bytes!("../corpus/window_state/idle").as_slice(),
        include_bytes!("../corpus/window_state/lease-key").as_slice(),
        include_bytes!("../corpus/window_state/raw-json").as_slice(),
        include_bytes!("../corpus/window_state/reconcile-edit").as_slice(),
        include_bytes!("../corpus/window_state/reconciling").as_slice(),
        include_bytes!("../corpus/window_state/revision").as_slice(),
        include_bytes!("../corpus/window_state/truncate").as_slice(),
        include_bytes!("../corpus/window_state/version").as_slice(),
        include_bytes!("../corpus/window_state/zero-expiry").as_slice(),
        include_bytes!("../corpus/window_state/zero-identity").as_slice(),
        include_bytes!("../corpus/window_state/zero-ticket").as_slice(),
    ] {
        acyclic_fs::operation_window::check_persisted_window(input);
    }
    // Exercise each semantic field independently, including absent parents,
    // through the same production-writer/reopened-reader oracle as the fuzzer.
    for parents in 0..4 {
        for case in 0..16 {
            acyclic_fs::operation_window::check_persisted_window(&[case | (parents << 6)]);
        }
    }
    for phase in 0..3 {
        for byte in 0..=u8::MAX {
            acyclic_fs::operation_window::check_persisted_window(&[
                13 + phase * 16,
                byte,
                0,
                !byte,
            ]);
            acyclic_fs::operation_window::check_persisted_window(&[14 + phase * 16, byte, 0]);
        }
    }
}
