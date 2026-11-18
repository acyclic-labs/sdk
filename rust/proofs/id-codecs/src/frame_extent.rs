#[kani::proof]
fn full_frame_extents_refuse_overflow_without_clamping() {
    let format = LogFrame {
        domain: b"extent-proof\0",
        wide: kani::any(),
        minimum_body: kani::any(),
        maximum_body: kani::any(),
    };
    let body: u64 = kani::any();
    let exact = u128::from(body) + format.prefix_bytes() as u128 + LogFrame::CHECKSUM_BYTES as u128;
    let representable = (format.wide || body <= u64::from(u32::MAX))
        && exact <= u128::from(u64::MAX)
        && exact <= usize::MAX as u128;
    assert_eq!(format.frame_bytes(body).is_some(), representable);
    if let Some(bytes) = format.frame_bytes(body) {
        assert_eq!(u128::from(bytes), exact);
    }
    assert_eq!(
        format.maximum_frame_bytes(),
        format.frame_bytes(format.maximum_body)
    );
}
