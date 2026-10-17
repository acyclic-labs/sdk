#[path = "../../src/arc_layout.rs"]
mod arc_layout;

#[cfg(kani)]
#[kani::proof]
#[kani::unwind(1)]
fn complete_arc_layout() {
    use std::alloc::Layout;

    let size: usize = kani::any();
    let shift: u8 = kani::any();
    kani::assume(u32::from(shift) < usize::BITS);
    let Ok(payload) = Layout::from_size_align(size, 1_usize << shift) else {
        return;
    };
    let align = payload.align().max(std::mem::align_of::<usize>());
    let mask = align - 1;
    let prefix = (2 * std::mem::size_of::<usize>() + mask) & !mask;
    let expected = (prefix + size + mask) & !mask;
    let result = arc_layout::allocation_bytes(payload);
    assert_eq!(result.is_ok(), expected <= isize::MAX as usize);
    let Ok(bytes) = result else { return };
    assert_eq!(bytes, expected);
}

#[cfg(kani)]
#[kani::proof]
#[kani::unwind(1)]
fn missing_weak_counter_is_detected() {
    use std::alloc::Layout;

    let payload = Layout::new::<usize>();
    let complete = arc_layout::allocation_bytes(payload).unwrap();
    let (missing_weak, _) = Layout::new::<usize>().extend(payload).unwrap();
    assert_eq!(complete, 3 * std::mem::size_of::<usize>());
    assert_ne!(missing_weak.pad_to_align().size(), complete);
    assert_ne!(payload.size(), complete);
}
