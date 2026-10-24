use super::NativeMaterializationWork;
use acyclic_native_runtime::ReadAllocation;
use acyclic_stream::OwnedBytes;

#[test]
fn reserved_byte_header_keeps_full_arc_custody_after_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    for cancel in [false, true] {
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let meter = work.runtime_meter();
        let baseline = work.live_allocation_bytes();
        let data = vec![42; 8];
        let capacity = data.capacity();
        let owner = ReadAllocation::new(&meter, capacity)?;
        let reservation = OwnedBytes::reserve_vec_guard::<ReadAllocation>(&meter)?;
        let allocations = work.receipt().allocation_operations;
        if cancel {
            cancellation.cancel();
        }
        let bytes = OwnedBytes::from_vec_with_reserved_guard((data, owner), reservation)
            .map_err(|failure| format!("{:?}", failure.into_error()))?;
        let header = work.live_allocation_bytes() - baseline - capacity as u64;
        let minimum = (std::mem::size_of::<Vec<u8>>()
            + 2 * std::mem::size_of::<ReadAllocation>()
            + 2 * std::mem::size_of::<std::sync::atomic::AtomicUsize>())
            as u64;
        assert!(
            header >= minimum,
            "reserved byte header omitted Arc counters"
        );
        assert_eq!(work.receipt().allocation_operations, allocations + 1);
        assert_eq!(bytes.as_slice(), &[42; 8]);
        drop(bytes);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}
