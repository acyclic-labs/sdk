use super::*;
use proptest::prelude::*;

fn work(bytes: u64) -> NativeMaterializationWork {
    NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(100_000, bytes),
        &acyclic_fs::CancellationToken::new(),
    )
    .expect("fixture work")
}

fn check_retention(input: Vec<u8>, broken: bool) {
    let work = work(4_000_000);
    let baseline = work.live_allocation_bytes();
    let bytes = Bytes::from(input);
    let expected = bytes.clone();
    let source = futures::stream::iter([Ok(bytes)]);
    let mut body = body(source, (!broken).then_some(&work)).unwrap();
    let received = futures::executor::block_on(body.next()).unwrap().unwrap();
    assert_eq!(received, expected);
    if !broken {
        assert_eq!(work.receipt().source_bytes_read, expected.len() as u64);
    }
    let clone = received.clone();
    drop((body, received));
    assert!(
        work.live_allocation_bytes() >= baseline + clone.len() as u64,
        "response payload lost custody while a reader retains it"
    );
    drop(clone);
    assert_eq!(work.live_allocation_bytes(), baseline);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn received_bytes_preserve_data_and_custody_until_the_last_reader_drops(
        input in prop::collection::vec(any::<u8>(), 1..4096),
    ) { check_retention(input, false); }
}

#[test]
#[should_panic(expected = "response payload lost custody")]
fn negative_control_detects_the_former_unfunded_response_buffer() {
    check_retention(vec![b'x'; 4096], true);
}

#[test]
fn an_oversized_received_buffer_is_rejected_before_a_reader_can_consume_it() {
    let work = work(8192);
    let baseline = work.live_allocation_bytes();
    let source = futures::stream::iter([Ok(Bytes::from(vec![0; 16384]))]);
    let mut body = body(source, Some(&work)).unwrap();
    assert!(futures::executor::block_on(body.next()).unwrap().is_err());
    drop(body);
    assert_eq!(work.live_allocation_bytes(), baseline);
}
