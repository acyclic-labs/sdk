use super::*;
use acyclic_stream::MemoryStream;
use acyclic_test_support::{AllocationInfo, differential, measure};
use futures::FutureExt as _;
use proptest::prelude::*;
use std::{future::Future, time::Instant};

mod baseline;

fn ready<T>(future: impl Future<Output = T>) -> T {
    tokio::task::unconstrained(future)
        .now_or_never()
        .expect("memory operation must be ready")
}

fn fixture(k: usize) -> TaskVolume<MemoryStream> {
    let provider = Arc::new(MemoryStream::default());
    let path = StreamPath::new("observer").unwrap();
    ready(provider.append(AppendRequest {
        path: path.clone(),
        records: vec![serde_json::to_vec(&Header {
            volume: Uuid::nil(),
            epoch: Uuid::nil(),
            schema: SCHEMA,
        }).unwrap().into()],
        if_tail: Some(0),
        idempotency_key: None,
    }))
    .unwrap();
    let volume = ready(TaskVolume::open_existing(provider, path)).unwrap();
    let publish = |file: &FileValue| {
        let (record, _) = segment::encode(file, None).unwrap();
        ready(volume.provider.append(AppendRequest {
            path: volume.path.clone(),
            records: vec![record.into()],
            if_tail: None,
            idempotency_key: None,
        }))
        .unwrap();
    };
    for n in 0..k {
        publish(&FileValue::new(
            FileKey::parse(&format!(".tasks/t/notifications/n{n:04}.json")).unwrap(),
            b"historical".to_vec(),
        ));
    }
    let status = FileValue::new(
        FileKey::parse(".tasks/t/status.json").unwrap(),
        b"before".to_vec(),
    );
    publish(&status);
    ready(volume.refresh()).unwrap();
    // Storage advances without updating this observer's decoded prefix.
    publish(&FileValue {
        bytes: b"after!".to_vec().into(),
        ..status
    });
    volume
}

fn observe(volume: TaskVolume<MemoryStream>, old: bool) -> (u64, AllocationInfo) {
    let started = Instant::now();
    let mut work = 0;
    let mut output = None;
    let allocations = measure(|| {
        let (result, examined) = crate::test_support::measure_task_volume_work(|| {
            let query = Query::File(FileKey::parse(".tasks/t/status.json").unwrap());
            let (snapshot, limited) = if old {
                let observer = ready(baseline::OldVolume::open_existing(
                    volume.provider.clone(),
                    volume.path.clone(),
                ))
                .unwrap();
                (
                    ready(observer.snapshot(&query, 1)).unwrap(),
                    ready(observer.snapshot(&query, 0))
                        .err()
                        .unwrap()
                        .to_string(),
                )
            } else {
                let observer = ready(volume.reopen()).unwrap();
                (
                    ready(observer.snapshot(&query, 1)).unwrap(),
                    ready(observer.snapshot(&query, 0))
                        .err()
                        .unwrap()
                        .to_string(),
                )
            };
            (serde_json::to_vec(&snapshot).unwrap(), limited)
        });
        work = examined;
        output = Some(result);
    });
    let (snapshot, limited) = output.unwrap();
    differential::record(("snapshot", snapshot));
    differential::record(("limit", limited.into_bytes()));
    println!(
        "observer old={old} work={work} allocations={} bytes={} elapsed={:?}",
        allocations.count_total,
        allocations.bytes_total,
        started.elapsed()
    );
    (work, allocations)
}

fn paired(k: usize) -> [(u64, AllocationInfo); 2] {
    let [before, after] = differential::pair::<_, _, (&str, Vec<u8>)>(|| fixture(k), observe);
    assert_eq!(
        before.1, after.1,
        "complete snapshots and exact errors differ"
    );
    println!("observer K={k}");
    [before.0, after.0]
}

fn assert_same_cost(actual: (u64, AllocationInfo), expected: (u64, AllocationInfo)) {
    assert_eq!(actual.0, expected.0, "work differs");
    assert_eq!(actual.1.count_total, expected.1.count_total);
    assert_eq!(actual.1.bytes_total, expected.1.bytes_total);
}

#[test]
#[should_panic(expected = "work differs")]
fn negative_control_replaying_the_prefix_fails_the_flat_cost_assertion() {
    let [old8, _] = paired(8);
    let [old24, _] = paired(24);
    assert_same_cost(old24, old8);
}

#[test]
fn reopening_observers_has_exact_flat_suffix_cost() {
    let [old8, new8] = paired(8);
    for k in [16, 24] {
        let [old, new] = paired(k);
        assert_same_cost(new, new8);
        // Adding the old replay back must fail those same equality assertions.
        assert_ne!(old.0, old8.0);
        assert_ne!(old.1.count_total, old8.1.count_total);
        assert_ne!(old.1.bytes_total, old8.1.bytes_total);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn suffix_observation_equals_independent_full_reconstruction(k in 0usize..32) {
        paired(k);
    }
}
