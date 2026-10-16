//! Admission schemas round-trip, reject unknown metadata and retain all bounds.
use acyclic_harness::client::{ClientViewLimits, QueryViewLimits, SwarmClient, SwarmClientLimits};
use acyclic_stream::StreamPath;

pub fn exercise(bytes: &[u8], omit_bound: bool) {
    let byte = |i: usize| usize::from(bytes.get(i).copied().unwrap_or(0));
    let agents = 1 + byte(0) % 8;
    let limits = SwarmClientLimits {
        agents,
        selected: byte(1) % (agents + 1),
        layout_bytes: 1 + byte(2),
        config_files: 1 + byte(3),
        snapshot_files: 1 + byte(4),
    };
    let views = ClientViewLimits {
        agents,
        selected: limits.selected,
        overview_values: agents * 2,
        overview_bytes: agents as u64 * 256,
        query: QueryViewLimits {
            files: limits.snapshot_files,
            segments: 1 + byte(5),
            bytes: 1 + byte(6) as u64,
        },
    };
    SwarmClient::new(StreamPath::new("swarm").unwrap(), limits, views).expect("valid bounds");
    let mut encoded = serde_json::to_value((limits, views)).unwrap();
    if omit_bound {
        encoded[0].as_object_mut().unwrap().remove("snapshot_files");
    }
    let decoded: (SwarmClientLimits, ClientViewLimits) =
        serde_json::from_value(encoded.clone()).expect("all bounds must round-trip");
    assert_eq!(decoded, (limits, views));
    for index in 0..2 {
        let mut unknown = encoded.clone();
        unknown[index]["unknown"] = true.into();
        assert!(serde_json::from_value::<(SwarmClientLimits, ClientViewLimits)>(unknown).is_err());
    }
    if let Ok(parsed) = serde_json::from_slice::<(SwarmClientLimits, ClientViewLimits)>(bytes) {
        assert_eq!(
            serde_json::from_slice::<(SwarmClientLimits, ClientViewLimits)>(
                &serde_json::to_vec(&parsed).unwrap()
            )
            .unwrap(),
            parsed
        );
    }
}
