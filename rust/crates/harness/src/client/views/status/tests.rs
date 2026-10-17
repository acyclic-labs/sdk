use super::*;
use proptest::{
    prelude::*,
    test_runner::{Config, TestError, TestRunner},
};

fn sequence(
    commands: &[(u8, u8, u8, bool)],
    broken: bool,
) -> std::result::Result<(), TestCaseError> {
    let limits = ClientViewLimits {
        agents: 3,
        selected: 0,
        overview_values: 5,
        overview_bytes: 80,
        query: QueryViewLimits {
            files: 0,
            segments: 0,
            bytes: 0,
        },
    };
    let mut views = ClientViews::new(StreamPath::new("swarm").unwrap(), limits).unwrap();
    let mut expected = BTreeMap::<(u8, u8), u8>::new();
    LEAK.with(|flag| flag.set(broken));
    let result = (|| {
        for (index, &(agent, key, length, abandon)) in commands.iter().enumerate() {
            let path = StreamPath::new(format!("swarm/a{}", agent % 3)).unwrap();
            let identity = (agent % 3, key % 3);
            if key == 3 {
                views
                    .apply(SwarmUpdate::Retired {
                        agent: path.clone(),
                    })
                    .unwrap();
                expected.retain(|&(agent, _), _| agent != identity.0);
                prop_assert!(views.status(&path).is_none());
            } else {
                let bytes: u64 = expected.values().map(|&length| u64::from(length) + 7).sum();
                let old = expected
                    .get(&identity)
                    .map_or(0, |&length| u64::from(length) + 7);
                let count = expected.len() + usize::from(!expected.contains_key(&identity));
                let fits = bytes - old + u64::from(length) + 7 <= limits.overview_bytes
                    && count <= limits.overview_values;
                let before = views.clone();
                ABANDON.with(|flag| flag.set(abandon));
                let result = views.apply(SwarmUpdate::Data {
                    agent: path,
                    overview: true,
                    data: wire::SessionData {
                        after: Some(wire::SessionCursor {
                            next: index as u64 + 1,
                            opaque: Bytes::from_static(b"cursor"),
                        }),
                        accepted: index as u64 + 1,
                        durable: index as u64 + 1,
                        item: Some(wire::session_data::Item::LatestValue(
                            wire::SessionLatestValue {
                                key: vec![identity.1].into(),
                                value: vec![length; length as usize].into(),
                                attention: false,
                            },
                        )),
                    },
                });
                ABANDON.with(|flag| flag.set(false));
                prop_assert_eq!(result.is_ok(), fits && !abandon);
                if result.is_ok() {
                    expected.insert(identity, length);
                } else {
                    prop_assert_eq!(
                        &views,
                        &before,
                        "refusal or abandoned copy mutated the view"
                    );
                }
            }
            prop_assert_eq!(
                views.agents.values,
                expected.len(),
                "quota detached from rows"
            );
            prop_assert_eq!(
                views.agents.bytes,
                expected
                    .values()
                    .map(|&length| u64::from(length) + 7)
                    .sum::<u64>(),
                "quota detached from payloads"
            );
            let actual: BTreeMap<_, _> =
                views
                    .agents
                    .entries
                    .iter()
                    .flat_map(|(path, agent)| {
                        let number = path
                            .to_string()
                            .chars()
                            .last()
                            .unwrap()
                            .to_digit(10)
                            .unwrap() as u8;
                        agent.status.values.values().map(move |row| {
                            ((number, row.value.key[0]), row.value.value.len() as u8)
                        })
                    })
                    .collect();
            prop_assert_eq!(&actual, &expected);
        }
        Ok(())
    })();
    LEAK.with(|flag| flag.set(false));
    result
}

fn commands() -> impl Strategy<Value = Vec<(u8, u8, u8, bool)>> {
    prop::collection::vec((0u8..3, 0u8..4, 0u8..32, any::<bool>()), 0..48)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn replacements_retirements_and_abandoned_copies_keep_exact_credit(commands in commands()) {
        sequence(&commands, false)?;
    }
}

#[test]
fn negative_control_forgetting_the_reservation_refund_fails_and_shrinks() {
    let mut runner = TestRunner::new(Config {
        cases: 32,
        failure_persistence: None,
        ..Config::default()
    });
    let abandoned = prop::collection::vec((0u8..3, 0u8..3, 0u8..32, Just(true)), 1..8);
    let result = runner.run(&abandoned, |commands| sequence(&commands, true));
    assert!(
        matches!(result, Err(TestError::Fail(ref reason, ref commands))
            if commands.len() == 1 && reason.to_string().contains("refusal or abandoned copy mutated the view")),
        "missing refund escaped or did not shrink: {result:?}"
    );
}

#[test]
fn negative_control_retiring_rows_without_refunding_fails_the_same_property() {
    let result = sequence(&[(0, 0, 1, false), (0, 3, 0, false)], true);
    assert!(
        matches!(result, Err(TestCaseError::Fail(ref reason))
        if reason.to_string().contains("quota detached from rows")),
        "{result:?}"
    );
}
