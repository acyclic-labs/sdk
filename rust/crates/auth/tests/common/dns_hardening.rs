use proptest::prelude::*;

pub fn numeric_name() -> impl Strategy<Value = String> {
    (
        prop::collection::vec(any::<u32>(), 1..5),
        0..3u8,
        any::<bool>(),
    )
        .prop_map(|(values, notation, trailing)| {
            let name = values
                .iter()
                .map(|value| match notation {
                    0 => value.to_string(),
                    1 => format!("0{value:o}"),
                    _ => format!("0x{value:x}"),
                })
                .collect::<Vec<_>>()
                .join(".");
            if trailing { format!("{name}.") } else { name }
        })
}
pub fn dns_property(
    name: &str,
    admit: impl Fn(&str) -> bool,
) -> proptest::test_runner::TestCaseResult {
    prop_assert!(!admit(name));
    Ok(())
}
