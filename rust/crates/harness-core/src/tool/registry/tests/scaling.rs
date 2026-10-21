use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};

proptest! {
    // Property: every nonempty authorized selection allocates exactly what
    // one funded reservation plus its admitted payload copies require. Hidden,
    // absent and unauthorized names do not increase the reserved slot count.
    #[test]
    fn funded_selection_reserves_once(count in 1usize..129) {
        let mut registry = ToolRegistry::default();
        let template = checked(prototype())?;
        let names: Vec<_> = (0..count).map(|index| format!("tool.{index:06}")).collect();
        for name in names.iter().map(String::as_str).chain(["hidden", "unauthorized"]) {
            let mut tool = template.clone();
            tool.definition.name = name.into();
            checked(registry.register((tool, None), &mut None, None))?;
        }
        checked(registry.remove_from_model("hidden"))?;
        let grants = crate::Capabilities::new(names.iter()
            .map(|name| format!("tool:call:{name}"))
            .chain(["tool:call:hidden", "tool:call:absent", "model:generate"].map(String::from)));
        let cancellation = CancellationToken::new();
        let actual_work = checked(NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation))?;
        let expected_work = checked(NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation))?;
        let actual_io = checked(crate::filesystem::PreparationIo::from_native_work(&actual_work))?;
        let expected_io = checked(crate::filesystem::PreparationIo::from_native_work(&expected_work))?;

        let mut actual = None;
        let actual_allocations = acyclic_test_support::measure(|| {
            actual = Some(registry.definitions_for_grants(&grants, Some(&actual_io)));
        });
        let mut expected = None;
        let expected_allocations = acyclic_test_support::measure(|| {
            expected = Some((|| {
                let mut definitions = crate::model::vec_with_capacity(count, Some(&expected_io))?;
                for name in &names {
                    let tool = registry.get(name, Some("1"), Some(&expected_io))?
                        .ok_or_else(|| Error::NotFound(name.clone()))?;
                    definitions.push(crate::model::clone_tool_definition(&tool.definition, Some(&expected_io))?);
                }
                Ok::<_, Error>(definitions)
            })());
        });
        let actual = checked(actual.ok_or("actual collection did not execute"))?;
        let actual = checked(actual)?;
        let expected = checked(expected.ok_or("reference collection did not execute"))?;
        let expected = checked(expected)?;
        prop_assert_eq!(actual.len(), count);
        prop_assert_eq!(actual.capacity(), expected.capacity());
        prop_assert_eq!(actual, expected);
        prop_assert_eq!(actual_allocations.count_total, expected_allocations.count_total);
        prop_assert_eq!(actual_allocations.bytes_total, expected_allocations.bytes_total);
    }
}
