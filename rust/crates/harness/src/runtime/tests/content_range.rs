use super::*;
use acyclic_fs::access::{Action, Capability, Generations, Grant, ResourcePattern};
use proptest::prelude::*;

fn grants(path: &str) -> Result<Capabilities> {
    Ok(Capabilities::new([Capability {
        layers: vec![vec![Grant {
            actions: [Action::Read].into_iter().collect(),
            resource: ResourcePattern {
                lineage_root: None,
                volume: None,
                generations: Generations::All,
                path: path.into(),
            },
        }]],
        expires_at: None,
    }
    .to_scope_label()
    .map_err(|error| Error::Invalid(error.to_string()))?]))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn ranged_callers_cannot_replace_the_tasks_scope(prefix in "[a-z]{1,8}") {
        tokio::runtime::Builder::new_current_thread().build()?.block_on(async {
            let narrow = grants(&format!("{prefix}/**"))?;
            let wide = grants("**")?;
            let scope = RuntimeScope::new(narrow, Limits::default())?;
            let context = test_context(Bindings { scope: scope.clone(), ..Bindings::local() }.build(None)?)?;
            let file = file_fixture("secrets/key", "text/plain")?;
            let result = context.read_bound_range(&file, acyclic_fs::ByteRange { offset: 0, length: 1 }, &scope, &wide, None).await;
            assert_eq!(result.unwrap_err().code(), crate::wire::ErrorCode::Unauthorized);
            // This was the complete former guard: the backing reader received
            // the replacement grants even though they widened the task scope.
            assert!(read_granted_with_work(&wide, &file, &mut None)?);
            Ok::<_, Error>(())
        }).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}
