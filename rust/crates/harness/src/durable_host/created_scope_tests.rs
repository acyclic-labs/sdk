use super::*;
use crate::durable_host::tests::fixture::Fixture;
use acyclic_fs::Fs;
use acyclic_fs::access::{Action, Capability, Generations, Grant, ResourcePattern, SCOPE_PREFIX};
use acyclic_stream::MemoryStream;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serde_json::json;

#[tokio::test]
async fn inherited_filesystem_scopes_accept_only_narrowing() -> Result<()> {
    let grant = |path: String, actions| Grant {
        actions,
        resource: ResourcePattern {
            lineage_root: None,
            volume: None,
            generations: Generations::All,
            path,
        },
    };
    let parent = Capability {
        layers: vec![vec![grant("**".into(), Action::ALL.into_iter().collect())]],
        expires_at: None,
    };
    let label = parent.to_scope_label().unwrap();
    let fixture = Fixture::new(
        Arc::new(MemoryStream::default()),
        Fs::memory(),
        json!({"type": "integer"}),
        None,
        &[&label],
    )
    .await?;
    // Every appended restriction is accepted; removing its signed ancestor or
    // dropping the filesystem ceiling while retaining legacy grants is rejected.
    TestRunner::new(Config::with_cases(32))
        .run(&(r"[a-z]{1,12}(/\*\*)?", any::<bool>()), |(path, read)| {
            let child = parent.clone().attenuate(
                vec![grant(
                    path,
                    [if read { Action::Read } else { Action::List }]
                        .into_iter()
                        .collect(),
                )],
                Some(100),
            );
            let mut admission = fixture.admission(1, json!(0));
            let inherited: Vec<_> = admission
                .grants
                .iter()
                .filter(|label| !label.starts_with(SCOPE_PREFIX))
                .map(str::to_owned)
                .collect();
            admission.grants = Capabilities::new(
                inherited
                    .iter()
                    .cloned()
                    .chain([child.to_scope_label().unwrap()]),
            );
            let (scope, _) = fixture
                .host
                .scope_from_access(&admission, None, &mut None)
                .unwrap();
            prop_assert!(
                scope
                    .grants()
                    .is_subset_of(fixture.host.root_scope.grants())
            );

            let mut unbound = child;
            unbound.layers.remove(0);
            admission.grants = Capabilities::new(
                inherited
                    .iter()
                    .cloned()
                    .chain([unbound.to_scope_label().unwrap()]),
            );
            prop_assert!(matches!(
                fixture.host.scope_from_access(&admission, None, &mut None),
                Err(Error::Unauthorized(_))
            ));
            admission.grants = Capabilities::new(inherited);
            prop_assert!(matches!(
                fixture.host.scope_from_access(&admission, None, &mut None),
                Err(Error::Unauthorized(_))
            ));
            Ok(())
        })
        .unwrap();
    Ok(())
}
