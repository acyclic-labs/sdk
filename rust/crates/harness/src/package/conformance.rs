//! Reusable event-history driver for in-process and deployed hosts.

use super::{ActorRuntime, Event, Placement, Snapshot};
use crate::Result;

/// One host-independent history action, including explicit recovery cuts.
#[derive(Clone, Debug)]
pub enum Action {
    /// Deliver an admitted input.
    Deliver(Event),
    /// Reconstruct from provider history without adding an input.
    Recover,
    /// Fence effects and settle a durable cancellation event.
    Cancel(Event),
}

/// Runs the identical ordered history on any installed runtime. Callers assert
/// snapshots and errors against an independent oracle, including negative controls.
pub async fn history(
    runtime: &dyn ActorRuntime,
    placement: &Placement,
    actions: &[Action],
) -> Vec<Result<Snapshot>> {
    let mut observations = Vec::with_capacity(actions.len());
    for action in actions {
        observations.push(match action {
            Action::Deliver(event) => runtime.deliver(placement.clone(), event.clone()).await,
            Action::Recover => runtime.recover(placement.clone()).await,
            Action::Cancel(event) => runtime.cancel(placement.clone(), event.clone()).await,
        });
    }
    observations
}

#[cfg(test)]
mod tests {
    use super::super::{ActorPackage, Bindings, Manifest, VolumePlacement, validate_package};
    use crate::{
        Result,
        workflow::{MachineIdentity, MachineTransition, ResumableMachine},
    };
    use serde_json::Value;
    use std::sync::Arc;

    struct Fixture {
        manifest: Manifest,
        identity: MachineIdentity,
    }

    impl ActorPackage for Fixture {
        fn manifest(&self) -> &Manifest {
            &self.manifest
        }
        fn machine(&self) -> Arc<dyn ResumableMachine> {
            Arc::new(Self {
                manifest: self.manifest.clone(),
                identity: self.identity.clone(),
            })
        }
    }

    impl ResumableMachine for Fixture {
        fn identity(&self) -> &MachineIdentity {
            &self.identity
        }
        fn state_schema(&self) -> &Value {
            &Value::Null
        }
        fn initialize(&self, input: &Value) -> Result<Value> {
            Ok(input.clone())
        }
        fn transition(&self, _: &Value, _: &Value) -> Result<MachineTransition> {
            Err(crate::Error::Unsupported(
                "contract fixture has no handler".into(),
            ))
        }
    }

    fn manifest() -> Manifest {
        Manifest {
            agent: MachineIdentity {
                name: "acyclic.agent".into(),
                version: "1".into(),
                digest: [1; 32],
            },
            effects: vec!["model.https".into(), "compute".into()],
            bindings: Bindings {
                streams: "swarm".into(),
                volume: "project".into(),
                placement: VolumePlacement::NodeLocal("local".into()),
                model_https: "https://model.example".into(),
                model_secret: "model-key".into(),
                compute: "local-compute".into(),
            },
        }
    }

    #[test]
    fn package_manifest_requires_explicit_safe_bindings() {
        let mut manifest = manifest();
        manifest.validate().unwrap();
        for origin in [
            "http://model.example",
            "https://key@model.example",
            "https://model.example/key?secret=x",
        ] {
            manifest.bindings.model_https = origin.into();
            assert!(manifest.validate().is_err());
        }
        manifest.bindings.model_https = "https://model.example".into();
        manifest.bindings.compute.clear();
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn package_handler_must_match_manifest() {
        let manifest = manifest();
        let mut fixture = Fixture {
            identity: manifest.agent.clone(),
            manifest,
        };
        assert!(validate_package(&fixture).is_ok());
        fixture.identity.digest = [2; 32];
        assert!(validate_package(&fixture).is_err());
    }

    #[test]
    fn package_effect_declarations_are_unique() {
        let mut manifest = manifest();
        manifest.effects.push("compute".into());
        assert!(manifest.validate().is_err());
    }
}
