//! Reusable event-history driver for in-process and deployed hosts.

use super::{ActorRuntime, Event, Placement, Snapshot};
use crate::Result;

/// One host-independent history action, including explicit recovery cuts.
#[derive(Clone, Debug)]
pub enum Action {
    /// Deliver an admitted input.
    Deliver {
        /// Owner attempting this delivery, including stale-owner probes.
        placement: Placement,
        /// Immutable inbox input.
        event: Event,
    },
    /// Reconstruct from provider history without adding an input.
    Recover(Placement),
    /// Fence effects and settle a durable cancellation event.
    Cancel {
        /// Owner attempting cancellation.
        placement: Placement,
        /// Immutable cancellation admission.
        event: Event,
    },
}

/// Runs the identical ordered history on any installed runtime. Callers assert
/// snapshots and errors against an independent oracle, including negative controls.
pub async fn history(runtime: &dyn ActorRuntime, actions: &[Action]) -> Vec<Result<Snapshot>> {
    let mut observations = Vec::with_capacity(actions.len());
    for action in actions {
        observations.push(match action {
            Action::Deliver { placement, event } => {
                runtime.deliver(placement.clone(), event.clone()).await
            }
            Action::Recover(placement) => runtime.recover(placement.clone()).await,
            Action::Cancel { placement, event } => {
                runtime.cancel(placement.clone(), event.clone()).await
            }
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

    struct EpochOracle;

    impl super::ActorRuntime for EpochOracle {
        fn install(
            &self,
            _: Arc<dyn ActorPackage>,
        ) -> acyclic_stream::BoxProviderFuture<'_, Result<()>> {
            Box::pin(async { Ok(()) })
        }
        fn place(
            &self,
            _: super::Placement,
            _: Value,
        ) -> acyclic_stream::BoxProviderFuture<'_, Result<super::Snapshot>> {
            Box::pin(async {
                Err(crate::Error::Unsupported(
                    "oracle only observes histories".into(),
                ))
            })
        }
        fn deliver(
            &self,
            placement: super::Placement,
            _: super::Event,
        ) -> acyclic_stream::BoxProviderFuture<'_, Result<super::Snapshot>> {
            self.recover(placement)
        }
        fn recover(
            &self,
            placement: super::Placement,
        ) -> acyclic_stream::BoxProviderFuture<'_, Result<super::Snapshot>> {
            Box::pin(async move {
                if placement.epoch != 2 {
                    return Err(crate::Error::Conflict("stale owner".into()));
                }
                Ok(super::Snapshot {
                    placement,
                    checkpoint: crate::workflow::MachineCheckpoint {
                        machine: manifest().agent,
                        revision: 0,
                        state: Value::Null,
                    },
                    consumed: 0,
                    status: crate::workflow::MachineStatus::Suspended,
                    pending: Vec::new(),
                    cancelled: false,
                })
            })
        }
        fn cancel(
            &self,
            placement: super::Placement,
            _: super::Event,
        ) -> acyclic_stream::BoxProviderFuture<'_, Result<super::Snapshot>> {
            self.recover(placement)
        }
    }

    #[tokio::test]
    async fn history_preserves_each_actions_placement() {
        let current = super::Placement {
            actor: crate::AgentId::from_bytes([1; 16]),
            node: "node-2".into(),
            epoch: 2,
        };
        let stale = super::Placement {
            node: "node-1".into(),
            epoch: 1,
            ..current.clone()
        };
        let event = super::Event {
            operation_id: crate::OperationId::from_bytes([1; 16]),
            idempotency_key: crate::IdempotencyKey::new("history").unwrap(),
            input: Value::Null,
        };
        let actions = [
            super::Action::Deliver {
                placement: current.clone(),
                event: event.clone(),
            },
            super::Action::Recover(stale.clone()),
            super::Action::Cancel {
                placement: current.clone(),
                event: event.clone(),
            },
            super::Action::Deliver {
                placement: stale,
                event,
            },
            super::Action::Recover(current),
        ];
        let observations = super::history(&EpochOracle, &actions).await;
        let successes: Vec<_> = observations.iter().map(Result::is_ok).collect();
        assert_eq!(successes, [true, false, true, false, true]);
    }

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
