//! Strict successful result of the first-party root allocation effect.
//!
//! These are retained references, not creation authority. Activating a root
//! still requires its original successful effect and actual provider evidence.

use super::{PiScratchCreation, PiTaskBinding, RootCreation};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiRootAllocation {
    pub(crate) roots: BTreeMap<String, RootCreation>,
    pub(crate) scratch: PiScratchCreation,
}

impl PiRootAllocation {
    /// Checks result references against the immutable original selection.
    /// No path is opened and no retained route is changed by this check.
    pub(crate) fn validate(&self, binding: &PiTaskBinding) -> Result<(), String> {
        binding.validate(
            None,
            binding.project_volumes.keys().map(String::as_str).collect(),
            true,
        )?;
        if !self.roots.keys().eq(binding.project_volumes.keys()) {
            return Err("Pi allocation differs from its original root selection".to_owned());
        }
        for creation in self.roots.values() {
            creation.validate()?;
        }
        self.scratch.validate(&binding.scratch.volume)
    }

    /// Pins selected root keys and the exact original provider in the staged
    /// schema. Resource fields describe only the generation returned by this
    /// effect, rather than a second general-purpose resource wire contract.
    pub(crate) fn schema(binding: &PiTaskBinding) -> Result<Value, String> {
        binding.validate(
            None,
            binding.project_volumes.keys().map(String::as_str).collect(),
            true,
        )?;
        let identity = json!({
            "type":"array", "minItems":16, "maxItems":16,
            "items":{"type":"integer","minimum":0,"maximum":255},
            "not":{"const":vec![0u8; 16]}
        });
        let root = json!({
            "type":"object", "additionalProperties":false,
            "required":["repository_workspace_id","source_identity","source_epoch","native_root_identity"],
            "properties":{
                "repository_workspace_id":identity, "source_identity":identity,
                "native_root_identity":identity,
                "source_epoch":{"type":"integer","minimum":0,"maximum":u64::MAX}
            }
        });
        let roots: BTreeMap<_, _> = binding
            .project_volumes
            .keys()
            .map(|key| (key, root.clone()))
            .collect();
        let keys: Vec<_> = roots.keys().copied().collect();
        Ok(json!({
            "type":"object", "additionalProperties":false, "required":["roots","scratch"],
            "properties":{
                "roots":{"type":"object","additionalProperties":false,"required":keys,"properties":roots},
                "scratch":{
                    "type":"object","additionalProperties":false,"required":["workspace_id","generation"],
                    "properties":{
                        "workspace_id":identity,
                        "generation":{
                            "type":"object","additionalProperties":false,"required":["kind","provider","key","version"],
                            "properties":{
                                "kind":{"const":"generation"},
                                "provider":{"const":binding.scratch.volume.provider()},
                                "key":{"type":"array","minItems":32,"maxItems":32,"items":{"type":"integer","minimum":0,"maximum":255}},
                                "version":{"type":"null"}
                            }
                        }
                    }
                }
            }
        }))
    }
}
