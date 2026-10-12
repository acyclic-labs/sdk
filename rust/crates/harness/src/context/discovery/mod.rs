//! Declared, bounded filesystem discovery feeding the ordinary context pipeline.

use super::{ContextInput, ContextPlacement, ContextSource, SourceStage};
use crate::{
    Error, Result,
    conversation::{
        ContentResidencyVerifier, FileRef, PrivateDirectoryEntryKind, VolumeRef,
        validate_content_path,
    },
    model::{FileProjectionPolicy, ModelContent, ModelContentPart, ModelMessage, ModelRole},
    resources::GenerationRef,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

mod declaration;
pub use declaration::{
    ContextDiscovery, ContextDiscoveryLimits, ContextDiscoveryPolicy, ContextReloadPolicy,
    ContextRoot, InstructionScope, PinnedContextPath,
};
mod skills;
use skills::discover_skills;
pub use skills::{SkillMetadata, parse_skill_metadata};
mod capture;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use super::reserve_context_vec_native;
use super::validate_source_messages;
pub use capture::DiscoveredContext;
use capture::{DiscoveryWork, child};
use skills::discovery_error;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use skills::{bound_skill_values, discovery_work_error, skill_text_native};
