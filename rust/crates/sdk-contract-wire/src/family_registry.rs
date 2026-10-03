//! Unified Rust-owned view of every public contract family.
//!
//! The descriptor-backed families and the older `ContractModel` families share
//! one registry at this boundary.  Downstream generators can therefore select
//! one family entry without maintaining a second list of contracts, routes, or
//! policy tables.

use crate::{filesystem::FILESYSTEM_OPERATION_POLICIES, harness::HARNESS_OPERATION_POLICIES};
use crate::{
    ContractSpec, OperationPolicy, RouteSpec, ACTORS, ACTOR_POLICIES, INFERENCE,
    INFERENCE_POLICIES, MACHINES, MACHINES_POLICIES, OBJECTS_POLICIES, OBJECTS_V2, STREAM,
    STREAM_POLICIES, WORKERS, WORKER_POLICIES,
};
use prost::Message;

/// The model representation used by one public family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FamilyModel {
    /// A complete structured wire model with messages, enums, services, and routes.
    ContractSpec(&'static ContractSpec),
    /// The Filesystem descriptor identity and archived handshake metadata.
    Filesystem(&'static crate::filesystem::ContractModel),
    /// The Harness descriptor identity and archived handshake metadata.
    Harness(&'static crate::harness::ContractModel),
}

impl FamilyModel {
    /// The protobuf file name owned by this model.
    pub const fn file_name(self) -> &'static str {
        match self {
            Self::ContractSpec(model) => model.file_name,
            Self::Filesystem(model) => model.file_name,
            Self::Harness(model) => model.file_name,
        }
    }

    /// The protobuf package owned by this model.
    pub const fn package(self) -> &'static str {
        match self {
            Self::ContractSpec(model) => model.package,
            Self::Filesystem(model) => model.package,
            Self::Harness(model) => model.package,
        }
    }

    /// The protobuf syntax owned by this model.
    pub const fn syntax(self) -> &'static str {
        match self {
            Self::ContractSpec(model) => model.syntax,
            Self::Filesystem(model) => model.syntax,
            Self::Harness(model) => model.syntax,
        }
    }

    /// Return the structured model when this family has one.
    pub const fn as_contract_spec(self) -> Option<&'static ContractSpec> {
        match self {
            Self::ContractSpec(model) => Some(model),
            Self::Filesystem(_) | Self::Harness(_) => None,
        }
    }

    /// Encode the model descriptor selected by this family.
    pub fn descriptor(self) -> Vec<u8> {
        match self {
            Self::ContractSpec(model) => model.descriptor_set().encode_to_vec(),
            Self::Filesystem(_) => crate::filesystem::filesystem_descriptor(),
            Self::Harness(_) => crate::harness::harness_descriptor(),
        }
    }
}

/// Availability of an explicit hosted HTTP projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpProjection {
    /// The route table is an explicit Rust-owned projection of the wire RPCs.
    Explicit(&'static [RouteSpec]),
    /// This family has no canonical HTTP projection in the current contract.
    Unavailable,
}

/// One entry in the single Rust-owned family registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FamilyView {
    /// Stable lowercase family name used by generators and manifests.
    pub name: &'static str,
    /// The model source selected for the family.
    pub model: FamilyModel,
    /// Operation policy metadata owned by this family.
    pub operation_policies: &'static [OperationPolicy],
    /// Explicit HTTP projection status and routes.
    pub http: HttpProjection,
}

impl FamilyView {
    /// Package name selected by the family model.
    pub const fn package(self) -> &'static str {
        self.model.package()
    }

    /// File name selected by the family model.
    pub const fn file_name(self) -> &'static str {
        self.model.file_name()
    }

    /// Whether this family has a canonical HTTP route projection.
    pub const fn has_http_projection(self) -> bool {
        matches!(self.http, HttpProjection::Explicit(_))
    }

    /// Return the route table, empty when HTTP is unavailable.
    pub const fn routes(self) -> &'static [RouteSpec] {
        match self.http {
            HttpProjection::Explicit(routes) => routes,
            HttpProjection::Unavailable => &[],
        }
    }
}

/// Every public contract family owned by this Rust source boundary.
pub const FAMILY_VIEWS: &[FamilyView] = &[
    FamilyView {
        name: "actors",
        model: FamilyModel::ContractSpec(&ACTORS),
        operation_policies: ACTOR_POLICIES,
        http: HttpProjection::Explicit(ACTORS.routes),
    },
    FamilyView {
        name: "workers",
        model: FamilyModel::ContractSpec(&WORKERS),
        operation_policies: WORKER_POLICIES,
        http: HttpProjection::Explicit(WORKERS.routes),
    },
    FamilyView {
        name: "objects",
        model: FamilyModel::ContractSpec(&OBJECTS_V2),
        operation_policies: OBJECTS_POLICIES,
        http: HttpProjection::Explicit(OBJECTS_V2.routes),
    },
    FamilyView {
        name: "stream",
        model: FamilyModel::ContractSpec(&STREAM),
        operation_policies: STREAM_POLICIES,
        http: HttpProjection::Explicit(STREAM.routes),
    },
    FamilyView {
        name: "inference",
        model: FamilyModel::ContractSpec(&INFERENCE),
        operation_policies: INFERENCE_POLICIES,
        http: HttpProjection::Explicit(INFERENCE.routes),
    },
    FamilyView {
        name: "machines",
        model: FamilyModel::ContractSpec(&MACHINES),
        operation_policies: MACHINES_POLICIES,
        http: HttpProjection::Unavailable,
    },
    FamilyView {
        name: "filesystem",
        model: FamilyModel::Filesystem(&crate::filesystem::FILESYSTEM),
        operation_policies: FILESYSTEM_OPERATION_POLICIES,
        http: HttpProjection::Unavailable,
    },
    FamilyView {
        name: "harness",
        model: FamilyModel::Harness(&crate::harness::HARNESS),
        operation_policies: HARNESS_OPERATION_POLICIES,
        http: HttpProjection::Unavailable,
    },
];

/// Find one family by its stable lowercase name.
pub fn family_view(name: &str) -> Option<&'static FamilyView> {
    FAMILY_VIEWS.iter().find(|family| family.name == name)
}

/// Return the HTTP families directly from the unified registry.
///
/// Consumers that need an OpenAPI or hosted-route inventory must derive it
/// from this projection instead of maintaining a second family-name list.
pub fn explicit_http_family_views() -> impl Iterator<Item = &'static FamilyView> {
    FAMILY_VIEWS
        .iter()
        .filter(|family| family.has_http_projection())
}
