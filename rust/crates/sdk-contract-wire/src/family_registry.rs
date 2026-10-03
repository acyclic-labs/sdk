//! Unified Rust-owned view of every public contract family.
//!
//! The descriptor-backed families and the older `ContractModel` families share
//! one registry at this boundary.  Downstream generators can therefore select
//! one family entry without maintaining a second list of contracts, routes, or
//! policy tables.

use crate::transport::{
    ACTORS_TRANSPORT, FILESYSTEM_TRANSPORT, FamilyTransportPolicy, HARNESS_TRANSPORT,
    INFERENCE_TRANSPORT, MACHINES_TRANSPORT, OBJECTS_TRANSPORT, STREAM_TRANSPORT,
    WORKERS_TRANSPORT,
};
use crate::{
    ACTOR_POLICIES, ACTORS, ContractSpec, INFERENCE, INFERENCE_POLICIES, MACHINES,
    MACHINES_POLICIES, OBJECTS_POLICIES, OBJECTS_V2, OperationPolicy, RouteSpec, STREAM,
    STREAM_POLICIES, WORKER_POLICIES, WORKERS,
};
use crate::{filesystem::FILESYSTEM_OPERATION_POLICIES, harness::HARNESS_OPERATION_POLICIES};
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
    /// Runtime-qualified transport options in preference order.
    pub transport: FamilyTransportPolicy,
}

/// One native RPC boundary paired with its Rust-owned domain policy.
///
/// The protobuf descriptor supplies the wire shape and streaming flags. The
/// operation policy supplies capability, error, and request-domain
/// validation rules. Keeping the pair together prevents a native facade from
/// selecting an RPC while silently losing the Rust model's validation map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeMethodBoundary {
    /// Family that owns the method.
    pub family: &'static str,
    /// Protobuf package name.
    pub package: &'static str,
    /// Protobuf service name.
    pub service: &'static str,
    /// Protobuf method name.
    pub method: &'static str,
    /// Fully qualified request message name.
    pub input: &'static str,
    /// Fully qualified response message name.
    pub output: &'static str,
    /// Whether the method accepts a client stream.
    pub client_streaming: bool,
    /// Whether the method returns a server stream.
    pub server_streaming: bool,
    /// Rust-owned capability, error, and validation policy.
    pub policy: Option<&'static OperationPolicy>,
}

impl NativeMethodBoundary {
    /// Return the canonical protobuf RPC identity used by generated facades.
    pub fn rpc(self) -> String {
        format!("{}.{}/{}", self.package, self.service, self.method)
    }
}

fn native_method_boundaries(view: FamilyView) -> Vec<NativeMethodBoundary> {
    let Some(spec) = view.model.as_contract_spec() else {
        return Vec::new();
    };
    spec.services
        .iter()
        .flat_map(|service| service.methods.iter().map(move |method| (service, method)))
        .map(|(service, method)| {
            let rpc = format!("{}.{}/{}", spec.package, service.name, method.name);
            NativeMethodBoundary {
                family: view.name,
                package: spec.package,
                service: service.name,
                method: method.name,
                input: method.input,
                output: method.output,
                client_streaming: method.client_streaming,
                server_streaming: method.server_streaming,
                policy: view
                    .operation_policies
                    .iter()
                    .find(|policy| policy.rpc == rpc),
            }
        })
        .collect()
}

/// Return the native method to policy mapping for a family.
///
/// A fresh vector makes this boundary safe for generators to sort or filter
/// without mutating the static Rust registry. Families backed by a
/// documentation-only `ContractModel` have no native RPC methods here.
pub fn native_method_boundaries_for_family(
    family: &'static FamilyView,
) -> Vec<NativeMethodBoundary> {
    native_method_boundaries(*family)
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
        transport: ACTORS_TRANSPORT,
    },
    FamilyView {
        name: "workers",
        model: FamilyModel::ContractSpec(&WORKERS),
        operation_policies: WORKER_POLICIES,
        http: HttpProjection::Explicit(WORKERS.routes),
        transport: WORKERS_TRANSPORT,
    },
    FamilyView {
        name: "objects",
        model: FamilyModel::ContractSpec(&OBJECTS_V2),
        operation_policies: OBJECTS_POLICIES,
        http: HttpProjection::Explicit(OBJECTS_V2.routes),
        transport: OBJECTS_TRANSPORT,
    },
    FamilyView {
        name: "stream",
        model: FamilyModel::ContractSpec(&STREAM),
        operation_policies: STREAM_POLICIES,
        http: HttpProjection::Explicit(STREAM.routes),
        transport: STREAM_TRANSPORT,
    },
    FamilyView {
        name: "inference",
        model: FamilyModel::ContractSpec(&INFERENCE),
        operation_policies: INFERENCE_POLICIES,
        http: HttpProjection::Explicit(INFERENCE.routes),
        transport: INFERENCE_TRANSPORT,
    },
    FamilyView {
        name: "machines",
        model: FamilyModel::ContractSpec(&MACHINES),
        operation_policies: MACHINES_POLICIES,
        http: HttpProjection::Unavailable,
        transport: MACHINES_TRANSPORT,
    },
    FamilyView {
        name: "filesystem",
        model: FamilyModel::Filesystem(&crate::filesystem::FILESYSTEM),
        operation_policies: FILESYSTEM_OPERATION_POLICIES,
        http: HttpProjection::Unavailable,
        transport: FILESYSTEM_TRANSPORT,
    },
    FamilyView {
        name: "harness",
        model: FamilyModel::Harness(&crate::harness::HARNESS),
        operation_policies: HARNESS_OPERATION_POLICIES,
        http: HttpProjection::Unavailable,
        transport: HARNESS_TRANSPORT,
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
