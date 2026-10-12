use super::*;

#[derive(Deserialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserVolumeOptions {
    #[tsify(type = "\"portable\" | \"posix\" | \"windows\" | \"browser\"")]
    pub(in crate::bindings) profile: Profile,
    #[tsify(type = "\"exclusive-writer\" | \"optimistic\" | \"serialized-authority\"")]
    pub(in crate::bindings) concurrency: Concurrency,
    #[tsify(type = "\"ephemeral\" | \"durable\"")]
    pub(in crate::bindings) lifecycle: VolumeLifecycle,
    #[tsify(type = "\"sensitive\" | \"profile-folded\"")]
    pub(in crate::bindings) case_sensitivity: NameCase,
    #[tsify(type = "\"preserve\" | \"require-nfc\"")]
    pub(in crate::bindings) unicode: Unicode,
    pub(in crate::bindings) symbolic_links: bool,
    pub(in crate::bindings) hard_links: bool,
    pub(in crate::bindings) sparse_files: bool,
    pub(in crate::bindings) limits: BrowserVolumeLimits,
}

pub(in crate::bindings) type VolumeOptions = BrowserVolumeOptions;

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(in crate::bindings) enum Profile {
    Portable,
    Posix,
    Windows,
    Browser,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(in crate::bindings) enum Concurrency {
    ExclusiveWriter,
    Optimistic,
    SerializedAuthority,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(in crate::bindings) enum VolumeLifecycle {
    Ephemeral,
    Durable,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(in crate::bindings) enum NameCase {
    Sensitive,
    ProfileFolded,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(in crate::bindings) enum Unicode {
    Preserve,
    RequireNfc,
}

#[derive(Deserialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
#[allow(
        clippy::struct_field_names,
        reason = "field names match the TypeScript volume-limit keys"
    )]
pub struct BrowserVolumeLimits {
    pub(in crate::bindings) maximum_path_bytes: u32,
    pub(in crate::bindings) maximum_component_bytes: u32,
    pub(in crate::bindings) maximum_path_depth: u16,
    pub(in crate::bindings) maximum_object_bytes: u64,
    pub(in crate::bindings) maximum_mutations_per_batch: u32,
    pub(in crate::bindings) maximum_paths_per_batch: u32,
    pub(in crate::bindings) maximum_checkout_dependencies: u32,
    pub(in crate::bindings) maximum_directory_page_entries: u32,
    pub(in crate::bindings) maximum_page_height: u16,
    pub(in crate::bindings) maximum_read_bytes: u64,
    pub(in crate::bindings) maximum_files_per_generation: u64,
    pub(in crate::bindings) maximum_objects_per_generation: u64,
    pub(in crate::bindings) maximum_generation_bytes: u64,
}

#[derive(Deserialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct BrowserCheckoutOptions {
    #[tsify(type = "\"read-only\" | \"read-write\"")]
    pub(in crate::bindings) access: CheckoutAccess,
    #[tsify(type = "\"pinned\" | \"tracking-safe\" | \"live\" | \"manual\"")]
    pub(in crate::bindings) consistency: Consistency,
    #[tsify(type = "\"none\" | \"private-cow\" | \"direct-live\"")]
    pub(in crate::bindings) mutation_mode: CheckoutMutationMode,
}

pub(in crate::bindings) type CheckoutOptions = BrowserCheckoutOptions;

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(in crate::bindings) enum CheckoutAccess {
    ReadOnly,
    ReadWrite,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(in crate::bindings) enum Consistency {
    Pinned,
    TrackingSafe,
    Live,
    Manual,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(in crate::bindings) enum CheckoutMutationMode {
    None,
    PrivateCow,
    DirectLive,
}

pub(in crate::bindings) fn browser_volume_config(
    options: VolumeOptions,
) -> Result<VolumeConfig, VolumeConfigError> {
    let limits = options.limits;
    VolumeConfig {
        profile: match options.profile {
            Profile::Portable => FilesystemProfile::Portable,
            Profile::Posix => FilesystemProfile::Posix,
            Profile::Windows => FilesystemProfile::Windows,
            Profile::Browser => FilesystemProfile::Browser,
        },
        concurrency: match options.concurrency {
            Concurrency::ExclusiveWriter => ConcurrencyMode::ExclusiveWriter,
            Concurrency::Optimistic => ConcurrencyMode::Optimistic,
            Concurrency::SerializedAuthority => ConcurrencyMode::SerializedAuthority,
        },
        lifecycle: match options.lifecycle {
            VolumeLifecycle::Ephemeral => Lifecycle::Ephemeral,
            VolumeLifecycle::Durable => Lifecycle::Durable,
        },
        case_sensitivity: match options.case_sensitivity {
            NameCase::Sensitive => CaseSensitivity::Sensitive,
            NameCase::ProfileFolded => CaseSensitivity::ProfileFolded,
        },
        unicode: match options.unicode {
            Unicode::Preserve => UnicodePolicy::Preserve,
            Unicode::RequireNfc => UnicodePolicy::RequireNfc,
        },
        symbolic_links: options.symbolic_links,
        hard_links: options.hard_links,
        sparse_files: options.sparse_files,
        limits: VolumeLimits {
            maximum_path_bytes: limits.maximum_path_bytes,
            maximum_component_bytes: limits.maximum_component_bytes,
            maximum_path_depth: limits.maximum_path_depth,
            maximum_object_bytes: limits.maximum_object_bytes,
            maximum_mutations_per_batch: limits.maximum_mutations_per_batch,
            maximum_paths_per_batch: limits.maximum_paths_per_batch,
            maximum_checkout_dependencies: limits.maximum_checkout_dependencies,
            maximum_directory_page_entries: limits.maximum_directory_page_entries,
            maximum_page_height: limits.maximum_page_height,
            maximum_read_bytes: limits.maximum_read_bytes,
            maximum_files_per_generation: limits.maximum_files_per_generation,
            maximum_objects_per_generation: limits.maximum_objects_per_generation,
            maximum_generation_bytes: limits.maximum_generation_bytes,
        },
    }
    .validate()
}

pub(in crate::bindings) fn checkout_mode(options: &CheckoutOptions) -> CheckoutMode {
    CheckoutMode {
        access: match options.access {
            CheckoutAccess::ReadOnly => AccessMode::ReadOnly,
            CheckoutAccess::ReadWrite => AccessMode::ReadWrite,
        },
        consistency: match options.consistency {
            Consistency::Pinned => ConsistencyMode::Pinned,
            Consistency::TrackingSafe => ConsistencyMode::TrackingSafe,
            Consistency::Live => ConsistencyMode::Live,
            Consistency::Manual => ConsistencyMode::Manual,
        },
        mutations: match options.mutation_mode {
            CheckoutMutationMode::None => MutationMode::None,
            CheckoutMutationMode::PrivateCow => MutationMode::PrivateOverlay,
            CheckoutMutationMode::DirectLive => MutationMode::DirectLive,
        },
    }
}
