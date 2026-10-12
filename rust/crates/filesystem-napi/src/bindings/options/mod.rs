use super::*;

/// Exact hard limits enforced before externally controlled work.
#[napi(object)]
pub struct NativeVolumeLimits {
    /// Maximum encoded bytes in an absolute path.
    pub maximum_path_bytes: NapiU32,
    /// Maximum encoded bytes in one component.
    pub maximum_component_bytes: NapiU32,
    /// Maximum number of path components.
    pub maximum_path_depth: NapiU32,
    /// Maximum canonical immutable-object bytes.
    pub maximum_object_bytes: BigInt,
    /// Maximum operations in one atomic mutation.
    pub maximum_mutations_per_batch: NapiU32,
    /// Maximum paths in one shared lookup batch.
    pub maximum_paths_per_batch: NapiU32,
    /// Maximum exact dependencies retained by a checkout.
    pub maximum_checkout_dependencies: NapiU32,
    /// Maximum entries returned by one listing page.
    pub maximum_directory_page_entries: NapiU32,
    /// Maximum authenticated tree height.
    pub maximum_page_height: NapiU32,
    /// Maximum bytes returned by one range read.
    pub maximum_read_bytes: BigInt,
    /// Maximum files in one generation closure.
    pub maximum_files_per_generation: BigInt,
    /// Maximum objects in one generation closure.
    pub maximum_objects_per_generation: BigInt,
    /// Maximum canonical bytes in one generation closure.
    pub maximum_generation_bytes: BigInt,
}

/// Complete independently configured volume semantics.
#[napi(object)]
pub struct NativeVolumeOptions {
    /// `portable`, `posix`, `windows`, or `browser`.
    pub profile: String,
    /// `exclusive-writer`, `optimistic`, or `serialized-authority`.
    pub concurrency: String,
    /// `ephemeral` or `durable`.
    pub lifecycle: String,
    /// `sensitive` or `profile-folded`.
    pub case_sensitivity: String,
    /// `preserve` or `require-nfc`.
    pub unicode: String,
    /// Whether symbolic links are representable.
    pub symbolic_links: bool,
    /// Whether multiple names may share one file identity.
    pub hard_links: bool,
    /// Whether holes remain distinct from allocated zeroes.
    pub sparse_files: bool,
    /// Mandatory externally controlled work bounds.
    pub limits: NativeVolumeLimits,
}

/// Checkout behavior selected independently for each volume view.
#[napi(object)]
pub struct NativeCheckoutOptions {
    /// `read-only` or `read-write`.
    pub access: String,
    /// One of `pinned`, `tracking-safe`, `live`, or `manual`.
    pub consistency: String,
    /// One of `none`, `private-cow`, or `direct-live`.
    pub mutation_mode: String,
}

pub(in crate::bindings) fn native_volume_config(
    options: NativeVolumeOptions,
) -> Result<VolumeConfig> {
    let profile = FilesystemProfile::from_public_str(&options.profile)
        .ok_or_else(|| Error::new(Status::InvalidArg, "invalid filesystem profile"))?;
    let concurrency = ConcurrencyMode::from_public_str(&options.concurrency)
        .ok_or_else(|| Error::new(Status::InvalidArg, "invalid concurrency mode"))?;
    let lifecycle = Lifecycle::from_public_str(&options.lifecycle)
        .ok_or_else(|| Error::new(Status::InvalidArg, "invalid lifecycle"))?;
    let case_sensitivity = CaseSensitivity::from_public_str(&options.case_sensitivity)
        .ok_or_else(|| Error::new(Status::InvalidArg, "invalid case sensitivity"))?;
    let unicode = UnicodePolicy::from_public_str(&options.unicode)
        .ok_or_else(|| Error::new(Status::InvalidArg, "invalid Unicode policy"))?;
    let limits = options.limits;
    let maximum_path_depth =
        u16_from_u32(limits.maximum_path_depth.into_inner(), "maximumPathDepth")?;
    let maximum_page_height =
        u16_from_u32(limits.maximum_page_height.into_inner(), "maximumPageHeight")?;
    VolumeConfig {
        profile,
        concurrency,
        lifecycle,
        case_sensitivity,
        unicode,
        symbolic_links: options.symbolic_links,
        hard_links: options.hard_links,
        sparse_files: options.sparse_files,
        limits: VolumeLimits {
            maximum_path_bytes: limits.maximum_path_bytes.into_inner(),
            maximum_component_bytes: limits.maximum_component_bytes.into_inner(),
            maximum_path_depth,
            maximum_object_bytes: bigint_u64(&limits.maximum_object_bytes, "maximumObjectBytes")?,
            maximum_mutations_per_batch: limits.maximum_mutations_per_batch.into_inner(),
            maximum_paths_per_batch: limits.maximum_paths_per_batch.into_inner(),
            maximum_checkout_dependencies: limits.maximum_checkout_dependencies.into_inner(),
            maximum_directory_page_entries: limits.maximum_directory_page_entries.into_inner(),
            maximum_page_height,
            maximum_read_bytes: bigint_u64(&limits.maximum_read_bytes, "maximumReadBytes")?,
            maximum_files_per_generation: bigint_u64(
                &limits.maximum_files_per_generation,
                "maximumFilesPerGeneration",
            )?,
            maximum_objects_per_generation: bigint_u64(
                &limits.maximum_objects_per_generation,
                "maximumObjectsPerGeneration",
            )?,
            maximum_generation_bytes: bigint_u64(
                &limits.maximum_generation_bytes,
                "maximumGenerationBytes",
            )?,
        },
    }
    .validate()
    .map_err(napi_error)
}
