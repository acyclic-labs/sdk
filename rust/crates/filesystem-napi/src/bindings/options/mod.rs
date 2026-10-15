use super::*;

/// Exact hard limits enforced before externally controlled work.
#[napi(object)]
pub struct NativeVolumeLimits {
    pub maximum_path_bytes: NapiU32,
    pub maximum_component_bytes: NapiU32,
    pub maximum_path_depth: NapiU32,
    pub maximum_object_bytes: BigInt,
    pub maximum_mutations_per_batch: NapiU32,
    pub maximum_paths_per_batch: NapiU32,
    pub maximum_checkout_dependencies: NapiU32,
    pub maximum_directory_page_entries: NapiU32,
    pub maximum_page_height: NapiU32,
    pub maximum_read_bytes: BigInt,
    pub maximum_files_per_generation: BigInt,
    pub maximum_objects_per_generation: BigInt,
    pub maximum_generation_bytes: BigInt,
}

/// Complete independently configured volume semantics.
#[napi(object)]
pub struct NativeVolumeOptions {
    pub profile: String,
    pub concurrency: String,
    pub lifecycle: String,
    pub case_sensitivity: String,
    pub unicode: String,
    pub symbolic_links: bool,
    pub hard_links: bool,
    pub sparse_files: bool,
    pub limits: NativeVolumeLimits,
}

/// Checkout behavior selected independently for each volume view.
#[napi(object)]
pub struct NativeCheckoutOptions {
    pub access: String,
    pub consistency: String,
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
