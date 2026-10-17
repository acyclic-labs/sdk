use super::*;

pub(crate) async fn folded_checkout(
    names: impl IntoIterator<Item = String>,
) -> Result<MemoryCheckout, Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
    config.unicode = UnicodePolicy::Preserve;
    let volume = fs
        .create_volume(config, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let mut checkout = volume
        .checkout(
            GenerationSelector::Head,
            CheckoutMode {
                access: AccessMode::ReadWrite,
                consistency: ConsistencyMode::Pinned,
                mutations: MutationMode::PrivateOverlay,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let path = |name: &str| -> Result<NamespacePath, Box<dyn std::error::Error>> {
        Ok(NamespacePath::from_portable(
            &crate::path::PortablePath::parse(&format!("/{name}"), config.limits)?,
            config.limits,
        )?)
    };
    let mut mutations = Vec::new();
    for name in names {
        mutations.push(AuthoredMutation::CreateFile {
            path: path(&name)?,
            bytes: acyclic_stream::OwnedBytes::new(),
            metadata: FileMetadata::default(),
        });
    }
    let mut mutations = mutations.into_iter();
    while !mutations.as_slice().is_empty() {
        let batch = mutations
            .by_ref()
            .take(config.limits.maximum_mutations_per_batch as usize)
            .collect();
        checkout
            .apply_authored_transaction(batch, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
    }
    checkout.volume.config.case_sensitivity = CaseSensitivity::ProfileFolded;
    Ok(checkout)
}

pub(crate) async fn legacy_folded_checkout()
-> Result<(MemoryCheckout, [FileId; 2]), Box<dyn std::error::Error>> {
    let names = ["Straße".to_owned(), "STRASSE".to_owned()]
        .into_iter()
        .chain((0..255).map(|index| format!("Sa{index:03}")));
    let mut checkout = folded_checkout(names).await?;
    let cancellation = CancellationToken::new();
    let mut ids = [FileId::new(); 2];
    for (id, name) in ids.iter_mut().zip(["Straße", "STRASSE"]) {
        let path = NamespacePath::new(
            vec![LogicalName::new(
                NameEncoding::Utf8,
                name.as_bytes().to_vec(),
                checkout.volume_config().limits.maximum_component_bytes,
            )?],
            checkout.volume_config().limits,
        )?;
        *id = checkout
            .lookup_no_follow(&path, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("legacy file missing")?
            .file_id;
    }
    Ok((checkout, ids))
}
