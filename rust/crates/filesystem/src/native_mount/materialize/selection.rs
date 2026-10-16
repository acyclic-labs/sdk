use super::*;

pub(super) fn generation_namespace_from_selection(
    relative: &str,
    config: crate::VolumeConfig,
    receipt: &mut GenerationReceipt,
) -> Result<(NamespacePath, crate::NativeAllocation), MaterializeError> {
    // Staging selects the whole generation with an empty string. Host-relative
    // restore paths keep their existing nonempty-path validation.
    if relative.is_empty() {
        let root = NamespacePath::new(Vec::new(), config.limits)
            .map_err(|_| MaterializeError::InvalidPath)?;
        return Ok((root, receipt.custody.split_off(0)?));
    }
    super::api::generation_namespace_from_host(Path::new(relative), config, receipt)
}

/// Exercises the production staging selector and its exact path round trip.
#[cfg(any(test, fuzzing))]
#[doc(hidden)]
pub fn verify_native_staging_selection(input: &str) {
    let config = crate::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral);
    let cancellation = CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(
        crate::performance::work_budget(1_000_000, 1_000_000),
        &cancellation,
    )
    .map_err(|failure| failure.0)
    .unwrap();
    let mut receipt = GenerationReceipt::from_work(&work).unwrap();
    let selected = generation_namespace_from_selection(input, config, &mut receipt);
    let legacy = super::api::generation_namespace_from_host(Path::new(input), config, &mut receipt);
    if input.is_empty() {
        assert!(legacy.is_err());
        let (root, custody) = selected.unwrap();
        assert!(root.is_root());
        assert_eq!(custody.bytes(), 0);
    } else {
        assert_eq!(selected.is_ok(), legacy.is_ok());
        if let Ok((selected, _custody)) = selected {
            assert_eq!(selected, legacy.unwrap().0);
            assert!(!selected.is_root());
            let mut spelling = PathBuf::new();
            for component in selected.components() {
                spelling.push(std::str::from_utf8(component.as_bytes()).unwrap());
            }
            let (round_trip, _custody) = generation_namespace_from_selection(
                spelling.to_str().unwrap(),
                config,
                &mut receipt,
            )
            .unwrap();
            assert_eq!(selected, round_trip);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        // Property: only the explicit empty selection adds a root; every other
        // selection preserves host validation and exact namespace round trips.
        #[test]
        fn staging_selection_preserves_host_paths_and_round_trips(
            components in prop::collection::vec(
                prop_oneof![".{0,64}", Just("..".into()), Just(".".into()), Just("".into())],
                0..16,
            ),
        ) {
            verify_native_staging_selection(&components.join("/"));
        }
    }
}
