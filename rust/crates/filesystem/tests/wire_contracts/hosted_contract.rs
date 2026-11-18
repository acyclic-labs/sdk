mod tests {
    use acyclic_fs::hosted_contract::*;
    use acyclic_fs::kernel::{
        DependencyUse, ExtentSeekTarget, FileKind as EngineFileKind, NameEncoding,
    };
    use acyclic_fs::model::FilesystemProfile as EngineProfile;
    use acyclic_fs::workspace::{JoinHistory, WorkspaceExtentKind};
    use serde_json::Value;

    #[test]
    fn contract_names_are_derived_from_typed_wire_values() {
        assert_eq!(
            FILESYSTEM_PROFILE[1].wire.as_str_name(),
            "FILESYSTEM_PROFILE_PORTABLE"
        );
        assert_eq!(FILE_KIND[9].wire.as_str_name(), "FILE_KIND_MOUNT_BOUNDARY");
        assert_eq!(
            JOIN_HISTORY[4].wire.as_str_name(),
            "JOIN_HISTORY_CHERRY_PICK"
        );
        assert_eq!(
            NAME_ENCODING[3].wire.as_str_name(),
            "NAME_ENCODING_WINDOWS_UTF16LE"
        );
    }

    #[test]
    fn engine_converters_share_the_typed_wire_values() {
        assert_eq!(profile(EngineProfile::Portable), FILESYSTEM_PROFILE[1].wire);
        assert_eq!(file_kind(EngineFileKind::MountBoundary), FILE_KIND[9].wire);
        assert_eq!(name_encoding(NameEncoding::Utf8), NAME_ENCODING[1].wire);
        assert_eq!(sparse_target(ExtentSeekTarget::Hole), SPARSE_TARGET[2].wire);
        assert_eq!(
            conflict_use(DependencyUse::ObservationAndMutation),
            CONFLICT_USE[3].wire
        );
        assert_eq!(
            extent_kind(WorkspaceExtentKind::AllocatedZero),
            EXTENT_KIND[2].wire
        );
        assert_eq!(join_history(JoinHistory::CherryPick), JOIN_HISTORY[4].wire);
    }

    #[test]
    fn generated_contract_contains_all_maps_and_preserves_public_values() -> Result<(), &'static str>
    {
        let contract = contract_json();
        let Some(maps) = contract.get("maps").and_then(Value::as_object) else {
            return Err("generated hosted contract has no maps object");
        };
        assert_eq!(maps.len(), 13);
        assert_public_values(
            "filesystem_profile",
            FILESYSTEM_PROFILE,
            &[
                None,
                Some("portable"),
                Some("posix"),
                Some("windows"),
                Some("browser"),
            ],
        );
        assert_public_values(
            "file_kind",
            FILE_KIND,
            &[
                None,
                Some("regular"),
                Some("directory"),
                Some("symbolic-link"),
                Some("fifo"),
                Some("socket"),
                Some("character-device"),
                Some("block-device"),
                Some("reparse-point"),
                Some("mount-boundary"),
            ],
        );
        assert_public_values(
            "mutation_status_commit",
            MUTATION_STATUS_COMMIT,
            &[
                None,
                Some("committed"),
                Some("already-committed"),
                Some("conflict"),
                Some("fenced"),
                Some("idempotency-conflict"),
                None,
            ],
        );
        assert_public_values(
            "mutation_status_delete",
            MUTATION_STATUS_DELETE,
            &[
                None,
                Some("deleted"),
                Some("already-deleted"),
                Some("conflict"),
                None,
                Some("idempotency-conflict"),
                None,
            ],
        );
        assert_public_values(
            "join_history",
            JOIN_HISTORY,
            &[
                None,
                Some("merge"),
                Some("rebase"),
                Some("squash"),
                Some("cherry-pick"),
            ],
        );
        assert_public_values(
            "conflict_use",
            CONFLICT_USE,
            &[
                None,
                Some("observation"),
                Some("mutation"),
                Some("observation-and-mutation"),
            ],
        );
        assert_public_values(
            "sparse_target",
            SPARSE_TARGET,
            &[None, Some("data"), Some("hole")],
        );
        assert_public_values(
            "extent_kind",
            EXTENT_KIND,
            &[None, Some("hole"), Some("allocated-zero"), Some("content")],
        );
        assert_public_values(
            "source_state",
            SOURCE_STATE,
            &[
                None,
                Some("clean"),
                Some("pending-capture"),
                Some("needs-rescan"),
                Some("conflict"),
                Some("sealed"),
            ],
        );
        assert_public_values(
            "source_invalidation_reason",
            SOURCE_INVALIDATION_REASON,
            &[
                None,
                Some("initial-snapshot-required"),
                Some("queue-overflow"),
                Some("native-rescan-required"),
                Some("backend-error"),
                Some("unrepresentable-path"),
                Some("ambiguous-rename"),
                Some("root-changed"),
            ],
        );
        assert_public_values(
            "name_encoding",
            NAME_ENCODING,
            &[
                None,
                Some("utf8"),
                Some("posix-bytes"),
                Some("windows-utf16le"),
            ],
        );
        assert_public_values(
            "rebase_status",
            REBASE_STATUS,
            &[
                None,
                Some("rebased"),
                Some("already-rebased"),
                Some("current"),
                Some("stale"),
                Some("conflicted"),
                Some("fenced"),
                Some("idempotency-conflict"),
            ],
        );
        assert_public_values(
            "join_status",
            JOIN_STATUS,
            &[
                None,
                Some("applied"),
                Some("already-applied"),
                Some("no-changes"),
                Some("stale-target"),
                Some("conflicted"),
                Some("fenced"),
                Some("idempotency-conflict"),
            ],
        );
        Ok(())
    }

    fn assert_public_values<E: HostedWireEnum>(
        name: &str,
        entries: &[HostedEnumEntry<E>],
        expected: &[Option<&'static str>],
    ) {
        assert_eq!(
            entries.iter().map(|entry| entry.public).collect::<Vec<_>>(),
            expected,
            "public semantics changed for hosted mapping {name}",
        );
    }

    #[test]
    fn advertised_profiles_are_the_typed_non_unspecified_values() {
        assert_eq!(
            supported_profiles(),
            FILESYSTEM_PROFILE[1..]
                .iter()
                .map(|entry| entry.wire as i32)
                .collect::<Vec<_>>()
        );
    }
}
