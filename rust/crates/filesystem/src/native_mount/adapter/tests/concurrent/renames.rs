use super::*;

#[test]
fn concurrent_directory_moves_preserve_reachability() -> Result<(), Box<dyn std::error::Error>> {
    for (sensitivity, second_from, second_to, order, disjoint) in [
        (CaseSensitivity::Sensitive, "b", "a/b", [0, 1], false),
        (CaseSensitivity::Sensitive, "b", "a/b", [1, 0], false),
        (CaseSensitivity::Sensitive, "c", "d/c", [0, 1], true),
        (CaseSensitivity::ProfileFolded, "b", "A/b", [0, 1], false),
        (CaseSensitivity::ProfileFolded, "B", "a/b", [0, 1], false),
        (CaseSensitivity::ProfileFolded, "c", "D/c", [0, 1], true),
    ] {
        let (source, _) = configured_sources(
            VolumeConfig {
                case_sensitivity: sensitivity,
                ..VolumeConfig::portable(Lifecycle::Ephemeral)
            },
            ConsistencyMode::Pinned,
            MountPublication::Manual,
        )?;
        let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
        for name in ["a", "b", "c", "d"] {
            source.create_directory(&native_test_path(name), metadata(), &work)?;
            let path = native_test_path(&format!("{name}/kept"));
            source.create_file(&path, metadata(), None, &work)?;
            source
                .open_file(&path)?
                .write_range(0, Bytes::from_static(b"kept"))?;
        }
        let from = [
            source.path(&native_test_path("a"))?,
            source.path(&native_test_path(second_from))?,
        ];
        let to = [
            source.path(&native_test_path("b/a"))?,
            source.path(&native_test_path(second_to))?,
        ];
        let identity = |name| {
            source
                .lookup(&native_test_path(name))?
                .map(|entry| entry.node.file_id)
                .ok_or(MountSourceError::NotFound)
        };
        let ids = [identity("a")?, identity(&second_from.to_lowercase())?];
        source.runtime.wait(|| async {
            let mut candidates = [
                Some(candidate_now(&source.checkout, &work).await?),
                Some(candidate_now(&source.checkout, &work).await?),
            ];
            for (index, candidate) in candidates.iter_mut().enumerate() {
                apply_authored(
                    &mut candidate.as_mut().unwrap().checkout,
                    &AuthoredMutation::Rename {
                        source: from[index].clone(),
                        destination: to[index].clone(),
                        replace: false,
                    },
                    &work,
                )
                .await?;
            }
            for (ordinal, index) in order.into_iter().enumerate() {
                let result = source
                    .checkout
                    .install_change(
                        candidates[index].take().unwrap(),
                        ViewChange::Moved {
                            from: &from[index],
                            to: &to[index],
                            moved: ids[index],
                            replaced: None,
                        },
                        &work,
                        source.cancellation(),
                        None,
                    )
                    .await;
                if ordinal == 0 || disjoint {
                    result?;
                } else {
                    assert!(
                        matches!(result, Err(MountSourceError::Stale)),
                        "conflicting moves must not form an unreachable cycle: {result:?}"
                    );
                }
            }
            Ok(())
        })?;
        let reachable = if disjoint {
            ["b/a/kept", "d/c/kept"]
        } else if order[0] == 0 {
            ["b/a/kept", "b/kept"]
        } else {
            ["a/kept", "a/b/kept"]
        };
        for path in reachable {
            assert_eq!(
                source
                    .open_file(&native_test_path(path))?
                    .read_up_to(0, 16)?
                    .as_ref(),
                b"kept"
            );
        }
    }
    Ok(())
}
