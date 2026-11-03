use super::*;

/// Exercise each tracked lifetime with a real namespace failure. The shared
/// conformance entry also runs the configured server and actual file client.
pub(super) fn observation_errors_preserve_requests(outcome: &Value) -> Result<()> {
    let checkout = tempfile::tempdir().map_err(storage)?;
    let directory = checkout.path().join(DIRECTORY);
    let requests = directory.join("requests");
    let status = directory.join("status");
    std::fs::create_dir_all(&requests).map_err(storage)?;
    std::fs::create_dir(&status).map_err(storage)?;
    let held_status = acyclic_fs::hold_root(&status).map_err(native_file_error)?;
    let session = uuid::Uuid::new_v4();
    let mut served = Served::default();
    for phase in 0..3 {
        let operation = OperationId::new();
        let name = OsString::from(format!("{operation}.json"));
        let path = requests.join(&name);
        immutable(
            &path,
            &serde_json::to_vec(&Request {
                session,
                operation,
                action: HostAction::Open {
                    host: "codex".into(),
                    session: "retained-observation".into(),
                },
            })
            .map_err(storage)?,
        )?;
        served.observe(&requests, session, &name)?;
        if phase != 0 {
            assert_eq!(served.start().len(), 1);
        }
        if phase == 2 {
            served.answer(&held_status, operation, Ok(outcome.clone()))?;
        }
        let replay = git_request(directory.clone(), operation);
        assert!((replay.unanswered)(replay.operation));

        // Retain the original directory and bytes while its visible name is
        // unresolvable. No fake metadata error or alternate observer is used.
        let retained = directory.join("retained-requests");
        std::fs::rename(&requests, &retained).map_err(storage)?;
        #[cfg(unix)]
        std::os::unix::fs::symlink(&requests, &requests).map_err(storage)?;
        #[cfg(windows)]
        std::os::windows::fs::symlink_dir(&requests, &requests).map_err(storage)?;
        let cause = std::fs::symlink_metadata(&path);
        let keeps_replay = (replay.unanswered)(replay.operation);
        let observation = served.observe(&requests, session, &name);
        let keeps_state = match served.known.get(&operation) {
            Some(Tracked::Queued) => phase == 0,
            Some(Tracked::Running { withdrawn: false }) => phase == 1,
            Some(Tracked::Answered) => phase == 2,
            _ => false,
        };
        #[cfg(unix)]
        std::fs::remove_file(&requests).map_err(storage)?;
        #[cfg(windows)]
        std::fs::remove_dir(&requests).map_err(storage)?;
        std::fs::rename(&retained, &requests).map_err(storage)?;

        let cause = cause.expect_err("the real namespace must refuse observation");
        assert_ne!(
            cause.kind(),
            std::io::ErrorKind::NotFound,
            "fixture must exercise observation failure, not absence"
        );
        assert!(
            keeps_replay,
            "observation failure acknowledged a replay result"
        );
        assert!(
            matches!(observation, Err(Error::Storage(ref detail))
                if detail.contains("step=observe native request projection:")
                    && detail.contains(&cause.to_string())),
            "observation must retain its original native failure: {observation:?}"
        );
        assert!(keeps_state, "observation failure retired a live request");
        if phase == 2 {
            assert!(reply_path(&status, operation).is_file());
        }
        served.observe(&requests, session, &name)?;
        assert!((replay.unanswered)(replay.operation));
        remove_projection(&path)?;
        assert!(!(replay.unanswered)(replay.operation));
        served.observe(&requests, session, &name)?;
        assert!(served.start().is_empty());
        served.answer(&held_status, operation, Ok(outcome.clone()))?;
        assert!(!served.known.contains_key(&operation));
        assert!(!reply_path(&status, operation).exists());
    }
    Ok(())
}
