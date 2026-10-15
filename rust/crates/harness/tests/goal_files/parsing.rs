use super::*;
use acyclic_harness::{
    AgentId,
    conversation::{FileDescriptor, FileRef, VolumeOwner, VolumeRef},
    goal::{GOAL_METADATA_HEADROOM, GOAL_REASON_BYTES},
    resources::ProviderRef,
};
use proptest::prelude::*;

fn file(path: &str, class: VolumeClass, bytes: &[u8], version: &str) -> Result<FileRef> {
    FileRef::new(
        VolumeRef::new(
            ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None)?,
            "private".into(),
            class,
            VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
            &mut None,
        )?,
        path.into(),
        version.into(),
        FileDescriptor::from_bytes(bytes, "text/markdown", &mut None)?,
        "GOAL.md".into(),
        &mut None,
    )
}

fn goal(bytes: &[u8]) -> Result<GoalView> {
    GoalView::from_file(
        file("GOAL.md", VolumeClass::AgentPrivate, bytes, "v1")?,
        bytes,
        4096,
    )
}

#[test]
fn frontmatter_cases() -> Result<()> {
    for (text, status, body) in [
        ("", GoalStatus::Active, ""),
        ("Ship it\n", GoalStatus::Active, "Ship it\n"),
        (
            "---\nstatus: active\n---\nShip it\n",
            GoalStatus::Active,
            "Ship it\n",
        ),
        (
            "---\nstatus: blocked\n---\nNeed input",
            GoalStatus::Blocked,
            "Need input",
        ),
        (
            "---\nstatus: done\n---\nFinished",
            GoalStatus::Done,
            "Finished",
        ),
        (
            "---\r\nstatus: done\r\n---\r\nFinished\r\n",
            GoalStatus::Done,
            "Finished\r\n",
        ),
        ("---\n---\nBody", GoalStatus::Active, "Body"),
        ("---\nstatus: done\n---", GoalStatus::Done, ""),
        (
            "# done\nstatus: blocked",
            GoalStatus::Active,
            "# done\nstatus: blocked",
        ),
        ("--- \nBody", GoalStatus::Active, "--- \nBody"),
    ] {
        let view = goal(text.as_bytes())?;
        assert_eq!(view.status, status, "{text:?}");
        assert_eq!(view.markdown, body, "{text:?}");
    }
    Ok(())
}

#[test]
fn malformed_goal_does_not_silently_activate() {
    for text in [
        "---",
        "---\nstatus: active",
        "---\nstatus: other\n---\n",
        "---\nstatus: [done]\n---\n",
        "---\nstatus: null\n---\n",
        "---\nstatus: done\nstatus: active\n---\n",
        "---\nstatus: &s active\n---\n",
        "---\nstatus: *s\n---\n",
        "---\nunknown: active\n---\n",
        "---\nstatus: {nested: active}\n---\n",
    ] {
        assert!(goal(text.as_bytes()).is_err(), "{text:?}");
    }
    assert!(goal(&[0xff]).is_err());
}

#[test]
fn private_root_and_integrity_are_required() -> Result<()> {
    for (path, class) in [
        ("GOAL.md", VolumeClass::Project),
        ("GOAL.md", VolumeClass::SessionShared),
        ("nested/GOAL.md", VolumeClass::AgentPrivate),
    ] {
        assert!(GoalView::from_file(file(path, class, b"goal", "v1")?, b"goal", 4096).is_err());
    }
    let source = file("GOAL.md", VolumeClass::AgentPrivate, b"goal", "v1")?;
    assert!(GoalView::from_file(source.clone(), b"changed", 4096).is_err());
    assert!(GoalView::from_file(source, b"goal", 3).is_err());
    Ok(())
}

#[test]
fn continuation_is_stable_per_settle_and_independent_of_goal_revision() -> Result<()> {
    let task = TaskId::from_bytes([2; 16]);
    let turn = OperationId::from_bytes([3; 16]);
    let view = goal(b"Ship it")?;
    let operation = view.continuation_operation(task, turn);
    assert!(operation.is_some());
    assert_eq!(operation, view.continuation_operation(task, turn));
    assert_ne!(operation, view.continuation_operation(TaskId::new(), turn));
    assert_ne!(
        operation,
        view.continuation_operation(task, OperationId::new())
    );
    let continued_turn = operation.ok_or_else(|| Error::Invalid("missing continuation".into()))?;
    let next = view.continuation_operation(task, continued_turn);
    assert!(next.is_some());
    assert_ne!(next, operation);
    let same_bytes_new_generation = GoalView::from_file(
        file("GOAL.md", VolumeClass::AgentPrivate, b"Ship it", "v2")?,
        b"Ship it",
        4096,
    )?;
    assert_eq!(same_bytes_new_generation.revision(), view.revision());
    assert_eq!(
        same_bytes_new_generation.continuation_operation(task, turn),
        operation
    );
    let edited = goal(b"Ship it with tests")?;
    assert_ne!(edited.revision(), view.revision());
    assert_eq!(edited.continuation_operation(task, turn), operation);
    for status in ["done", "blocked"] {
        let stopped = goal(format!("---\nstatus: {status}\n---\nShip it").as_bytes())?;
        assert_eq!(stopped.continuation_operation(task, turn), None);
        assert_eq!(
            GoalContinuationPolicy::default().on_settle(&stopped, task, turn, 0),
            None
        );
    }
    Ok(())
}

#[test]
fn policy_has_a_finite_configurable_default_and_blocks_at_the_boundary() -> Result<()> {
    let default = GoalContinuationPolicy::default();
    assert_eq!(default.max_continuations, 32);
    let declared: GoalContinuationPolicy =
        serde_json::from_str("{}").map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(declared, default);
    let view = goal(b"Ship it")?;
    let task = TaskId::new();
    let turn = OperationId::new();
    assert!(matches!(
        default.on_settle(&view, task, turn, 31),
        Some(GoalContinuation::Continue(_))
    ));
    let Some(GoalContinuation::Block(contents)) = default.on_settle(&view, task, turn, 32) else {
        return Err(Error::Invalid(
            "expected blocked goal at default cap".into(),
        ));
    };
    let blocked = goal(contents.as_bytes())?;
    assert_eq!(blocked.status, GoalStatus::Blocked);
    assert_eq!(blocked.markdown, view.markdown);
    assert_eq!(
        blocked.reason.as_deref(),
        Some("Maximum goal continuations reached (limit 32).")
    );
    assert_eq!(default.on_settle(&blocked, task, turn, 32), None);
    for limit in [0, 1, 7] {
        let declared: GoalContinuationPolicy =
            serde_json::from_value(serde_json::json!({"max_continuations": limit}))
                .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(declared.max_continuations, limit);
        assert!(matches!(
            declared.on_settle(&view, task, turn, limit),
            Some(GoalContinuation::Block(_))
        ));
    }
    for invalid in [
        "{\"max_continuations\":-1}",
        "{\"max_continuations\":4294967296}",
        "{\"unlimited\":true}",
    ] {
        assert!(serde_json::from_str::<GoalContinuationPolicy>(invalid).is_err());
    }
    Ok(())
}

#[test]
fn accepted_boundary_reserves_room_for_blocked_and_done() -> Result<()> {
    let limit = 4096;
    let body = "x".repeat(limit - GOAL_METADATA_HEADROOM);
    let view = goal(body.as_bytes())?;
    assert!(goal("x".repeat(limit).as_bytes()).is_err());
    assert!(goal(format!("{body}x").as_bytes()).is_err());
    let reason = "x".repeat(GOAL_REASON_BYTES - 2);
    let blocked = view.status_contents(GoalStatus::Blocked, Some(&reason))?;
    assert_eq!(blocked.len(), limit);
    let blocked = goal(blocked.as_bytes())?;
    assert_eq!(blocked.markdown, body);
    assert_eq!(blocked.reason.as_deref(), Some(reason.as_str()));
    let done = blocked.status_contents(GoalStatus::Done, Some(&reason))?;
    assert!(done.len() <= limit);
    assert_eq!(goal(done.as_bytes())?.status, GoalStatus::Done);
    assert!(
        view.status_contents(GoalStatus::Done, Some(&format!("{reason}x")))
            .is_err()
    );
    assert!(
        view.status_contents(GoalStatus::Blocked, Some(&"\0".repeat(22)))
            .is_err()
    );
    let oversized_reason = format!(
        "---\nstatus: active\nreason: {}\n---\n",
        "x".repeat(GOAL_REASON_BYTES)
    );
    assert!(goal(oversized_reason.as_bytes()).is_err());
    Ok(())
}

#[test]
fn yaml_reasons_escape_noncharacters_controls_and_apply_the_encoded_bound() -> Result<()> {
    let view = goal(b"Goal")?;
    for ch in [
        '\u{7f}',
        '\u{81}',
        '\u{fffe}',
        '\u{ffff}',
        '\u{85}',
        '\u{2028}',
        '\u{2029}',
        '\u{feff}',
        '\u{10ffff}',
        '"',
        '\\',
    ] {
        let reason = ch.to_string();
        for status in [GoalStatus::Active, GoalStatus::Blocked, GoalStatus::Done] {
            let contents = view.status_contents(status, Some(&reason))?;
            assert!(contents.is_ascii());
            let rewritten = goal(contents.as_bytes())?;
            assert_eq!(rewritten.reason.as_deref(), Some(reason.as_str()));
            assert_eq!(rewritten.status, status);
        }
    }
    let reason = "\u{81}".repeat(21);
    let contents = view.status_contents(GoalStatus::Blocked, Some(&reason))?;
    assert_eq!(
        goal(contents.as_bytes())?.reason.as_deref(),
        Some(reason.as_str())
    );
    // Raw bytes fit, but the YAML-escaped scalar exceeds the allowance.
    assert!(
        view.status_contents(GoalStatus::Blocked, Some(&"\u{81}".repeat(22)))
            .is_err()
    );
    assert!(
        view.status_contents(GoalStatus::Done, Some(&"\u{10ffff}".repeat(13)))
            .is_err()
    );
    Ok(())
}

proptest! {
    #[test]
    fn every_accepted_goal_status_transition_fits(
        size in 0usize..4097,
        unit_chars in prop::collection::vec(any::<char>(), 0..5),
        extra_limit in 0usize..20000,
        reason_chars in prop::collection::vec(any::<char>(), 0..13),
        cap in any::<u32>(),
    ) {
        let unit: String = unit_chars.into_iter().collect();
        let body = format!("G{}", unit.repeat(size));
        let reason: String = reason_chars.into_iter().collect();
        let required = body.len() + GOAL_METADATA_HEADROOM;
        // Exercise the exact admission boundary, one byte below, and varied limits.
        for limit in [required, required - 1, extra_limit] {
            let plain = file("GOAL.md", VolumeClass::AgentPrivate, body.as_bytes(), "v1")?;
            let accepted = GoalView::from_file(plain, body.as_bytes(), limit as u64);
            prop_assert_eq!(accepted.is_ok(), limit >= required);
            let Ok(view) = accepted else { continue };
            for from in [GoalStatus::Active, GoalStatus::Blocked, GoalStatus::Done] {
                let source = view.status_contents(from, Some(&reason))?;
                prop_assert!(source.len() <= limit);
                let source_ref = file("GOAL.md", VolumeClass::AgentPrivate, source.as_bytes(), "v2")?;
                let source_view = GoalView::from_file(source_ref, source.as_bytes(), limit as u64)?;
                for to in [GoalStatus::Active, GoalStatus::Blocked, GoalStatus::Done] {
                    for next_reason in [None, Some(reason.as_str())] {
                        let rewritten = source_view.status_contents(to, next_reason)?;
                        prop_assert!(rewritten.len() <= limit);
                        let reference = file("GOAL.md", VolumeClass::AgentPrivate, rewritten.as_bytes(), "v3")?;
                        let transitioned = GoalView::from_file(reference, rewritten.as_bytes(), limit as u64)?;
                        prop_assert_eq!(transitioned.status, to);
                        prop_assert_eq!(transitioned.markdown.as_str(), body.as_str());
                        prop_assert_eq!(transitioned.reason.as_deref(), next_reason);
                    }
                }
            }
            let policy = GoalContinuationPolicy { max_continuations: cap };
            let action = policy.on_settle(&view, TaskId::new(), OperationId::new(), cap);
            let Some(GoalContinuation::Block(rewritten)) = action else {
                prop_assert!(false, "expected cap block");
                continue;
            };
            prop_assert!(rewritten.len() <= limit);
            let reference = file("GOAL.md", VolumeClass::AgentPrivate, rewritten.as_bytes(), "v4")?;
            let blocked = GoalView::from_file(reference, rewritten.as_bytes(), limit as u64)?;
            prop_assert_eq!(blocked.status, GoalStatus::Blocked);
            prop_assert_eq!(blocked.markdown.as_str(), body.as_str());
        }
    }
}

#[tokio::test]
async fn boundary_cap_output_is_accepted_by_the_ordinary_writer() -> Result<()> {
    let storage =
        acyclic_harness::filesystem::MemoryHarnessStorage::new(AgentId::new(), 4096).await?;
    let body = "x".repeat(4096 - GOAL_METADATA_HEADROOM);
    let reference = storage
        .stage(
            OperationId::new(),
            "GOAL.md",
            body.as_bytes(),
            "text/markdown",
            "GOAL.md",
        )
        .await?;
    let view = GoalView::from_file(reference, body.as_bytes(), 4096)?;
    let policy = GoalContinuationPolicy {
        max_continuations: u32::MAX,
    };
    let Some(GoalContinuation::Block(contents)) =
        policy.on_settle(&view, TaskId::new(), OperationId::new(), u32::MAX)
    else {
        return Err(Error::Invalid("expected cap block".into()));
    };
    let blocked = storage
        .stage(
            OperationId::new(),
            "GOAL.md",
            contents.as_bytes(),
            "text/markdown",
            "GOAL.md",
        )
        .await?;
    let bytes = storage.read(&blocked).await?;
    let blocked = GoalView::from_file(blocked, &bytes, 4096)?;
    assert_eq!(blocked.status, GoalStatus::Blocked);
    assert_eq!(blocked.markdown, body);
    assert_eq!(
        blocked.reason.as_deref(),
        Some("Maximum goal continuations reached (limit 4294967295).")
    );
    Ok(())
}
