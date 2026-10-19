//! A checkout's incarnation is the token published in its held root, bound to
//! the root's object id where the filesystem keeps it stable.
use super::super::config::LocalProject;
use super::super::owner::{
    Checkout, DEVICE_KEYED, INJECTED, Owner, RootWitness, STATE_ROOT, UNSTABLE_KEYED, UNTOKENED,
    directory, incarnation_of, state_directory_in,
};
use super::*;
use acyclic_fs::NativeRootGeneration::{FileHandle, StableObjectId, Unstable};
use acyclic_fs::facade::FsWork as _;

/// An object id that every injected capture reports, as a filesystem that
/// reissues a deleted directory's id to the next one.
const REUSED: [u8; 16] = [5; 16];

/// Clears every capture injection and control on every exit.
struct Reset;
impl Drop for Reset {
    fn drop(&mut self) {
        INJECTED.set(None);
        UNTOKENED.set(false);
        UNSTABLE_KEYED.set(false);
        DEVICE_KEYED.set(false);
        STATE_ROOT.set(None);
        super::super::workspace::follow_path(false);
    }
}

/// The owner that state selection and validation attach to the checkout at
/// `workspace`, under the per-user data root `users`.
fn attached(workspace: &std::path::Path, users: &std::path::Path) -> Result<Owner> {
    let checkout = Checkout::capture(workspace)?;
    let project = LocalProject::Workspace(workspace.to_path_buf());
    let state = directory(&state_directory_in(users, &project, Some(&checkout))?)?;
    Owner::open(&state, &project, Some(&checkout))
}

/// Over `steps` (true: delete and re-create the checkout; false: reopen it)
/// on a filesystem that reissues the same object id, every re-created
/// checkout attaches a new owner and every reopen keeps its owner.
fn recreations_attach_new_state(steps: &[bool]) -> std::result::Result<(), String> {
    let parent = tempfile::tempdir().map_err(|error| error.to_string())?;
    let users = tempfile::tempdir().map_err(|error| error.to_string())?;
    let workspace = parent.path().join("checkout");
    std::fs::create_dir(&workspace).map_err(|error| error.to_string())?;
    let _reset = Reset;
    INJECTED.set(Some((REUSED, StableObjectId)));
    let attach = || {
        attached(&workspace, users.path())
            .map(|owner| owner.agent)
            .map_err(|error| error.to_string())
    };
    let mut owners = vec![attach()?];
    for (index, recreate) in steps.iter().enumerate() {
        if *recreate {
            std::fs::remove_dir_all(&workspace).map_err(|error| error.to_string())?;
            std::fs::create_dir(&workspace).map_err(|error| error.to_string())?;
        }
        let owner = attach()?;
        if *recreate && owners.contains(&owner) {
            return Err(format!(
                "step {index}: a re-created checkout attached stale state"
            ));
        }
        if !*recreate && owners.last() != Some(&owner) {
            return Err(format!("step {index}: a reopened checkout lost its state"));
        }
        if *recreate {
            owners.push(owner);
        }
    }
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    /// Any sequence of reopening, deleting and re-creating a checkout on a
    /// reused object id attaches new state to each re-creation only.
    #[test]
    fn every_recreation_on_a_reused_object_id_attaches_new_state(
        steps in proptest::collection::vec(proptest::prelude::any::<bool>(), 1..8),
    ) {
        proptest::prop_assert_eq!(recreations_attach_new_state(&steps), Ok(()));
    }
}

/// Negative control: without the token, a checkout re-created on a reused
/// object id attaches its predecessor's state.
#[test]
fn negative_control_an_untokened_incarnation_attaches_stale_state() {
    UNTOKENED.set(true);
    let attached = recreations_attach_new_state(&[true]);
    UNTOKENED.set(false);
    assert!(
        attached.is_err_and(|error| error.contains("attached stale state")),
        "a re-created checkout kept new state without its token"
    );
}

/// The default entry point opens a checkout re-created on a reused object id
/// with new per-user state, and accepts its prompts.
#[tokio::test]
async fn a_checkout_recreated_on_a_reused_object_id_opens_new_state() -> Result<()> {
    let parent = tempfile::tempdir().map_err(super::super::storage)?;
    let users = tempfile::tempdir().map_err(super::super::storage)?;
    let workspace = parent.path().join("checkout");
    let _reset = Reset;
    STATE_ROOT.set(Some(users.path().to_path_buf()));
    INJECTED.set(Some((REUSED, StableObjectId)));
    for _ in 0..2 {
        std::fs::create_dir(&workspace).map_err(super::super::storage)?;
        let harness =
            LocalHarness::open(super::checkout_config(&workspace, (model()?, text()))?).await?;
        super::checkout_reopen::run(&harness).await?;
        harness.close().await?;
        std::fs::remove_dir_all(&workspace).map_err(super::super::storage)?;
    }
    let states = std::fs::read_dir(users.path().join("acyclic/harness"))
        .map_err(super::super::storage)?
        .count();
    assert_eq!(
        states, 2,
        "the re-created checkout attached its predecessor's state"
    );
    Ok(())
}

/// Reopen explicit state after a remount: an unstable filesystem may renumber
/// everything, while a stable one may renumber the Unix device alone. Controls
/// restore either unstable object ids or the mount's device number to the key.
#[tokio::test]
async fn a_checkout_without_stable_ids_reopens_its_state_after_renumbering() -> Result<()> {
    async fn renumbered(
        keyed: bool,
        generation: acyclic_fs::NativeRootGeneration,
    ) -> Result<Result<()>> {
        let workspace = tempfile::tempdir().map_err(super::super::storage)?;
        let state = tempfile::tempdir().map_err(super::super::storage)?;
        let config = || {
            super::checkout_config(workspace.path(), (model()?, text()))
                .map(|config| config.state_directory(state.path()))
        };
        let _reset = Reset;
        UNSTABLE_KEYED.set(keyed);
        DEVICE_KEYED.set(keyed);
        INJECTED.set(Some(([1; 16], generation)));
        let harness = LocalHarness::open(config()?).await?;
        super::checkout_reopen::run(&harness).await?;
        harness.close().await?;
        // A remount can renumber the device, and unstable object ids as well.
        let mut remounted = [1; 16];
        remounted[..8].fill(2);
        if generation == Unstable {
            remounted.fill(2);
        }
        INJECTED.set(Some((remounted, generation)));
        Ok(match LocalHarness::open(config()?).await {
            Ok(harness) => {
                super::checkout_reopen::run(&harness).await?;
                harness.close().await
            }
            Err(error) => Err(error),
        })
    }
    let generations = [Unstable, StableObjectId, FileHandle([3; 32])];
    for generation in generations {
        if generation != Unstable && !cfg!(unix) {
            continue;
        }
        renumbered(false, generation).await??;
        assert!(
            matches!(
                renumbered(true, generation).await?,
                Err(Error::Unauthorized(_))
            ),
            "a device/id-keyed root kept its state across renumbering: {generation:?}"
        );
    }
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    /// State selection depends on the token; strong evidence independently
    /// rejects replacement roots and ignores remount-local device numbers.
    #[test]
    fn token_selection_and_root_evidence_are_independent(
        objects: ([u8; 16], [u8; 16]), handle: [u8; 32],
        tokens: ([u8; 16], [u8; 16]),
    ) {
        proptest::prop_assert_eq!(incarnation_of(tokens.0), incarnation_of(tokens.0));
        if tokens.0 != tokens.1 { proptest::prop_assert_ne!(incarnation_of(tokens.0), incarnation_of(tokens.1)); }
        let distinct = if cfg!(unix) { objects.0[8..] != objects.1[8..] } else { objects.0 != objects.1 };
        for generation in [FileHandle(handle), StableObjectId] {
            let original = RootWitness::capture(objects.0, generation);
            if distinct {
                proptest::prop_assert!(matches!(original.verify(RootWitness::capture(objects.1, generation)), Err(Error::Unauthorized(_))));
            }
            #[cfg(unix)] {
                let mut remounted = objects.0;
                remounted[..8].copy_from_slice(&objects.1[..8]);
                proptest::prop_assert!(original.verify(RootWitness::capture(remounted, generation)).is_ok());
                DEVICE_KEYED.set(true);
                let old = RootWitness::capture(objects.0, generation);
                let new = RootWitness::capture(remounted, generation);
                DEVICE_KEYED.set(false);
                if objects.0[..8] != objects.1[..8] { proptest::prop_assert!(old.verify(new).is_err()); }
            }
        }
    }
}

async fn changed_root(
    explicit: bool,
    before: acyclic_fs::NativeRootGeneration,
    after: acyclic_fs::NativeRootGeneration,
    replacement: Option<[u8; 16]>,
) -> Result<Result<()>> {
    let parent = tempfile::tempdir().map_err(super::super::storage)?;
    let users = tempfile::tempdir().map_err(super::super::storage)?;
    let state = parent.path().join("state");
    let workspace = parent.path().join("checkout");
    std::fs::create_dir(&workspace).map_err(super::super::storage)?;
    let _reset = Reset;
    STATE_ROOT.set(Some(users.path().to_path_buf()));
    let config = || {
        super::checkout_config(&workspace, (model()?, text())).map(|config| {
            if explicit {
                config.state_directory(&state)
            } else {
                config
            }
        })
    };
    INJECTED.set(Some((REUSED, before)));
    let harness = LocalHarness::open(config()?).await?;
    super::checkout_reopen::run(&harness).await?;
    harness.close().await?;
    let owner_path = if explicit {
        state.join("owner.json")
    } else {
        std::fs::read_dir(users.path().join("acyclic/harness"))
            .map_err(super::super::storage)?
            .next()
            .unwrap()
            .map_err(super::super::storage)?
            .path()
            .join("owner.json")
    };
    let original = std::fs::read(&owner_path).map_err(super::super::storage)?;
    if replacement.is_some() {
        // Fixture SDK token only; this never reads a user login or home.
        let token = std::fs::read(workspace.join(".volumes/.incarnation/token"))
            .map_err(super::super::storage)?;
        std::fs::rename(&workspace, parent.path().join("original"))
            .map_err(super::super::storage)?;
        std::fs::create_dir_all(workspace.join(".volumes/.incarnation"))
            .map_err(super::super::storage)?;
        std::fs::write(workspace.join(".volumes/.incarnation/token"), token)
            .map_err(super::super::storage)?;
    }
    INJECTED.set(Some((replacement.unwrap_or(REUSED), after)));
    let result = match LocalHarness::open(config()?).await {
        Ok(harness) => {
            super::checkout_reopen::run(&harness).await?;
            harness.close().await
        }
        Err(error) => Err(error),
    };
    assert_eq!(
        std::fs::read(&owner_path).map_err(super::super::storage)?,
        original
    );
    if !explicit {
        assert_eq!(
            std::fs::read_dir(users.path().join("acyclic/harness"))
                .map_err(super::super::storage)?
                .count(),
            1
        );
    }
    Ok(result)
}

/// Both entry points keep the selected owner through verifiable changes and
/// refuse ambiguous downgrades without silently allocating a second state.
#[tokio::test]
async fn capability_changes_keep_state_and_verify_retained_evidence() -> Result<()> {
    for explicit in [false, true] {
        changed_root(explicit, StableObjectId, FileHandle([3; 32]), None).await??;
        changed_root(explicit, Unstable, StableObjectId, None).await??;
        for (before, after) in [
            (FileHandle([3; 32]), StableObjectId),
            (StableObjectId, Unstable),
            (FileHandle([3; 32]), Unstable),
        ] {
            assert!(matches!(
                changed_root(explicit, before, after, None).await?,
                Err(Error::Unsupported(_))
            ));
        }
    }
    Ok(())
}

/// A copied token locates the original state but cannot replace its strong
/// object/handle proof. This catches a token-only hash or owner comparison.
#[tokio::test]
async fn copied_tokens_cannot_attach_another_strongly_bound_root() -> Result<()> {
    for explicit in [false, true] {
        for (before, object, after) in [
            (StableObjectId, [9; 16], StableObjectId),
            (FileHandle([3; 32]), REUSED, FileHandle([4; 32])),
            (FileHandle([3; 32]), [9; 16], FileHandle([3; 32])),
        ] {
            assert!(matches!(
                changed_root(explicit, before, after, Some(object)).await?,
                Err(Error::Unauthorized(_))
            ));
        }
    }
    Ok(())
}

/// Replacing the checkout's path after its root is held and before scratch
/// setup never redirects setup: every setup side effect works in the held
/// root. Negative control: setup that reopens the saved path writes into the
/// replacement.
#[cfg(unix)]
#[tokio::test]
async fn replacing_the_checkout_before_setup_never_redirects_it() -> Result<()> {
    /// Whether setup touched the replacement.
    async fn replaced(follow: bool) -> Result<bool> {
        let parent = tempfile::tempdir().map_err(super::super::storage)?;
        let state = tempfile::tempdir().map_err(super::super::storage)?;
        let workspace = parent.path().join("checkout");
        let moved = parent.path().join("moved");
        std::fs::create_dir(&workspace).map_err(super::super::storage)?;
        let _reset = Reset;
        let (from, to) = (workspace.clone(), moved.clone());
        super::super::workspace::replace_before_setup(move |_| {
            std::fs::rename(&from, &to).unwrap();
            std::fs::create_dir(&from).unwrap();
        });
        super::super::workspace::follow_path(follow);
        let harness = LocalHarness::open(
            super::checkout_config(&workspace, (model()?, text()))?.state_directory(state.path()),
        )
        .await?;
        harness.close().await?;
        assert!(moved.join(".volumes/.incarnation/token").is_file());
        Ok(workspace.join(".volumes").exists())
    }
    assert!(
        !replaced(false).await?,
        "scratch setup wrote into the replacement"
    );
    assert!(
        replaced(true).await?,
        "setup that reopens the saved path left the replacement untouched"
    );
    Ok(())
}

/// Both initial source selection and later watcher capture preserve authored
/// siblings while keeping the SDK incarnation subtree outside the conversation.
#[tokio::test]
async fn incarnation_controls_never_enter_initial_or_watched_capture() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state = tempfile::tempdir().map_err(super::super::storage)?;
    let authored = directory.path().join(".volumes/authored");
    std::fs::create_dir_all(directory.path().join(".volumes")).map_err(super::super::storage)?;
    std::fs::write(&authored, b"base").map_err(super::super::storage)?;
    let fs = crate::tools::defaults()?
        .get("fs", None, None)?
        .ok_or_else(|| Error::NotFound("fs".into()))?
        .clone();
    let provider = scripted([
        super::checked_commands::call(0, "fs", "status"),
        ScriptedTurn::Text("baseline".into()),
        super::checked_commands::call(1, "fs", "status"),
        ScriptedTurn::Text("watched".into()),
    ]);
    let harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, provider))?
            .state_directory(state.path())
            .without_default_tools()
            .tool(fs),
    )
    .await?;
    let token_path = directory.path().join(".volumes/.incarnation/token");
    let token = std::fs::read(&token_path).map_err(super::super::storage)?;
    let workspace = harness
        .workspace()
        .ok_or_else(|| Error::NotFound("checkout".into()))?
        .view();
    let cancellation = acyclic_fs::CancellationToken::new();
    let initial = workspace
        .read(
            "/.volumes/.incarnation/token",
            64,
            &mut acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation),
        )
        .await;
    assert!(matches!(
        initial,
        Err(acyclic_fs::LazyWorkspaceError::NotFound)
    ));
    for (reply, expected) in [("baseline", b"base".as_slice()), ("watched", b"changed")] {
        if reply == "watched" {
            std::fs::write(&authored, expected).map_err(super::super::storage)?;
            std::fs::write(token_path.with_file_name("private-probe"), b"control")
                .map_err(super::super::storage)?;
        }
        assert_eq!(harness.run(reply).await?.text, reply);
        let head = workspace
            .workspace()
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(super::super::storage)?
            .value;
        let contents = head
            .read(
                "/.volumes/authored",
                64,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(super::super::storage)?
            .value;
        assert_eq!(contents.as_ref(), expected);
        for path in [
            "/.volumes/.incarnation/token",
            "/.volumes/.incarnation/private-probe",
        ] {
            let captured = head
                .read(path, 64, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
                .await;
            assert!(
                matches!(captured, Err(failure) if matches!(failure.error, acyclic_fs::WorkspaceError::NotFound)),
                "private control entered capture: {path}"
            );
        }
    }
    assert_eq!(
        std::fs::read(&token_path).map_err(super::super::storage)?,
        token
    );
    harness.close().await
}
