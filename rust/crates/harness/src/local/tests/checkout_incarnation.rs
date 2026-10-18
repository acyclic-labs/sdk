//! A checkout's incarnation is the token published in its held root, bound to
//! the root's object id where the filesystem keeps it stable.
use super::super::config::LocalProject;
use super::super::owner::{
    Checkout, INJECTED, Owner, STATE_ROOT, UNSTABLE_KEYED, UNTOKENED, directory, incarnation_of,
    state_directory_in,
};
use super::*;
use acyclic_fs::NativeRootGeneration::{FileHandle, StableObjectId, Unstable};

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

/// A filesystem that may renumber the root across remounts is keyed on its
/// token alone and needs no identity capability, with explicit state: a
/// renumbered root reopens its state. Negative control: keying it on the
/// object id refuses the renumbered root its own state.
#[tokio::test]
async fn a_checkout_without_stable_ids_reopens_its_state_after_renumbering() -> Result<()> {
    async fn renumbered(keyed: bool) -> Result<Result<()>> {
        let workspace = tempfile::tempdir().map_err(super::super::storage)?;
        let state = tempfile::tempdir().map_err(super::super::storage)?;
        let config = || {
            super::checkout_config(workspace.path(), (model()?, text()))
                .map(|config| config.state_directory(state.path()))
        };
        let _reset = Reset;
        UNSTABLE_KEYED.set(keyed);
        INJECTED.set(Some(([1; 16], Unstable)));
        let harness = LocalHarness::open(config()?).await?;
        super::checkout_reopen::run(&harness).await?;
        harness.close().await?;
        // A remount gives the same root another object id.
        INJECTED.set(Some(([2; 16], Unstable)));
        Ok(match LocalHarness::open(config()?).await {
            Ok(harness) => {
                super::checkout_reopen::run(&harness).await?;
                harness.close().await
            }
            Err(error) => Err(error),
        })
    }
    renumbered(false).await??;
    assert!(
        matches!(renumbered(true).await?, Err(Error::Unauthorized(_))),
        "an id-keyed unstable root kept its state across renumbering"
    );
    Ok(())
}

proptest::proptest! {
    /// The token separates incarnations; a stable object id and its handle
    /// separate roots; an unstable id never enters the key.
    #[test]
    fn an_incarnation_is_its_token_bound_to_a_stable_id(
        objects: ([u8; 16], [u8; 16]),
        handle: [u8; 32],
        tokens: ([u8; 16], [u8; 16]),
    ) {
        let first = incarnation_of(objects.0, FileHandle(handle), tokens.0);
        proptest::prop_assert_eq!(first, incarnation_of(objects.0, FileHandle(handle), tokens.0));
        if tokens.0 != tokens.1 {
            for generation in [FileHandle(handle), StableObjectId, Unstable] {
                proptest::prop_assert_ne!(
                    incarnation_of(objects.0, generation, tokens.0),
                    incarnation_of(objects.0, generation, tokens.1),
                    "a new token kept the incarnation"
                );
            }
        }
        if objects.0 != objects.1 {
            proptest::prop_assert_ne!(first, incarnation_of(objects.1, FileHandle(handle), tokens.0));
            proptest::prop_assert_ne!(
                incarnation_of(objects.0, StableObjectId, tokens.0),
                incarnation_of(objects.1, StableObjectId, tokens.0)
            );
        }
        proptest::prop_assert_eq!(
            incarnation_of(objects.0, Unstable, tokens.0),
            incarnation_of(objects.1, Unstable, tokens.0),
            "a renumbered unstable root changed its incarnation"
        );
        proptest::prop_assert_ne!(first, incarnation_of(objects.0, StableObjectId, tokens.0));
        proptest::prop_assert_ne!(
            incarnation_of(objects.0, StableObjectId, tokens.0),
            incarnation_of(objects.0, Unstable, tokens.0)
        );
    }
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
