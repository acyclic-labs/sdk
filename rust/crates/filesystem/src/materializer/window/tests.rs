use super::*;
use crate::WorkBudget;

const APPENDS: u64 = 10;
const COPIED: u64 = 100;
const PEAK: u64 = 1 << 20;

fn budget() -> WorkBudget {
    WorkBudget {
        authority_records_appended: APPENDS,
        bytes_copied: COPIED,
        peak_allocation_bytes: PEAK,
        ..crate::performance::BOUNDARY_BUDGET
    }
}

fn root() -> NativeMaterializationWork {
    NativeMaterializationWork::new(budget(), &crate::CancellationToken::new()).unwrap()
}

#[cfg(windows)]
#[test]
fn window_prompt_child() -> std::io::Result<()> {
    use std::io::{Read, Write};
    if std::env::var_os("ACYCLIC_TEST_WINDOW_PROMPT").is_some() {
        let mut input = Vec::new();
        std::io::stdin().read_to_end(&mut input)?;
        assert_eq!(input, b"original window prompt");
        std::io::stdout().write_all(b"WINDOW_PROMPT_ACCEPTED")?;
    }
    Ok(())
}

#[cfg(windows)]
#[tokio::test]
async fn windows_child_prompt_retains_the_actual_invocation_window() -> std::io::Result<()> {
    use acyclic_native_runtime::{CaptureInput, ProcessInput, ProcessTreeStorage};
    use std::process::{Command, Stdio};
    use std::time::Duration;

    let prompt = b"original window prompt";
    for queued in [true, false] {
        let work = NativeMaterializationWork::new(
            WorkBudget {
                bytes_copied: 1 << 20,
                ..budget()
            },
            &crate::CancellationToken::new(),
        )
        .unwrap();
        let window = work.invocation_window().unwrap();
        let meter = window.runtime_meter();
        // This is the production window/base-view meter, whose allocation
        // owner deliberately excludes the current operation window.
        assert!(meter.allocation_owner().is_none());
        let mut process = ProcessTreeStorage::new(&meter)?;
        let mut command = Command::new(std::env::current_exe()?);
        command
            .args([
                "--exact",
                "materializer::window::tests::window_prompt_child",
                "--nocapture",
            ])
            .env("ACYCLIC_TEST_WINDOW_PROMPT", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        process.spawn(&mut command, None)?;
        let before = work.receipt().output_bytes;
        if queued {
            process.io(&meter)?.queue_stdin(prompt.to_vec())?;
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if process.io(&meter)?.pump_stdin()? {
                        return Ok::<_, std::io::Error>(());
                    }
                    process.io(&meter)?.readiness()?.await.into_result()?;
                }
            })
            .await
            .map_err(std::io::Error::other)??;
            process.require_stdin_complete()?;
        }
        let _buffers = window.claim(8192, 2, WorkCounters::UNCHARGED).unwrap();
        let output = process
            .wait_with_output(
                Duration::from_secs(3),
                4096,
                (!queued).then_some(CaptureInput::Literal(prompt)),
                || Ok(()),
                |_| Ok(ProcessInput::default()),
                [
                    Vec::with_capacity(4096),
                    Vec::with_capacity(4096),
                    Vec::new(),
                ],
            )
            .map_err(|failure| failure.error)?;
        assert!(output.status.success());
        assert!(
            output
                .stdout
                .windows(b"WINDOW_PROMPT_ACCEPTED".len())
                .any(|bytes| bytes == b"WINDOW_PROMPT_ACCEPTED")
        );
        assert!(process.is_reaped());
        assert_eq!(work.receipt().output_bytes - before, prompt.len() as u64);
        assert_eq!(
            window.remaining().unwrap().output_bytes,
            budget().output_bytes - prompt.len() as u64
        );
        assert_eq!(
            work.invocation_window()
                .unwrap()
                .remaining()
                .unwrap()
                .output_bytes,
            budget().output_bytes
        );
    }
    Ok(())
}

fn appends(count: u64) -> WorkCounters {
    WorkCounters {
        authority_records_appended: count,
        ..WorkCounters::UNCHARGED
    }
}

fn exceeded(error: &NativeTreeMaterializationError, counter: &str) {
    let shown = format!("{error:?}");
    assert!(
        shown.contains("BudgetExceeded {") && shown.contains(&format!("counter: {counter:?}")),
        "expected {counter} to exceed its window, got {shown}"
    );
}

/// One run of a session: a run window holding an effect window, like a Pi run that
/// dispatches its root effect. Both charge appends directly and bytes through the
/// plain-ledger runtime meter.
fn run(session: &NativeMaterializationWork) -> Result<(), NativeTreeMaterializationError> {
    let run = session.invocation_window()?;
    run.record_admitted(appends(APPENDS - 2))?;
    let effect = run.invocation_window()?;
    effect.record_admitted(appends(APPENDS))?;
    effect
        .runtime_meter()
        .examine(0, COPIED)
        .map_err(|error| effect.runtime_error(error))?;
    run.record_admitted(appends(2))?;
    run.runtime_meter()
        .examine(0, COPIED)
        .map_err(|error| run.runtime_error(error))
}

#[test]
fn every_run_of_a_long_session_gets_its_own_full_window() {
    const RUNS: u64 = 16;
    let session = root();
    for _ in 0..RUNS {
        run(&session).unwrap();
    }
    let receipt = session.receipt();
    // The shared ledger still records every run: only the checks are windowed.
    assert_eq!(receipt.authority_records_appended, RUNS * 2 * APPENDS);
    assert_eq!(receipt.bytes_copied, RUNS * 2 * COPIED);
    assert!(receipt.authority_records_appended > APPENDS);
    // Unwindowed work on the session keeps its own full budget.
    session.record_admitted(appends(APPENDS)).unwrap();
    exceeded(
        &session.record_admitted(appends(1)).unwrap_err(),
        "authority_records_appended",
    );
}

#[test]
fn the_same_session_without_windows_exceeds_the_shared_budget() {
    // Negative control for the session above: every run on the shared ledger itself.
    let session = root();
    session.record_admitted(appends(APPENDS)).unwrap();
    exceeded(
        &session.record_admitted(appends(APPENDS)).unwrap_err(),
        "authority_records_appended",
    );
}

#[test]
fn one_run_over_its_own_window_still_fails() {
    let session = root();
    run(&session).unwrap();
    let over = session.invocation_window().unwrap();
    over.record_admitted(appends(APPENDS)).unwrap();
    exceeded(
        &over.record_admitted(appends(1)).unwrap_err(),
        "authority_records_appended",
    );
    let copied = session.invocation_window().unwrap();
    let meter = copied.runtime_meter();
    meter.examine(0, COPIED).unwrap();
    exceeded(
        &copied.runtime_error(meter.examine(0, 1).unwrap_err()),
        "bytes_copied",
    );
}

#[test]
fn an_enclosing_window_is_never_charged_for_a_nested_one() {
    let session = root();
    let outer = session.invocation_window().unwrap();
    outer.record_admitted(appends(APPENDS - 4)).unwrap();
    let inner = outer.invocation_window().unwrap();
    inner.record_admitted(appends(APPENDS)).unwrap();
    exceeded(
        &inner.record_admitted(appends(1)).unwrap_err(),
        "authority_records_appended",
    );
    // A clone keeps its window, and the outer window still has exactly its own remainder.
    let alias = outer.clone();
    alias.record_admitted(appends(4)).unwrap();
    assert_eq!(outer.remaining().unwrap().authority_records_appended, 0);
    exceeded(
        &outer.record_admitted(appends(1)).unwrap_err(),
        "authority_records_appended",
    );
}

#[test]
fn reservations_and_refunds_stay_exact_across_windows() {
    let session = root();
    let window = session.invocation_window().unwrap();
    let reservation = window.reserve_work(appends(APPENDS)).unwrap();
    assert_eq!(window.remaining().unwrap().authority_records_appended, 0);
    drop(reservation);
    assert_eq!(
        window.remaining().unwrap().authority_records_appended,
        APPENDS
    );
    assert_eq!(
        session.remaining().unwrap().authority_records_appended,
        APPENDS
    );
    window.record_admitted(appends(3)).unwrap();
    assert_eq!(
        window.remaining().unwrap().authority_records_appended,
        APPENDS - 3
    );
    assert_eq!(
        session.remaining().unwrap().authority_records_appended,
        APPENDS
    );
}

#[test]
fn a_reservation_held_by_one_window_never_blocks_a_fresh_window() {
    let session = root();
    let first = session.invocation_window().unwrap();
    let held = first.reserve_work(appends(APPENDS)).unwrap();
    assert_eq!(first.remaining().unwrap().authority_records_appended, 0);
    let second = session.invocation_window().unwrap();
    assert_eq!(
        second.remaining().unwrap().authority_records_appended,
        APPENDS
    );
    second.record_admitted(appends(APPENDS)).unwrap();
    assert_eq!(second.remaining().unwrap().authority_records_appended, 0);
    let third = session.invocation_window().unwrap();
    third.record_admitted(appends(1)).unwrap();
    assert_eq!(first.remaining().unwrap().authority_records_appended, 0);
    drop(held);
    assert_eq!(
        first.remaining().unwrap().authority_records_appended,
        APPENDS
    );
    assert_eq!(second.remaining().unwrap().authority_records_appended, 0);
    assert_eq!(
        third.remaining().unwrap().authority_records_appended,
        APPENDS - 1
    );
}

#[test]
fn a_window_holding_its_full_reservation_cannot_charge() {
    let session = root();
    let window = session.invocation_window().unwrap();
    let _held = window.reserve_work(appends(APPENDS)).unwrap();
    exceeded(
        &window.record_admitted(appends(1)).unwrap_err(),
        "authority_records_appended",
    );
}

#[test]
fn live_allocation_is_never_forgotten_by_a_new_window() {
    let session = root();
    let first = session.invocation_window().unwrap();
    let held = first.claim(PEAK / 2, 1, WorkCounters::UNCHARGED).unwrap();
    drop(first);
    let second = session.invocation_window().unwrap();
    let live = second.live_allocation_bytes();
    assert!(live >= PEAK / 2);
    assert!(
        second
            .claim(PEAK - live + 1, 1, WorkCounters::UNCHARGED)
            .is_err()
    );
    drop(held);
    second.claim(PEAK / 2, 1, WorkCounters::UNCHARGED).unwrap();
}

#[test]
fn window_layout_matches_its_claim() {
    let session = root();
    let before = session.live_allocation_bytes();
    let window = session.invocation_window().unwrap();
    let expected =
        crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<NativeWorkWindow>()).unwrap()
            as u64;
    assert_eq!(session.live_allocation_bytes(), before + expected);
    drop(window);
    assert_eq!(session.live_allocation_bytes(), before);
}

fn left(window: &NativeMaterializationWork) -> u64 {
    window.remaining().unwrap().authority_records_appended
}

#[test]
fn two_windows_hold_full_reservations_at_once() {
    let session = root();
    let first = session.invocation_window().unwrap();
    let second = session.invocation_window().unwrap();
    let first_held = first.reserve_work(appends(APPENDS)).unwrap();
    let second_held = second.reserve_work(appends(APPENDS)).unwrap();
    // The second window holds its whole budget: no later call can grant it more.
    assert_eq!(left(&second), 0);
    assert!(second.reserve_work(appends(1)).is_err());
    assert_eq!(left(&second), 0);
    assert_eq!(left(&first), 0);
    drop(second_held);
    assert_eq!(left(&second), APPENDS);
    assert_eq!(left(&first), 0);
    drop(first_held);
    assert_eq!(left(&first), APPENDS);
    assert_eq!(left(&session), APPENDS);
}

#[test]
fn interleaved_reserve_charge_and_release_stay_exact_per_window() {
    let session = root();
    let first = session.invocation_window().unwrap();
    let second = session.invocation_window().unwrap();
    let first_held = first.reserve_work(appends(4)).unwrap();
    let second_held = second.reserve_work(appends(7)).unwrap();
    assert_eq!((left(&first), left(&second)), (6, 3));
    first.record_admitted(appends(3)).unwrap();
    second_held.record_completed(appends(5)).unwrap();
    assert_eq!((left(&first), left(&second)), (3, 3));
    drop(first_held);
    assert_eq!((left(&first), left(&second)), (7, 3));
    second.record_admitted(appends(3)).unwrap();
    assert_eq!((left(&first), left(&second)), (7, 0));
    assert!(second.reserve_work(appends(1)).is_err());
    drop(second_held);
    assert_eq!((left(&first), left(&second)), (7, 2));
    assert_eq!(left(&session), APPENDS);
}

#[derive(Clone, Debug)]
enum Step {
    Reserve(usize, u64),
    Spend(usize, usize, u64),
    Charge(usize, u64),
    Release(usize, usize),
}

fn step() -> impl proptest::strategy::Strategy<Value = Step> {
    use proptest::prelude::*;
    prop_oneof![
        (0..3_usize, 0..=APPENDS).prop_map(|(window, count)| Step::Reserve(window, count)),
        (0..3_usize, 0..4_usize, 0..=APPENDS)
            .prop_map(|(window, held, count)| Step::Spend(window, held, count)),
        (0..3_usize, 0..=APPENDS).prop_map(|(window, count)| Step::Charge(window, count)),
        (0..3_usize, 0..4_usize).prop_map(|(window, held)| Step::Release(window, held)),
    ]
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(256))]
    /// Any interleaving across three windows: no window ever has more than its own
    /// limit, and every release restores exactly what its reservation still held.
    #[test]
    fn every_interleaving_keeps_each_window_within_its_own_limit(
        steps in proptest::collection::vec(step(), 1..40),
    ) {
        let session = root();
        let windows: Vec<_> = (0..3).map(|_| session.invocation_window().unwrap()).collect();
        let mut held: Vec<Vec<NativeWorkReservation>> = (0..3).map(|_| Vec::new()).collect();
        let mut consumed = [0_u64; 3];
        let reserved = |held: &[NativeWorkReservation]| -> u64 {
            held.iter().map(|r| r.remaining_work().authority_records_appended).sum()
        };
        for step in steps {
            match step {
                Step::Reserve(w, count) => {
                    let fits = reserved(&held[w]) + consumed[w] + count <= APPENDS;
                    let result = windows[w].reserve_work(appends(count));
                    proptest::prop_assert_eq!(result.is_ok(), fits);
                    if let Ok(reservation) = result {
                        held[w].push(reservation);
                    }
                }
                Step::Spend(w, index, count) => {
                    if let Some(reservation) = held[w].get(index % held[w].len().max(1)) {
                        let count = count % (reservation.remaining_work().authority_records_appended + 1);
                        reservation.record_completed(appends(count)).unwrap();
                        consumed[w] += count;
                    }
                }
                Step::Charge(w, count) => {
                    if reserved(&held[w]) + consumed[w] + count <= APPENDS {
                        windows[w].record_admitted(appends(count)).unwrap();
                        consumed[w] += count;
                    } else {
                        proptest::prop_assert!(windows[w].reserve_work(appends(count)).is_err());
                    }
                }
                Step::Release(w, index) => {
                    if !held[w].is_empty() {
                        let index = index % held[w].len();
                        drop(held[w].swap_remove(index));
                    }
                }
            }
            for w in 0..3 {
                proptest::prop_assert_eq!(left(&windows[w]), APPENDS - reserved(&held[w]) - consumed[w]);
            }
            proptest::prop_assert_eq!(left(&session), APPENDS);
        }
        drop(held);
        for w in 0..3 {
            proptest::prop_assert_eq!(left(&windows[w]), APPENDS - consumed[w]);
        }
    }
}

#[test]
fn unbounded_windows_reserve_complete_and_refund_exactly() {
    let session =
        NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .unwrap();
    let window = session.invocation_window().unwrap();
    let completed = window.reserve_work(appends(3)).unwrap();
    let refunded = window.reserve_work(appends(4)).unwrap();
    let unbounded = || window.remaining().unwrap().authority_records_appended;
    assert_eq!(unbounded(), u64::MAX - 7);
    completed.record_completed(appends(2)).unwrap();
    drop(completed);
    drop(refunded);
    window.record_admitted(appends(1)).unwrap();
    assert_eq!(
        session.receipt().authority_records_appended,
        3,
        "every completed and admitted append is recorded once"
    );
    // Nothing poisoned the shared ledger: every context still charges.
    session.record_admitted(appends(1)).unwrap();
    session
        .invocation_window()
        .unwrap()
        .reserve_work(appends(5))
        .unwrap()
        .record_completed(appends(5))
        .unwrap();
    assert!(window.remaining().is_ok());
}

#[test]
fn opening_windows_leaves_no_charge_on_the_session() {
    const OPERATIONS: u64 = 8;
    let session = NativeMaterializationWork::new(
        WorkBudget {
            allocation_operations: OPERATIONS,
            ..budget()
        },
        &crate::CancellationToken::new(),
    )
    .unwrap();
    let before = session.remaining().unwrap().allocation_operations;
    for _ in 0..4 * OPERATIONS {
        let window = session.invocation_window().unwrap();
        // The handle's own allocation is the window's work.
        assert_eq!(
            window.remaining().unwrap().allocation_operations,
            OPERATIONS - 1
        );
        drop(window);
    }
    assert_eq!(session.remaining().unwrap().allocation_operations, before);
}

#[test]
fn native_io_of_a_view_is_charged_to_the_window_opened_on_it() {
    let session = root();
    let (view, _cancellation) = session.invocation_view().unwrap();
    let copied = |work: &NativeMaterializationWork, bytes| {
        work.runtime_meter()
            .examine(0, bytes)
            .map_err(|error| work.runtime_error(error))
    };
    let first = view.invocation_window().unwrap();
    let second = view.invocation_window().unwrap();
    // Each window spends its own full copy budget through native I/O.
    copied(&first, COPIED).unwrap();
    copied(&second, COPIED).unwrap();
    // Direct charges and native I/O spend the same window budget.
    exceeded(
        &first
            .record_admitted(WorkCounters {
                bytes_copied: 1,
                ..WorkCounters::UNCHARGED
            })
            .unwrap_err(),
        "bytes_copied",
    );
    let third = view.invocation_window().unwrap();
    third
        .record_admitted(WorkCounters {
            bytes_copied: COPIED,
            ..WorkCounters::UNCHARGED
        })
        .unwrap();
    exceeded(&copied(&third, 1).unwrap_err(), "bytes_copied");
    // A recorded overrun is still work done; a refused native charge is not.
    assert_eq!(session.receipt().bytes_copied, 3 * COPIED + 1);
}

#[test]
fn a_window_never_widens_a_native_work_handle() {
    // Every native allocation and error custody carries a work handle by value: the
    // window replaces a per-handle copy of the ledger's cancellation, which the shared
    // ledger now holds once, so the handle keeps its size without a window.
    let words = if cfg!(feature = "native-mount") { 7 } else { 6 };
    assert_eq!(
        std::mem::size_of::<NativeMaterializationWork>(),
        words * std::mem::size_of::<usize>()
    );
}

#[test]
fn one_window_overrunning_its_budget_leaves_every_other_window_running() {
    let session = root();
    let first = session.invocation_window().unwrap();
    exceeded(
        &first.record_admitted(appends(APPENDS + 1)).unwrap_err(),
        "authority_records_appended",
    );
    // The overrun is the first window's own refusal, and it stays refused.
    assert!(first.remaining().is_err());
    let second = session.invocation_window().unwrap();
    second.record_admitted(appends(APPENDS)).unwrap();
    assert_eq!(second.remaining().unwrap().authority_records_appended, 0);
    assert_eq!(
        session.remaining().unwrap().authority_records_appended,
        APPENDS
    );
    session
        .invocation_window()
        .unwrap()
        .record_admitted(appends(1))
        .unwrap();
}

#[test]
fn the_same_overrun_on_the_shared_ledger_refuses_every_later_caller() {
    let session = root();
    exceeded(
        &session.record_admitted(appends(APPENDS + 1)).unwrap_err(),
        "authority_records_appended",
    );
    assert!(session.remaining().is_err());
    assert!(
        session
            .outside_window()
            .record_admitted(appends(1))
            .is_err()
    );
}

#[test]
fn a_runtime_failure_is_reported_only_to_the_window_that_failed() {
    let session = root();
    let first = session.invocation_window().unwrap();
    let copied = first.runtime_meter().examine(0, COPIED + 1).unwrap_err();
    let second = session.invocation_window().unwrap();
    let allocated = second.runtime_meter().allocate(PEAK + 1).unwrap_err();
    exceeded(&second.runtime_error(allocated), "peak_allocation_bytes");
    exceeded(&first.runtime_error(copied), "bytes_copied");
    // A failure outside every window stays with the unwindowed ledger.
    let plain = session.runtime_meter().examine(0, COPIED + 1).unwrap_err();
    assert!(
        session
            .invocation_window()
            .unwrap()
            .take_native_io_failure()
            .is_none()
    );
    exceeded(&session.runtime_error(plain), "bytes_copied");
}
