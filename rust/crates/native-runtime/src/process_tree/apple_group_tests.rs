//! The macOS decision after a process-group kill fails with EPERM.

use super::{
    Probe, decide_group_with, group_has_no_live_members, group_has_no_live_members_within,
};
use crate::process_tree::ProcessTree;
use crate::tests::ReadBudgetMeter;
use std::io;
use std::os::unix::process::CommandExt as _;
use std::process::{Child, Command};
use std::sync::atomic::Ordering;

/// Blocks until `pid`, a child of this process, has exited, leaving it unreaped.
fn exited(pid: u32) -> io::Result<()> {
    loop {
        // SAFETY: plain fields; all-zero is a valid value.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        // SAFETY: writable status storage and this process's own child.
        if unsafe {
            libc::waitid(
                libc::P_PID,
                pid,
                &raw mut info,
                libc::WEXITED | libc::WNOWAIT,
            )
        } == 0
        {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

/// A tree whose leader exited first, and `count` members that exited after
/// it, each left unreaped by its parent: every member is a zombie.
fn zombie_group(count: usize) -> io::Result<(ProcessTree, Vec<Child>)> {
    let tree = ProcessTree::spawn(Command::new("/bin/sleep").arg("30"))?;
    let group = tree.guard.process_group;
    // SAFETY: signalling this test's own unreaped child.
    assert_eq!(unsafe { libc::kill(group, libc::SIGKILL) }, 0);
    exited(u32::try_from(group).map_err(io::Error::other)?)?;
    let mut members = Vec::new();
    for _ in 0..count {
        // The unreaped leader keeps the group, which this session may join.
        let member = Command::new("/usr/bin/true").process_group(group).spawn()?;
        exited(member.id())?;
        members.push(member);
    }
    // SAFETY: signalling the group of this test's own unreaped leader.
    assert_eq!(unsafe { libc::kill(-group, libc::SIGKILL) }, -1);
    assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::EPERM));
    Ok((tree, members))
}

fn reap(tree: &mut ProcessTree, members: Vec<Child>) -> io::Result<()> {
    tree.terminate(None)?;
    assert!(tree.is_reaped());
    for mut member in members {
        assert!(member.wait()?.success());
    }
    Ok(())
}

/// A group whose members are all zombies refuses the kill with EPERM, and is
/// decided gone at once: one listing, one probe per member, one first entry,
/// and no wait. The members exited after the leader list ahead of it, as in a
/// tree whose leader dies first, so a decision that looks for the leader
/// alone has to wait for other parents to reap them.
#[test]
fn eperm_from_a_group_of_zombies_is_decided_gone_without_waiting() -> io::Result<()> {
    let (mut tree, members) = zombie_group(2)?;
    let group = tree.guard.process_group;
    let meter = ReadBudgetMeter::new(u64::MAX, u64::MAX);
    assert!(group_has_no_live_members(group, Some(&meter))?);
    assert_eq!(meter.used_operations.load(Ordering::Acquire), 5);
    // Production entry: termination meets the same EPERM and succeeds.
    reap(&mut tree, members)
}

/// A group too large for the in-place listing is listed again whole, into
/// storage the meter admits and gets back.
#[test]
fn a_group_larger_than_the_listing_is_listed_whole() -> io::Result<()> {
    let (mut tree, members) = zombie_group(3)?;
    let group = tree.guard.process_group;
    let meter = ReadBudgetMeter::new(u64::MAX, u64::MAX);
    assert!(group_has_no_live_members_within::<2>(group, Some(&meter))?);
    // Full listing, process count, whole listing, four probes, first entry.
    assert_eq!(meter.used_operations.load(Ordering::Acquire), 8);
    assert_eq!(meter.used_allocations.load(Ordering::Acquire), 1);
    assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    reap(&mut tree, members)
}

/// A member that has not exited keeps the group, decided by its probe alone:
/// live members list first.
#[test]
fn a_live_member_keeps_the_group() -> io::Result<()> {
    let mut tree = ProcessTree::spawn(Command::new("/bin/sleep").arg("30"))?;
    let group = tree.guard.process_group;
    let member = Command::new("/usr/bin/true").process_group(group).spawn()?;
    exited(member.id())?;
    let meter = ReadBudgetMeter::new(u64::MAX, u64::MAX);
    assert!(!group_has_no_live_members(group, Some(&meter))?);
    assert_eq!(meter.used_operations.load(Ordering::Acquire), 2);
    reap(&mut tree, vec![member])
}

/// The decision over every combination of up to three probed members and
/// any second-listing first entry: the group is gone exactly when no member
/// was live and the first entry, if any, is a member a probe saw dead.
#[test]
fn the_decision_is_gone_exactly_when_the_first_entry_was_seen_dead() -> io::Result<()> {
    const PROBES: [Probe; 3] = [Probe::Live, Probe::Elsewhere, Probe::Dead];
    const PIDS: [libc::pid_t; 3] = [11, 12, 13];
    const NEWCOMER: libc::pid_t = 99;
    let mut outcomes: Vec<Vec<Probe>> = vec![Vec::new()];
    for _ in PIDS {
        let longer: Vec<Vec<Probe>> = outcomes
            .iter()
            .filter(|found| found.len() == outcomes.last().map_or(0, Vec::len))
            .flat_map(|found| {
                PROBES.iter().map(move |probe| {
                    let mut found = found.clone();
                    found.push(*probe);
                    found
                })
            })
            .collect();
        outcomes.extend(longer);
    }
    assert_eq!(outcomes.len(), 1 + 3 + 9 + 27);
    for found in &outcomes {
        let pids = PIDS.get(..found.len()).unwrap_or_default();
        let firsts = pids.iter().copied().map(Some).chain([None, Some(NEWCOMER)]);
        for first in firsts {
            let mut members = pids.to_vec();
            let mut probed = Vec::new();
            let gone = decide_group_with(
                &mut members,
                |member| {
                    probed.push(member);
                    let index = pids.iter().position(|pid| *pid == member);
                    Ok(*index
                        .and_then(|index| found.get(index))
                        .expect("a listed member"))
                },
                || Ok(first),
            )?;
            let live = found.iter().position(|probe| *probe == Probe::Live);
            let seen_dead = |member: libc::pid_t| {
                pids.iter()
                    .zip(found)
                    .any(|(pid, probe)| *pid == member && *probe == Probe::Dead)
            };
            assert_eq!(
                gone,
                live.is_none() && first.is_none_or(seen_dead),
                "{found:?} then {first:?}"
            );
            // A live member decides without probing past it.
            let expected = live.map_or(pids, |live| pids.get(..=live).unwrap_or_default());
            assert_eq!(probed, expected);
        }
    }
    Ok(())
}

/// A probe failure other than a missing process is not a decision.
#[test]
fn a_failed_probe_is_an_error() {
    let mut members = [1, 2];
    let error = decide_group_with(
        &mut members,
        |_| Err(io::Error::from_raw_os_error(libc::EPERM)),
        || Ok(None),
    )
    .expect_err("a probe failure is not a decision");
    assert_eq!(error.raw_os_error(), Some(libc::EPERM));
}
