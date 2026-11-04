//! Native object controls and interpretation of the actual generated filter.
//! These complement real-process cases without replacing kernel enforcement.
use super::*;
use proptest::prelude::*;

#[test]
fn rooted_directory_buffer_uses_the_original_allocation_and_operation_credits() {
    use std::sync::atomic::Ordering;
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("nested")).unwrap();
    let parent = File::open(root.path()).unwrap();
    let denied = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
    denied.deny_allocations.store(true, Ordering::Release);
    let error = open_beneath_with_flags(
        &parent,
        Path::new("nested"),
        libc::O_DIRECTORY,
        Some(&denied),
    )
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert_eq!(denied.used_operations.load(Ordering::Acquire), 0);
    assert_eq!(denied.read_attempts.load(Ordering::Acquire), 0);
    assert_eq!(denied.used_allocations.load(Ordering::Acquire), 0);
    assert_eq!(denied.memory.load(Ordering::Acquire), 0);

    // The identical rooted native open without the allocation ceiling is the
    // negative control. This exercises real kernel resolution, not a mock.
    match open_beneath(&parent, Path::new("nested"), None) {
        Ok(file) => assert!(file.metadata().unwrap().is_dir()),
        Err(error) if error.kind() == io::ErrorKind::Unsupported => return,
        Err(error) => panic!("rooted directory negative control failed: {error}"),
    }
    for (limit, failure, name, succeeds) in [
        (0, None, "nested", false),
        (1, Some(1), "nested", false),
        (1, None, "missing", false),
        (1, None, "nested", true),
    ] {
        let mut meter = crate::tests::ReadBudgetMeter::new(limit, u64::MAX);
        meter.fail_recorded_operation = failure;
        let output =
            open_beneath_with_flags(&parent, Path::new(name), libc::O_DIRECTORY, Some(&meter));
        assert_eq!(output.is_ok(), succeeds, "{output:?}");
        if let Ok(file) = output {
            assert!(file.metadata().unwrap().is_dir());
        }
        let attempted = u64::from(limit != 0);
        assert_eq!(meter.read_attempts.load(Ordering::Acquire), attempted);
        assert_eq!(meter.used_allocations.load(Ordering::Acquire), attempted);
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    }
}

#[test]
fn computation_devices_bind_objects_and_refuse_name_substitution() {
    let root = tempfile::tempdir().unwrap();
    let alias = root.path().join("device");
    let ordinary = root.path().join("ordinary");
    std::fs::write(&ordinary, "outside authority").unwrap();
    for (path, expected, wrong_device) in [
        ("/dev/null", entry::NULL_DEVICE, "/dev/urandom"),
        ("/dev/urandom", URANDOM_DEVICE, "/dev/null"),
    ] {
        std::os::unix::fs::symlink(path, &alias).unwrap();
        let held = computation_device(&alias, expected, None).unwrap();
        for replacement in [Path::new(wrong_device), &ordinary, root.path()] {
            std::fs::remove_file(&alias).unwrap();
            std::os::unix::fs::symlink(replacement, &alias).unwrap();
            // The original name-only opening accepts every replacement,
            // including a directory that would grant a whole hierarchy.
            let unvalidated = BoundPath::open_path(&alias).unwrap();
            let metadata = unvalidated.metadata().unwrap();
            assert!(!entry::character_device(
                metadata.mode(),
                metadata.rdev(),
                expected
            ));
            let error = computation_device(&alias, expected, None).unwrap_err();
            assert!(
                error
                    .get_ref()
                    .unwrap()
                    .is::<super::super::ExecutionRefusal>()
            );
            // Replacing the name after admission cannot replace its object.
            let metadata = held.metadata().unwrap();
            assert!(entry::character_device(
                metadata.mode(),
                metadata.rdev(),
                expected
            ));
        }
        std::fs::remove_file(&alias).unwrap();
    }
    assert_eq!(
        std::fs::read_to_string(ordinary).unwrap(),
        "outside authority"
    );
}

#[cfg(target_arch = "x86_64")]
const ARCH: u32 = 0xc000_003e;
#[cfg(target_arch = "aarch64")]
const ARCH: u32 = 0xc000_00b7;

fn evaluate(code: &[Instruction], arch: u32, syscall: u32, arguments: [u64; 6]) -> u32 {
    evaluate_work(code, arch, syscall, arguments).0
}

fn evaluate_work(
    code: &[Instruction],
    arch: u32,
    syscall: u32,
    arguments: [u64; 6],
) -> (u32, usize) {
    let mut accumulator = 0;
    let mut at = 0;
    let mut steps = 0;
    while let Some(instruction) = code.get(at) {
        steps += 1;
        match instruction.code {
            LOAD => {
                accumulator = match instruction.k {
                    0 => syscall,
                    4 => arch,
                    offset @ 16..64 if offset % 4 == 0 => {
                        let offset = (offset - 16) as usize;
                        (arguments[offset / 8] >> ((offset % 8) * 8)) as u32
                    }
                    offset => panic!("invalid seccomp data load at {offset}"),
                };
            }
            EQUAL | GREATER_EQUAL | GREATER => {
                let yes = if instruction.code == EQUAL {
                    accumulator == instruction.k
                } else if instruction.code == GREATER {
                    accumulator > instruction.k
                } else {
                    accumulator >= instruction.k
                };
                at += usize::from(if yes { instruction.jt } else { instruction.jf });
            }
            JUMP => at += instruction.k as usize,
            AND => accumulator &= instruction.k,
            RETURN => return (instruction.k, steps),
            opcode => panic!("unchecked filter instruction {opcode}"),
        }
        at += 1;
    }
    panic!("filter fell through without a verdict");
}

#[test]
fn syscall_dispatch_preserves_classification_with_bounded_work() {
    const fn dispatch<const N: usize>() -> Filter<N> {
        let mut code = Filter::new();
        code.push(instruction(LOAD, 0, 0, 0));
        const RANGES: SyscallRanges = syscall_ranges();
        syscall_dispatch(&mut code, RANGES.ranges.split_at(RANGES.len).0);
        code
    }
    const LEN: usize = dispatch::<0>().len;
    const CODE: [Instruction; LEN] = dispatch::<LEN>().instructions;
    let ranges = syscall_ranges();
    let bound = 1 + 2 * ranges.len.next_power_of_two().ilog2() as usize + 4;
    let last = ranges.ranges[ranges.len - 1].last;
    let mut maximum = 0;
    // Exhaust every current number and gap, both outer boundaries, and the
    // largest future number. The unchanged declaration is the independent
    // classification oracle; range generation cannot widen it unnoticed.
    for call in (0..=last + 1).chain([u32::MAX]) {
        let (result, steps) = evaluate_work(&CODE, ARCH, call, [0; 6]);
        assert_eq!(
            result,
            if ALLOWED_SYSCALLS.contains(&libc::c_long::from(call)) {
                ALLOW
            } else {
                DENY
            },
            "syscall {call}"
        );
        assert!(steps <= bound, "syscall {call}: {steps} > {bound}");
        maximum = maximum.max(steps);
    }
    // The former linear dispatcher is a cost negative control, using the
    // original declaration and encoding rather than a fabricated receipt.
    let mut linear = Filter::<{ 2 * ALLOWED_SYSCALLS.len() + 2 }>::new();
    linear.push(instruction(LOAD, 0, 0, 0));
    for call in ALLOWED_SYSCALLS {
        verdict(&mut linear, *call, ALLOW);
    }
    linear.push(instruction(RETURN, 0, 0, DENY));
    assert!(evaluate_work(&linear.instructions, ARCH, u32::MAX, [0; 6]).1 > bound);
    assert!(CODE.len() < linear.len);
    eprintln!(
        "syscall dispatch: instructions {} -> {}, maximum steps {} -> {maximum}, bound={bound}",
        linear.len,
        CODE.len(),
        ALLOWED_SYSCALLS.len() + 2
    );
}

proptest! {
    #[test]
    fn unclassified_syscalls_and_external_process_controls_are_closed(
        syscall in any::<u32>(),
        arguments in any::<[u64; 6]>(),
    ) {
        let code = filter(&ExecutionNetwork::On).unwrap();
        if !ALLOWED_SYSCALLS.iter().any(|call| *call as u32 == syscall)
            && ![libc::SYS_read, libc::SYS_write, libc::SYS_fcntl,
                libc::SYS_prlimit64, libc::SYS_getpgid, libc::SYS_getsid,
                libc::SYS_clone3].contains(&(i64::from(syscall))) {
            prop_assert_eq!(evaluate(code, ARCH, syscall, arguments), DENY);
        }
        for call in [libc::SYS_shmget, libc::SYS_shmat, libc::SYS_shmctl,
            libc::SYS_msgget, libc::SYS_msgsnd, libc::SYS_msgrcv, libc::SYS_msgctl,
            libc::SYS_semget, libc::SYS_semop, libc::SYS_semctl,
            libc::SYS_mq_open, libc::SYS_mq_unlink, libc::SYS_process_vm_writev,
            libc::SYS_process_madvise, libc::SYS_process_mrelease,
            libc::SYS_setpgid, libc::SYS_setsid] {
            prop_assert_eq!(evaluate(code, ARCH, call as u32, arguments), DENY);
        }
        for call in [libc::SYS_prlimit64, libc::SYS_getpgid, libc::SYS_getsid] {
            for pid in [arguments[0], 0, 1, u64::MAX] {
                let mut selected = arguments;
                selected[0] = pid;
                prop_assert_eq!(evaluate(code, ARCH, call as u32, selected),
                    if pid as u32 == 0 { ALLOW } else { DENY });
            }
        }
        for command in [libc::F_SETOWN, libc::F_GETOWN, libc::F_SETLEASE] {
            let mut control = arguments;
            control[1] = command as u64;
            prop_assert_eq!(evaluate(code, ARCH, libc::SYS_fcntl as u32, control), DENY);
        }
        for command in [libc::F_GETFD, libc::F_SETFD, libc::F_GETFL, libc::F_SETFL,
            libc::F_DUPFD_CLOEXEC] {
            let mut control = arguments;
            control[1] = command as u64;
            prop_assert_eq!(evaluate(code, ARCH, libc::SYS_fcntl as u32, control), ALLOW);
        }
        // Restore a permissive default in the actual generated program.
        // The same host IPC operation is then admitted without any path or FD.
        let mut permissive = code.to_vec();
        for instruction in &mut permissive {
            if instruction.code == RETURN && instruction.k == DENY {
                instruction.k = ALLOW;
            }
        }
        prop_assert_eq!(evaluate(&permissive, ARCH, libc::SYS_shmat as u32, arguments), ALLOW);
    }

    #[test]
    fn port_grants_refuse_implicit_fast_open_connections(
        flags in any::<u32>(),
        level in any::<u32>(),
        option in any::<u32>(),
    ) {
        let code = filter(&ExecutionNetwork::TcpPorts(
            std::collections::BTreeSet::from([443]))).unwrap();
        for (call, argument) in [(libc::SYS_sendto, 3), (libc::SYS_sendmsg, 2),
            (libc::SYS_sendmmsg, 3)] {
            let mut arguments = [0; 6];
            arguments[argument] = u64::from(flags);
            prop_assert_eq!(evaluate(code, ARCH, call as u32, arguments),
                if flags & libc::MSG_FASTOPEN as u32 == 0 { ALLOW } else { DENY });
        }
        let mut arguments = [0; 6];
        arguments[1] = u64::from(level) | (u64::from(flags) << 32);
        arguments[2] = u64::from(option) | (u64::from(flags.rotate_left(16)) << 32);
        prop_assert_eq!(evaluate(code, ARCH, libc::SYS_setsockopt as u32, arguments),
            if level == libc::IPPROTO_TCP as u32 && option == libc::TCP_FASTOPEN_CONNECT as u32 {
                DENY
            } else {
                ALLOW
            });
        arguments[1] = libc::IPPROTO_TCP as u64;
        arguments[2] = libc::TCP_FASTOPEN_CONNECT as u64;
        prop_assert_eq!(evaluate(code, ARCH, libc::SYS_setsockopt as u32, arguments), DENY);
        // The same real filter generator retains this option without a port
        // ceiling; a blanket setsockopt refusal would also reject NODELAY.
        let unrestricted = filter(&ExecutionNetwork::On).unwrap();
        prop_assert_eq!(evaluate(unrestricted, ARCH, libc::SYS_setsockopt as u32, arguments), ALLOW);
        arguments[2] = libc::TCP_NODELAY as u64;
        prop_assert_eq!(evaluate(code, ARCH, libc::SYS_setsockopt as u32, arguments), ALLOW);
    }

    // Generated programs must not admit a second syscall ABI, x32 numbers,
    // namespace-bearing clone or an IPC socket through an incorrect jump.
    #[test]
    fn generated_filter_seals_abi_and_escape_branches(
        architecture in any::<u32>(),
        syscall in any::<u32>(),
        arguments in any::<[u64; 6]>(),
        mode in 0..3u8,
    ) {
        let network = match mode {
            0 => ExecutionNetwork::Off,
            1 => ExecutionNetwork::On,
            _ => ExecutionNetwork::TcpPorts(std::collections::BTreeSet::from([443])),
        };
        let code = filter(&network).unwrap();
        if architecture != ARCH {
            prop_assert_eq!(evaluate(code, architecture, syscall, arguments), 0x8000_0000);
        }
        prop_assert_eq!(evaluate(code, ARCH, syscall | 0x4000_0000, arguments), DENY);
        for syscall in [libc::SYS_ptrace, libc::SYS_setns, libc::SYS_mount, libc::SYS_socketpair] {
            prop_assert_eq!(evaluate(code, ARCH, syscall as u32, arguments), DENY);
        }
        for flag in [libc::CLONE_NEWUSER, libc::CLONE_NEWNET, libc::CLONE_NEWPID, libc::CLONE_PARENT] {
            let mut clone = arguments;
            clone[0] |= flag as u64;
            prop_assert_eq!(evaluate(code, ARCH, libc::SYS_clone as u32, clone), DENY);
        }
        prop_assert_eq!(evaluate(code, ARCH, libc::SYS_clone3 as u32, arguments),
            0x0005_0000 | libc::ENOSYS as u32);
        let mut socket = arguments;
        socket[0] = libc::AF_UNIX as u64;
        prop_assert_eq!(evaluate(code, ARCH, libc::SYS_socket as u32, socket), DENY);
    }

    #[test]
    fn generated_filter_preserves_normal_threads_and_tcp_without_opening_other_families(
        family in any::<u32>(),
        socket_flags in prop::sample::select(vec![0, libc::SOCK_CLOEXEC, libc::SOCK_NONBLOCK,
            libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK]),
    ) {
        for network in [ExecutionNetwork::Off, ExecutionNetwork::On,
            ExecutionNetwork::TcpPorts(std::collections::BTreeSet::new()),
            ExecutionNetwork::TcpPorts(std::collections::BTreeSet::from([443]))] {
            let mut code: &[Instruction] = &[];
            let selection = acyclic_test_support::measure(|| {
                code = std::hint::black_box(filter(std::hint::black_box(&network)).unwrap());
            });
            prop_assert_eq!(selection.count_total, 0);
            // A per-grant instruction copy is the allocating negative control.
            let copying = acyclic_test_support::measure(|| {
                drop(std::hint::black_box(code.to_vec()));
            });
            prop_assert!(copying.count_total > 0);
            let enabled = matches!(network, ExecutionNetwork::On)
                || matches!(&network, ExecutionNetwork::TcpPorts(ports) if !ports.is_empty());
            let mut socket = [0; 6];
            socket[1] = (libc::SOCK_STREAM | socket_flags) as u64;
            socket[2] = libc::IPPROTO_TCP as u64;
            for family in [family, libc::AF_INET as u32, libc::AF_INET6 as u32, libc::AF_UNIX as u32] {
                socket[0] = u64::from(family);
                prop_assert_eq!(evaluate(code, ARCH, libc::SYS_socket as u32, socket),
                    if enabled && (family == libc::AF_INET as u32 || family == libc::AF_INET6 as u32) { ALLOW } else { DENY });
            }
            let mut clone = [0; 6];
            clone[0] = (libc::CLONE_VM | libc::CLONE_FS | libc::CLONE_FILES
                | libc::CLONE_SIGHAND | libc::CLONE_THREAD | libc::CLONE_SYSVSEM) as u64;
            prop_assert_eq!(evaluate(code, ARCH, libc::SYS_clone as u32, clone), ALLOW);
            if !enabled {
                for syscall in [libc::SYS_socket, libc::SYS_connect, libc::SYS_socketpair] {
                    prop_assert_eq!(evaluate(code, ARCH, syscall as u32, [0; 6]), DENY);
                }
            }
        }
    }
}
