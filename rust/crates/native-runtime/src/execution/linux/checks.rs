//! Interpret the actual generated filter, including its jumps. These checks
//! complement the real-process cases rather than replacing kernel enforcement.
use super::*;
use proptest::prelude::*;

#[cfg(target_arch = "x86_64")]
const ARCH: u32 = 0xc000_003e;
#[cfg(target_arch = "aarch64")]
const ARCH: u32 = 0xc000_00b7;

fn evaluate(code: &[libc::sock_filter], arch: u32, syscall: u32, arguments: [u64; 6]) -> u32 {
    let mut accumulator = 0;
    let mut at = 0;
    while let Some(instruction) = code.get(at) {
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
            EQUAL | 0x35 => {
                let yes = if instruction.code == EQUAL {
                    accumulator == instruction.k
                } else {
                    accumulator >= instruction.k
                };
                at += usize::from(if yes { instruction.jt } else { instruction.jf });
            }
            AND => accumulator &= instruction.k,
            RETURN => return instruction.k,
            opcode => panic!("unchecked filter instruction {opcode}"),
        }
        at += 1;
    }
    panic!("filter fell through without a verdict");
}

proptest! {
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
            prop_assert_eq!(evaluate(&code, architecture, syscall, arguments), 0x8000_0000);
        }
        prop_assert_eq!(evaluate(&code, ARCH, syscall | 0x4000_0000, arguments), DENY);
        for syscall in [libc::SYS_ptrace, libc::SYS_setns, libc::SYS_mount, libc::SYS_socketpair] {
            prop_assert_eq!(evaluate(&code, ARCH, syscall as u32, arguments), DENY);
        }
        for flag in [libc::CLONE_NEWUSER, libc::CLONE_NEWNET, libc::CLONE_NEWPID, libc::CLONE_PARENT] {
            let mut clone = arguments;
            clone[0] |= flag as u64;
            prop_assert_eq!(evaluate(&code, ARCH, libc::SYS_clone as u32, clone), DENY);
        }
        prop_assert_eq!(evaluate(&code, ARCH, libc::SYS_clone3 as u32, arguments),
            0x0005_0000 | libc::ENOSYS as u32);
        let mut socket = arguments;
        socket[0] = libc::AF_UNIX as u64;
        prop_assert_eq!(evaluate(&code, ARCH, libc::SYS_socket as u32, socket), DENY);
    }

    #[test]
    fn generated_filter_preserves_normal_threads_and_tcp_without_opening_other_families(
        family in any::<u32>(),
        socket_flags in prop::sample::select(vec![0, libc::SOCK_CLOEXEC, libc::SOCK_NONBLOCK,
            libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK]),
    ) {
        for network in [ExecutionNetwork::On,
            ExecutionNetwork::TcpPorts(std::collections::BTreeSet::from([443]))] {
            let code = filter(&network).unwrap();
            let mut socket = [0; 6];
            socket[0] = u64::from(family);
            socket[1] = (libc::SOCK_STREAM | socket_flags) as u64;
            socket[2] = libc::IPPROTO_TCP as u64;
            prop_assert_eq!(evaluate(&code, ARCH, libc::SYS_socket as u32, socket),
                if family == libc::AF_INET as u32 || family == libc::AF_INET6 as u32 { ALLOW } else { DENY });
            let mut clone = [0; 6];
            clone[0] = (libc::CLONE_VM | libc::CLONE_FS | libc::CLONE_FILES
                | libc::CLONE_SIGHAND | libc::CLONE_THREAD | libc::CLONE_SYSVSEM) as u64;
            prop_assert_eq!(evaluate(&code, ARCH, libc::SYS_clone as u32, clone), ALLOW);
        }
        let off = filter(&ExecutionNetwork::Off).unwrap();
        for syscall in [libc::SYS_socket, libc::SYS_connect, libc::SYS_socketpair] {
            prop_assert_eq!(evaluate(&off, ARCH, syscall as u32, [0; 6]), DENY);
        }
    }
}
