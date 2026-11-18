//! Captured and generated process-host argument admission cases.

#![cfg(any(unix, windows))]

mod process_tree {
    mod external_host {
        mod tests {
            use acyclic_process_host_arguments::Invocation;
            use std::{ffi::OsString, io, path::Path};

            proptest::proptest! {
                #[test]
                fn process_host_argument_fuzz(
                    recorded in proptest::sample::select(
                        serde_json::from_str::<Vec<serde_json::Value>>(include_str!(
                            "corpus/linux-arguments.json")).unwrap()),
                    arguments in proptest::collection::vec(
                        proptest::collection::vec(proptest::prelude::any::<u8>(), 0..64), 0..32),
                    inherit_input in proptest::prelude::any::<bool>(),
                    literal in proptest::prelude::any::<bool>(),
                    custody in 0_u8..3,
                    control in proptest::prelude::any::<bool>(),
                    receipt in proptest::prelude::any::<bool>(),
                    bound in proptest::prelude::any::<bool>(),
                    order in proptest::prelude::any::<[u8; 5]>(),
                    binding in "[a-zA-Z0-9_-]{1,32}",
                ) {
                    // Conformance to the original executable's portable argv, not
                    // another parser model or rewritten installation paths.
                    let original = recorded.get("arguments").unwrap().as_array().unwrap().iter()
                        .map(|value| OsString::from(value.as_str().unwrap()));
                    proptest::prop_assert_eq!(Invocation::parse(original).is_ok(),
                        recorded.get("code").unwrap() == 17);
                    let path = std::env::current_exe().unwrap().into_os_string();
                    let executable = if literal { "relative-program".into() } else { path.clone() };
                    let mut tail: Vec<OsString> = arguments.into_iter().map(|bytes| {
                        #[cfg(unix)] {
                            use std::os::unix::ffi::OsStringExt as _;
                            OsString::from_vec(bytes)
                        }
                        #[cfg(windows)] {
                            use std::os::windows::ffi::OsStringExt as _;
                            let wide: Vec<u16> = bytes.chunks(2)
                                .map(|pair| u16::from_le_bytes([pair[0], *pair.get(1).unwrap_or(&0)]))
                                .collect();
                            OsString::from_wide(&wide)
                        }
                    }).collect();
                    tail.splice(0..0, ["--lease".into(), "--invocation".into(), "--".into()]);
                    let mut options: Vec<Vec<OsString>> = vec![
                        if receipt { vec!["--completion".into(), path.clone()] } else { vec![] },
                        if control { vec!["--control".into(), path.clone()] } else { vec![] },
                        match custody {
                            1 => vec!["--custody".into(), path.clone()],
                            2 => vec!["--lease".into(), path.clone()],
                            _ => vec![],
                        },
                        if receipt && bound { vec!["--invocation".into(), binding.clone().into()] } else { vec![] },
                        if inherit_input { vec!["--inherit-input".into()] } else { vec![] },
                    ];
                    let mut positions = [0, 1, 2, 3, 4];
                    positions.sort_by_key(|index| *order.get(*index).unwrap());
                    let mut command: Vec<OsString> = positions.into_iter()
                        .flat_map(|index| std::mem::take(options.get_mut(index).unwrap())).collect();
                    if literal { command.push("--".into()); }
                    command.push(executable.clone());
                    command.extend(tail.clone());
                    let parsed = Invocation::parse(command).unwrap();
                    proptest::prop_assert_eq!(parsed.executable, executable);
                    proptest::prop_assert_eq!(parsed.arguments, tail);
                    proptest::prop_assert_eq!(parsed.inherit_input, inherit_input);
                    proptest::prop_assert_eq!(parsed.control.as_deref(), control.then(|| Path::new(&path)));
                    proptest::prop_assert_eq!(parsed.completion.as_deref(), receipt.then(|| Path::new(&path)));
                    proptest::prop_assert_eq!(parsed.custody.as_deref(), (custody == 1).then(|| Path::new(&path)));
                    proptest::prop_assert_eq!(parsed.lease.as_deref(), (custody == 2).then(|| Path::new(&path)));
                    proptest::prop_assert_eq!(parsed.invocation.as_deref(), (receipt && bound).then_some(binding.as_str()));
                    // The same admission predicate rejects missing, relative and
                    // duplicate paths for every endpoint, not only completion files.
                    for option in ["--completion", "--control", "--custody", "--lease"] {
                        for refused in [vec![option.into()],
                            vec![option.into(), "relative".into(), path.clone()],
                            vec![option.into(), path.clone(), option.into(), path.clone(), path.clone()]] {
                            proptest::prop_assert_eq!(Invocation::parse(refused).unwrap_err().kind(),
                                io::ErrorKind::InvalidInput);
                        }
                    }
                    for invalid in ["", " ", ".", "/", "\n", "π"] {
                        proptest::prop_assert_eq!(Invocation::parse([
                            "--completion".into(), path.clone(), "--invocation".into(),
                            invalid.into(), path.clone()]).unwrap_err().kind(), io::ErrorKind::InvalidInput);
                    }
                    #[cfg(unix)]
                    let invalid_unicode = {
                        use std::os::unix::ffi::OsStringExt as _;
                        OsString::from_vec(vec![0xff])
                    };
                    #[cfg(windows)]
                    let invalid_unicode = {
                        use std::os::windows::ffi::OsStringExt as _;
                        OsString::from_wide(&[0xd800])
                    };
                    for refused in [vec![], vec!["--".into(), "".into()],
                        vec!["--unknown".into(), path.clone()],
                        vec!["--invocation".into()],
                        vec!["--completion".into(), path.clone(), "--invocation".into(), invalid_unicode, path.clone()],
                        vec!["--completion".into(), path.clone(), "--invocation".into(), binding.clone().into(),
                            "--invocation".into(), binding.clone().into(), path.clone()],
                        vec!["--invocation".into(), binding.into(), path.clone()],
                        vec!["--inherit-input".into(), "--inherit-input".into(), path.clone()],
                        vec!["--custody".into(), path.clone(), "--lease".into(), path.clone(), path]] {
                        proptest::prop_assert_eq!(Invocation::parse(refused).unwrap_err().kind(),
                            io::ErrorKind::InvalidInput);
                    }
                }
            }
        }
    }
}
