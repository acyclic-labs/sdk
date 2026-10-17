//! Shrinking properties for portable filesystem path admission.
#[path = "path_input.rs"]
mod input;
use proptest::prelude::*;

#[test]
fn path_seed_corpus() {
    for bytes in [
        include_bytes!("../fuzz/corpus/paths/unicode").as_slice(),
        include_bytes!("../fuzz/corpus/paths/parent"),
        include_bytes!("../fuzz/corpus/paths/repeated-separator"),
        include_bytes!("../fuzz/corpus/paths/embedded-nul"),
        include_bytes!("../fuzz/corpus/paths/posix-bytes"),
        include_bytes!("../fuzz/corpus/paths/utf16-surrogate"),
        include_bytes!("../fuzz/corpus/paths/fold-expansions"),
    ] {
        input::check(bytes);
        path_oracle().check(&bytes.to_vec());
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn canonical_paths_and_fold_keys_preserve_namespace_boundaries(
        data in prop_oneof![
            prop::collection::vec(any::<u8>(), 0..2048),
            prop::collection::vec(any::<char>(), 0..48)
                .prop_map(|characters| characters.into_iter().collect::<String>().into_bytes()),
        ]
    ) {
        path_oracle().check(&data);
    }
}

#[derive(Debug)]
enum Fault {
    WrongPath,
    Unfolded,
    Lowercase,
    RejectedPosix,
    RejectedUtf16,
}

fn path_oracle() -> acyclic_test_support::differential::oracle::Oracle<Vec<u8>, Fault, input::Seen>
{
    use acyclic_fs::{
        VolumeLimits,
        kernel::{LogicalName, NameEncoding, TreePageError},
        path::PortablePath,
    };
    use acyclic_test_support::differential::oracle::Oracle;
    let mut builder = Oracle::declare(|data: &Vec<u8>, fault: Option<&Fault>, see| {
        let parse: input::Parse = if matches!(fault, Some(Fault::WrongPath)) {
            |_, limits: VolumeLimits| PortablePath::parse("/wrong", limits)
        } else {
            PortablePath::parse
        };
        let fold: input::Fold = match fault {
            Some(Fault::Unfolded) => |name| name.as_bytes().to_vec(),
            Some(Fault::Lowercase) => |name| {
                std::str::from_utf8(name.as_bytes())
                    .unwrap()
                    .to_lowercase()
                    .into_bytes()
            },
            _ => LogicalName::case_fold_key,
        };
        let new: input::New = match fault {
            Some(Fault::RejectedPosix) => |encoding, bytes, maximum| {
                if encoding == NameEncoding::PosixBytes {
                    Err(TreePageError::InvalidName)
                } else {
                    LogicalName::new(encoding, bytes, maximum)
                }
            },
            Some(Fault::RejectedUtf16) => |encoding, bytes, maximum| {
                if encoding == NameEncoding::WindowsUtf16Le {
                    Err(TreePageError::InvalidName)
                } else {
                    LogicalName::new(encoding, bytes, maximum)
                }
            },
            _ => LogicalName::new,
        };
        input::observe(data, parse, new, fold, &mut |event| see.see(event))
    });
    for (name, invariant) in input::INVARIANTS {
        builder = builder.invariant(name, invariant);
    }
    builder
        .control("portable", b"/original".to_vec(), Fault::WrongPath)
        .control("utf8-fold", b"a".to_vec(), Fault::Unfolded)
        .control("full-fold", vec![1], Fault::Lowercase)
        .control("posix", Vec::new(), Fault::RejectedPosix)
        .control("utf16", Vec::new(), Fault::RejectedUtf16)
        .build()
}

#[test]
fn every_path_invariant_detects_its_control_and_survives_deletion() {
    path_oracle().check(&Vec::new());
}
