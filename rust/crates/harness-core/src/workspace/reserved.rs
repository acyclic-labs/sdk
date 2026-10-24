//! The one policy for root names a host checkout reserves.
//!
//! Every demand, capture and publication path takes its exclusions from
//! [`RESERVED_NAMES`], derived from the single [`RESERVED`] table, so adding a
//! name updates every path at once.
//!
//! Every reserved name is hidden from source demand, never read by capture
//! and preserved on the host by publication, so no path can carry host bytes
//! that another path refuses to refresh. Host tools such as git read these
//! names on the host itself, never through a checkout head.
//! Volume projections are excluded individually so authored `.volumes` siblings remain content.

/// One reserved root name.
struct Reserved {
    name: &'static str,
    /// Subtrees the SDK itself writes into a checkout head under this name.
    /// Everything else a head holds under a reserved name was imported from
    /// the host and is removed when a binding is reconciled.
    owned: &'static [&'static str],
}

// The order is the watcher's fence placement preference: the first existing
// directory hosts fence cookies, and a root with none gains the first name.
const RESERVED: [Reserved; 3] = [
    // Host-side SDK staging, such as watcher fence cookies.
    Reserved {
        name: ".acyclic-sdk",
        owned: &[],
    },
    // The host's own repository. Bash runs host git against it in place.
    Reserved {
        name: ".git",
        owned: &[],
    },
    // SDK state, never checkout content.
    Reserved {
        name: ".system",
        owned: &[
            "execution",
            "harness-file-metadata",
            "harness-uploads",
            "inherited-conversation",
            "interactions",
            "workflows",
        ],
    },
];

const fn names() -> [&'static str; RESERVED.len()] {
    let mut names = [""; RESERVED.len()];
    let mut index = 0;
    while index < RESERVED.len() {
        names[index] = RESERVED[index].name;
        index += 1;
    }
    names
}

/// Every reserved root name: hidden from source demand, never read by
/// capture, and preserved on the host by publication.
pub const RESERVED_NAMES: [&str; RESERVED.len()] = names();

/// Whether the SDK owns `relative` in a checkout head: it lies within, or is
/// an ancestor of, an owned subtree of a reserved name. Containers must be
/// directories. A name without owned subtrees holds no SDK state.
#[cfg(feature = "native-execution")]
pub(crate) fn sdk_owned(relative: &str, kind: acyclic_fs::kernel::FileKind) -> bool {
    let directory = kind == acyclic_fs::kernel::FileKind::Directory;
    let (name, rest) = relative.split_once('/').unwrap_or((relative, ""));
    let Some(reserved) = RESERVED.iter().find(|reserved| reserved.name == name) else {
        return false;
    };
    (directory && rest.is_empty() && !reserved.owned.is_empty())
        || reserved.owned.iter().any(|owned| {
            (directory && rest == *owned)
                || rest
                    .strip_prefix(owned)
                    .is_some_and(|tail| tail.starts_with('/'))
        })
}

#[cfg(all(test, feature = "native-execution"))]
mod tests {
    use super::*;

    #[test]
    fn only_owned_subtrees_and_their_ancestors_are_sdk_owned() {
        use acyclic_fs::kernel::FileKind::*;
        for (owned, kind) in [
            (".system", Directory),
            (".system/execution", Directory),
            (".system/execution/operation/digest.json", Regular),
            (".system/inherited-conversation/prefix.json", Regular),
        ] {
            assert!(sdk_owned(owned, kind), "{owned}");
        }
        for imported in [
            ".volumes",
            ".volumes/scratch/file",
            ".acyclic-sdk/fence",
            ".system/existing",
            ".system/executions/file",
            ".git",
            ".git/config",
            "docs/.system/execution",
            ".system/reserved-names",
            ".system/reserved-names/child",
        ] {
            assert!(!sdk_owned(imported, Regular), "{imported}");
            assert!(!sdk_owned(imported, Directory), "{imported}");
        }
        let container_shapes_match = |ignore_kind| {
            [
                Regular,
                Directory,
                SymbolicLink,
                Fifo,
                Socket,
                CharacterDevice,
                BlockDevice,
                ReparsePoint,
                MountBoundary,
            ]
            .into_iter()
            .all(|kind| {
                [
                    ".system",
                    ".system/execution",
                    ".system/harness-file-metadata",
                    ".system/harness-uploads",
                    ".system/inherited-conversation",
                    ".system/interactions",
                    ".system/workflows",
                ]
                .into_iter()
                .all(|container| {
                    sdk_owned(container, if ignore_kind { Directory } else { kind })
                        == (kind == Directory)
                })
            })
        };
        assert!(container_shapes_match(false));
        // Negative control: the old ownership decision discarded entry kind.
        assert!(!container_shapes_match(true));
    }
}
