use super::*;

macro_rules! git_commands {
    ($(#[$meta:meta])* pub enum $name:ident {
        $($(#[$doc:meta])* $variant:ident $({ $($field:tt)* })? => ($kind:literal, $typescript:literal)),* $(,)?
    }) => {
        $(#[$meta])*
        pub enum $name {
            $($(#[$doc])* $variant $({ $($field)* })?),*
        }
        /// Canonical command tags.
        pub const GIT_COMPAT_COMMAND_VARIANTS: &[&str] = &[$(stringify!($variant)),*];
        /// Public command spellings and their canonical tags.
        pub const GIT_COMPAT_PUBLIC_COMMAND_KINDS: &[(&str, &str)] = &[$(($kind, stringify!($variant))),*];
        /// Public TypeScript command declarations.
        pub const GIT_COMPAT_COMMAND_TYPESCRIPT_TYPES: &[(&str, &str)] = &[$((stringify!($variant), $typescript)),*];
        impl $name {
            /// Returns the canonical command tag.
            pub const fn variant_name(&self) -> &'static str {
                match self {
                    $(git_commands!(@pattern Self, $variant $({ $($field)* })?) => stringify!($variant)),*
                }
            }
        }
    };
    (@pattern $name:ident, $variant:ident { $($field:tt)* }) => { $name::$variant { .. } };
    (@pattern $name:ident, $variant:ident) => { $name::$variant };
}

git_commands! {
/// Parsed Git-shaped command.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum GitCommand {
/// Compare compatibility HEAD with the live workspace.
    Status => ("status", r#"{ readonly kind: "status" }"#),
/// Diff HEAD-to-workspace; cached has identical semantics.
    Diff {
        /// Whether the caller used `--cached`; behavior is identical without an index.
        cached: bool,
    } => ("diff", r#"{ readonly kind: "diff"; readonly cached?: boolean }"#),
/// Walk explicit compatibility history.
    Log {
        /// Maximum first-parent commits to return.
        maximum: u32,
    } => ("log", r#"{ readonly kind: "log"; readonly maximum: number }"#),
/// Inspect one compatibility commit.
    Show {
        /// Object to show, defaulting to `HEAD`.
        object: Option<GitObjectName>,
    } => ("show", r#"{ readonly kind: "show"; readonly object?: string }"#),
/// Accepted no-op because changes are always staged.
    Add {
        /// Accepted pathspecs; staging remains automatic.
        paths: Vec<String>,
    } => ("add", r#"{ readonly kind: "add"; readonly paths: readonly string[] }"#),
/// Capture the complete eligible workspace state.
    Commit {
        /// Commit message.
        message: String,
        /// Author identity.
        author: String,
        /// Signed Unix epoch time.
        authored_at_seconds: i64,
    } => ("commit", r#"{ readonly kind: "commit"; readonly message: string; readonly author: string; readonly authoredAtSeconds: bigint }"#),
/// List or create a branch.
    Branch {
        /// Optional branch to create before listing.
        create: Option<String>,
    } => ("branch", r#"{ readonly kind: "branch"; readonly create?: string }"#),
/// Select a branch, optionally creating it first.
    Switch {
        /// Destination branch.
        branch: String,
        /// Whether to create a missing destination.
        create: bool,
    } => ("switch", r#"{ readonly kind: "switch"; readonly branch: string; readonly create?: boolean }"#),
/// Restore paths from an object.
    Restore {
        /// Source object, defaulting to `HEAD`.
        source: Option<GitObjectName>,
        /// Portable paths to restore.
        paths: Vec<String>,
    } => ("restore", r#"{ readonly kind: "restore"; readonly source?: string; readonly paths: readonly string[] }"#),
/// Move HEAD and optionally the workspace.
    Reset {
        /// Commit-like target.
        target: GitObjectName,
        /// Workspace restoration policy.
        mode: GitResetMode,
    } => ("reset", r#"{ readonly kind: "reset"; readonly target: string; readonly mode: GitResetMode }"#),
/// Join another compatibility branch.
    Merge {
        /// Source branch.
        branch: String,
    } => ("merge", r#"{ readonly kind: "merge"; readonly branch: string }"#),
/// Continue the one durable conflicted merge after ordinary workspace edits.
    MergeContinue => ("merge-continue", r#"{ readonly kind: "merge-continue" }"#),
/// Restore the pre-merge working tree and clear the durable merge intent.
    MergeAbort => ("merge-abort", r#"{ readonly kind: "merge-abort" }"#),
/// Rebase onto another compatibility branch.
    Rebase {
        /// New base branch.
        branch: String,
    } => ("rebase", r#"{ readonly kind: "rebase"; readonly branch: string }"#),
/// Stash the complete working generation.
    StashPush => ("stash-push", r#"{ readonly kind: "stash-push" }"#),
/// Restore and remove the newest stash.
    StashPop => ("stash-pop", r#"{ readonly kind: "stash-pop" }"#),
/// Apply one commit as a cherry-pick.
    CherryPick {
        /// Commit to apply.
        object: GitObjectName,
    } => ("cherry-pick", r#"{ readonly kind: "cherry-pick"; readonly object: string }"#),
/// Invert one explicit commit.
    Revert {
        /// Commit to invert.
        object: GitObjectName,
    } => ("revert", r#"{ readonly kind: "revert"; readonly object: string }"#),
/// List, create, or delete a lightweight tag.
    Tag {
        /// Optional tag name; omission lists tags.
        name: Option<String>,
        /// Target object, defaulting to `HEAD`.
        target: Option<GitObjectName>,
        /// Whether to delete instead of create.
        delete: bool,
    } => ("tag", r#"{ readonly kind: "tag"; readonly name?: string; readonly target?: string; readonly delete?: boolean }"#),
/// Attribute lines to compatibility commits.
    Blame {
        /// Portable path to attribute.
        path: String,
    } => ("blame", r#"{ readonly kind: "blame"; readonly path: string }"#),
/// Search one live or historical tree.
    Grep {
        /// Search pattern interpreted by the executor.
        pattern: String,
        /// Optional subtree path.
        path: Option<String>,
    } => ("grep", r#"{ readonly kind: "grep"; readonly pattern: string; readonly path?: string }"#),
/// Remove untracked, non-ignored paths.
    Clean {
        /// Whether to report candidates without mutation.
        dry_run: bool,
    } => ("clean", r#"{ readonly kind: "clean"; readonly dryRun: boolean }"#),
/// Export one compatibility tree.
    Archive {
        /// Object to archive, defaulting to the workspace.
        object: Option<GitObjectName>,
    } => ("archive", r#"{ readonly kind: "archive"; readonly object?: string }"#),
/// Apply a patch to the working workspace.
    Apply {
        /// Opaque patch bytes.
        patch: Vec<u8>,
    } => ("apply", r#"{ readonly kind: "apply"; readonly patch: Uint8Array }"#),
/// Advance a caller-directed bisect session.
    Bisect {
        /// Bisect subcommand and arguments.
        arguments: Vec<String>,
    } => ("bisect", r#"{ readonly kind: "bisect"; readonly arguments: readonly string[] }"#),
/// Resolve one compatibility revision or a small read-only probe.
    RevParse {
        /// Probe or object name accepted by the compatibility subset.
        argument: String,
    } => ("rev-parse", r#"{ readonly kind: "rev-parse"; readonly argument: string }"#),
/// Print the symbolic compatibility HEAD.
    SymbolicRef {
        /// Whether to omit the `refs/heads/` prefix.
        short: bool,
    } => ("symbolic-ref", r#"{ readonly kind: "symbolic-ref"; readonly short?: boolean }"#),
/// Find the nearest common explicit compatibility commit.
    MergeBase {
        /// First commit-like object.
        left: GitObjectName,
        /// Second commit-like object.
        right: GitObjectName,
    } => ("merge-base", r#"{ readonly kind: "merge-base"; readonly left: string; readonly right: string }"#),
/// List paths tracked by explicit compatibility history.
    LsFiles => ("ls-files", r#"{ readonly kind: "ls-files" }"#),
/// Test paths against the live compatibility ignore policy.
    CheckIgnore {
        /// Portable paths to test.
        paths: Vec<String>,
    } => ("check-ignore", r#"{ readonly kind: "check-ignore"; readonly paths: readonly string[] }"#)
}
}
