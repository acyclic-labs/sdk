use super::*;

/// Identity byte widths used by language bindings.
pub const GIT_COMPAT_IDENTITY_WIRE_FIELDS: &[(&str, usize)] = &[
    ("workspace_id", std::mem::size_of::<WorkspaceId>()),
    ("source_workspace", std::mem::size_of::<WorkspaceId>()),
    ("generation", std::mem::size_of::<GenerationId>()),
    ("id", std::mem::size_of::<GenerationId>()),
    ("authored_generation", std::mem::size_of::<GenerationId>()),
    ("identity", std::mem::size_of::<WorkspaceId>()),
    ("overlay", std::mem::size_of::<GenerationId>()),
    ("shadows", std::mem::size_of::<GenerationId>()),
    ("initial_generation", std::mem::size_of::<GenerationId>()),
];

/// Byte array fields and their optional fixed widths.
pub const GIT_COMPAT_BYTE_WIRE_FIELDS: &[(&str, Option<usize>)] = &[
    ("workspace_id", Some(std::mem::size_of::<WorkspaceId>())),
    ("source_workspace", Some(std::mem::size_of::<WorkspaceId>())),
    ("generation", Some(std::mem::size_of::<GenerationId>())),
    ("id", Some(std::mem::size_of::<GenerationId>())),
    (
        "authored_generation",
        Some(std::mem::size_of::<GenerationId>()),
    ),
    ("identity", Some(std::mem::size_of::<WorkspaceId>())),
    ("overlay", Some(std::mem::size_of::<GenerationId>())),
    ("shadows", Some(std::mem::size_of::<GenerationId>())),
    (
        "initial_generation",
        Some(std::mem::size_of::<GenerationId>()),
    ),
    ("patch", None),
];

/// Field names using the signed JSON timestamp representation.
pub const GIT_COMPAT_TIMESTAMP_WIRE_FIELDS: &[&str] = &["authored_at_seconds"];

/// Paths whose values are UUID encoded operation identities.
pub const GIT_COMPAT_UUID_WIRE_PATHS: &[&str] = &[
    "Prepared.transition",
    "pending.id",
    "capture_proof.operation_id",
];

/// Tagged Rust variants containing opaque executor-owned JSON.
pub const GIT_COMPAT_OPAQUE_WIRE_PATHS: &[&str] = &["Filesystem.Data"];

/// Public spelling aliases applied by generated binding projections.
pub const GIT_COMPAT_PUBLIC_WIRE_ALIASES: &[(&str, &str)] =
    &[("Status.all_changes_staged", "allChangesStaged")];

/// Required fields of the durable pending-transition JSON object.
pub const GIT_COMPAT_PENDING_WIRE_FIELDS: &[&str] = &["id", "action", "mutation"];

/// Width of the UUID encoded by `GitTransitionId` in compatibility JSON.
pub const GIT_COMPAT_TRANSITION_ID_BYTES: usize = std::mem::size_of::<GitTransitionId>();

/// Natural JavaScript command kinds paired with canonical serde tags.
pub const GIT_COMPAT_PUBLIC_COMMAND_KINDS: &[(&str, &str)] = &[
    ("status", "Status"),
    ("diff", "Diff"),
    ("log", "Log"),
    ("show", "Show"),
    ("add", "Add"),
    ("commit", "Commit"),
    ("branch", "Branch"),
    ("switch", "Switch"),
    ("restore", "Restore"),
    ("reset", "Reset"),
    ("merge", "Merge"),
    ("merge-continue", "MergeContinue"),
    ("merge-abort", "MergeAbort"),
    ("rebase", "Rebase"),
    ("stash-push", "StashPush"),
    ("stash-pop", "StashPop"),
    ("cherry-pick", "CherryPick"),
    ("revert", "Revert"),
    ("tag", "Tag"),
    ("blame", "Blame"),
    ("grep", "Grep"),
    ("clean", "Clean"),
    ("archive", "Archive"),
    ("apply", "Apply"),
    ("bisect", "Bisect"),
    ("rev-parse", "RevParse"),
    ("symbolic-ref", "SymbolicRef"),
    ("merge-base", "MergeBase"),
    ("ls-files", "LsFiles"),
    ("check-ignore", "CheckIgnore"),
];

pub(super) fn public_git_command_variant(kind: &str) -> Option<&'static str> {
    GIT_COMPAT_PUBLIC_COMMAND_KINDS
        .iter()
        .find_map(|(public, wire)| (*public == kind).then_some(*wire))
}

/// Parses natural JavaScript command JSON into the canonical command.
pub fn parse_git_public_command(value: &str) -> Result<GitCommand, String> {
    let mut value: serde_json::Value = serde_json::from_str(value)
        .map_err(|error| format!("invalid Git command JSON: {error}"))?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| "Git command must be an object".to_owned())?;
    let kind = object
        .remove("kind")
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or_else(|| "Git command kind must be a string".to_owned())?;
    let variant = public_git_command_variant(&kind)
        .ok_or_else(|| format!("unknown Git command kind: {kind}"))?;
    object.retain(|_, value| !value.is_null());
    if let Some(mut value) = object.remove("authoredAtSeconds") {
        if let Some(text) = value.as_str() {
            let number = text
                .parse::<i64>()
                .map_err(|_| "Git authoredAtSeconds must be an i64".to_owned())?;
            value = serde_json::Value::Number(number.into());
        }
        object.insert("authored_at_seconds".to_owned(), value);
    }
    if let Some(value) = object.get("authored_at_seconds") {
        let number = value
            .as_i64()
            .ok_or_else(|| "Git authoredAtSeconds must be an i64".to_owned())?;
        if number.unsigned_abs() > 9_007_199_254_740_991 {
            return Err("Git authoredAtSeconds must fit a safe JSON integer".to_owned());
        }
    }
    if let Some(value) = object.remove("dryRun") {
        object.insert("dry_run".to_owned(), value);
    }
    if let Some(field) = match variant {
        "Diff" => Some("cached"),
        "Switch" => Some("create"),
        "Tag" => Some("delete"),
        "Clean" => Some("dry_run"),
        "SymbolicRef" => Some("short"),
        _ => None,
    } {
        object
            .entry(field)
            .or_insert(serde_json::Value::Bool(false));
    }
    if variant == "Reset" && object.contains_key("mode") {
        let mode = object
            .get("mode")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "Git reset mode must be a string".to_owned())?;
        let mode = match mode {
            "soft" => "Soft",
            "mixed" => "Mixed",
            "hard" => "Hard",
            _ => return Err(format!("unsupported Git reset mode: {mode}")),
        };
        object.insert(
            "mode".to_owned(),
            serde_json::Value::String(mode.to_owned()),
        );
    }
    let unit_variant = matches!(
        variant,
        "Status" | "MergeContinue" | "MergeAbort" | "StashPush" | "StashPop" | "LsFiles"
    );
    let wire = if unit_variant && object.is_empty() {
        serde_json::Value::String(variant.to_owned())
    } else {
        let mut tagged = serde_json::Map::new();
        tagged.insert(variant.to_owned(), value);
        serde_json::Value::Object(tagged)
    };
    serde_json::from_value(wire).map_err(|error| format!("invalid Git command: {error}"))
}

/// Validates and reserializes canonical command output JSON.
pub fn canonicalize_git_output_json(value: &str) -> Result<String, String> {
    let output: GitCommandOutput = serde_json::from_str(value)
        .map_err(|error| format!("invalid Git command output JSON: {error}"))?;
    serde_json::to_string(&output)
        .map_err(|error| format!("failed to serialize Git command output: {error}"))
}

/// Validates and reserializes one durable pending transition.
pub fn canonicalize_git_pending_transition_json(value: &str) -> Result<String, String> {
    let pending: GitPendingTransition = serde_json::from_str(value)
        .map_err(|error| format!("invalid Git pending transition JSON: {error}"))?;
    serde_json::to_string(&pending)
        .map_err(|error| format!("failed to serialize Git pending transition: {error}"))
}

/// Defines one canonical enum tag inventory and its exhaustive Rust matcher.
macro_rules! git_compat_enum_contract {
    (
        $enum:ident,
        $inventory:ident;
        [$($name:literal => $pattern:pat),+ $(,)?];
        [$($extra_name:literal => $extra_pattern:pat),* $(,)?]
    ) => {
        #[doc = "Canonical serde/TypeScript variant inventory."]
        pub const $inventory: &[&str] = &[$($name),+];

        impl $enum {
            /// Returns the canonical serde/TypeScript tag for this value.
            #[must_use]
            pub const fn variant_name(&self) -> &'static str {
                match self {
                    $($pattern => $name,)+
                    $($extra_pattern => $extra_name,)*
                }
            }
        }
    };
}

/// Defines scalar serde tags and their TypeScript literals.
macro_rules! git_compat_scalar_contract {
    (
        $enum:ident,
        $inventory:ident,
        $typescript:ident;
        $wire_mapping:ident;
        [$($wire:literal => $ts:literal => $pattern:pat),+ $(,)?]
    ) => {
        #[doc = "Canonical serde variant inventory."]
        pub const $inventory: &[&str] = &[$($wire),+];
        #[doc = "Natural TypeScript literal values for the canonical enum."]
        pub const $typescript: &[&str] = &[$($ts),+];
        #[doc = "Natural TypeScript values paired with their serde wire tags."]
        pub const $wire_mapping: &[(&str, &str)] = &[$(($ts, $wire)),+];

        impl $enum {
            /// Returns the canonical serde tag for this value.
            #[must_use]
            pub const fn variant_name(&self) -> &'static str {
                match self {
                    $($pattern => $wire,)+
                }
            }
        }
    };
}

git_compat_enum_contract! {
    GitCommand, GIT_COMPAT_COMMAND_VARIANTS;
    [
        "Status" => GitCommand::Status,
        "Diff" => GitCommand::Diff { .. },
        "Log" => GitCommand::Log { .. },
        "Show" => GitCommand::Show { .. },
        "Add" => GitCommand::Add { .. },
        "Commit" => GitCommand::Commit { .. },
        "Branch" => GitCommand::Branch { .. },
        "Switch" => GitCommand::Switch { .. },
        "Restore" => GitCommand::Restore { .. },
        "Reset" => GitCommand::Reset { .. },
        "Merge" => GitCommand::Merge { .. },
        "MergeContinue" => GitCommand::MergeContinue,
        "MergeAbort" => GitCommand::MergeAbort,
        "Rebase" => GitCommand::Rebase { .. },
        "StashPush" => GitCommand::StashPush,
        "StashPop" => GitCommand::StashPop,
        "CherryPick" => GitCommand::CherryPick { .. },
        "Revert" => GitCommand::Revert { .. },
        "Tag" => GitCommand::Tag { .. },
        "Blame" => GitCommand::Blame { .. },
        "Grep" => GitCommand::Grep { .. },
        "Clean" => GitCommand::Clean { .. },
        "Archive" => GitCommand::Archive { .. },
        "Apply" => GitCommand::Apply { .. },
        "Bisect" => GitCommand::Bisect { .. },
        "RevParse" => GitCommand::RevParse { .. },
        "SymbolicRef" => GitCommand::SymbolicRef { .. },
        "MergeBase" => GitCommand::MergeBase { .. },
        "LsFiles" => GitCommand::LsFiles,
        "CheckIgnore" => GitCommand::CheckIgnore { .. },
    ];
    []
}

git_compat_scalar_contract! {
    GitResetMode,
    GIT_COMPAT_RESET_MODE_VARIANTS,
    GIT_COMPAT_RESET_MODE_TYPESCRIPT_VARIANTS;
    GIT_COMPAT_RESET_MODE_WIRE;
    [
        "Soft" => "soft" => GitResetMode::Soft,
        "Mixed" => "mixed" => GitResetMode::Mixed,
        "Hard" => "hard" => GitResetMode::Hard,
    ]
}

git_compat_scalar_contract! {
    GitDirtyState,
    GIT_COMPAT_DIRTY_STATE_VARIANTS,
    GIT_COMPAT_DIRTY_STATE_TYPESCRIPT_VARIANTS;
    GIT_COMPAT_DIRTY_STATE_WIRE;
    [
        "clean" => "clean" => GitDirtyState::Clean,
        "dirty" => "dirty" => GitDirtyState::Dirty,
        "unknown" => "unknown" => GitDirtyState::Unknown,
    ]
}

/// Natural TypeScript projection for each canonical command variant.
pub const GIT_COMPAT_COMMAND_TYPESCRIPT_TYPES: &[(&str, &str)] = &[
    ("Status", r#"{ readonly kind: "status" }"#),
    (
        "Diff",
        r#"{ readonly kind: "diff"; readonly cached?: boolean }"#,
    ),
    (
        "Log",
        r#"{ readonly kind: "log"; readonly maximum: number }"#,
    ),
    (
        "Show",
        r#"{ readonly kind: "show"; readonly object?: string }"#,
    ),
    (
        "Add",
        r#"{ readonly kind: "add"; readonly paths: readonly string[] }"#,
    ),
    (
        "Commit",
        r#"{ readonly kind: "commit"; readonly message: string; readonly author: string; readonly authoredAtSeconds: bigint }"#,
    ),
    (
        "Branch",
        r#"{ readonly kind: "branch"; readonly create?: string }"#,
    ),
    (
        "Switch",
        r#"{ readonly kind: "switch"; readonly branch: string; readonly create?: boolean }"#,
    ),
    (
        "Restore",
        r#"{ readonly kind: "restore"; readonly source?: string; readonly paths: readonly string[] }"#,
    ),
    (
        "Reset",
        r#"{ readonly kind: "reset"; readonly target: string; readonly mode: GitResetMode }"#,
    ),
    (
        "Merge",
        r#"{ readonly kind: "merge"; readonly branch: string }"#,
    ),
    ("MergeContinue", r#"{ readonly kind: "merge-continue" }"#),
    ("MergeAbort", r#"{ readonly kind: "merge-abort" }"#),
    (
        "Rebase",
        r#"{ readonly kind: "rebase"; readonly branch: string }"#,
    ),
    ("StashPush", r#"{ readonly kind: "stash-push" }"#),
    ("StashPop", r#"{ readonly kind: "stash-pop" }"#),
    (
        "CherryPick",
        r#"{ readonly kind: "cherry-pick"; readonly object: string }"#,
    ),
    (
        "Revert",
        r#"{ readonly kind: "revert"; readonly object: string }"#,
    ),
    (
        "Tag",
        r#"{ readonly kind: "tag"; readonly name?: string; readonly target?: string; readonly delete?: boolean }"#,
    ),
    (
        "Blame",
        r#"{ readonly kind: "blame"; readonly path: string }"#,
    ),
    (
        "Grep",
        r#"{ readonly kind: "grep"; readonly pattern: string; readonly path?: string }"#,
    ),
    (
        "Clean",
        r#"{ readonly kind: "clean"; readonly dryRun: boolean }"#,
    ),
    (
        "Archive",
        r#"{ readonly kind: "archive"; readonly object?: string }"#,
    ),
    (
        "Apply",
        r#"{ readonly kind: "apply"; readonly patch: Uint8Array }"#,
    ),
    (
        "Bisect",
        r#"{ readonly kind: "bisect"; readonly arguments: readonly string[] }"#,
    ),
    (
        "RevParse",
        r#"{ readonly kind: "rev-parse"; readonly argument: string }"#,
    ),
    (
        "SymbolicRef",
        r#"{ readonly kind: "symbolic-ref"; readonly short?: boolean }"#,
    ),
    (
        "MergeBase",
        r#"{ readonly kind: "merge-base"; readonly left: string; readonly right: string }"#,
    ),
    ("LsFiles", r#"{ readonly kind: "ls-files" }"#),
    (
        "CheckIgnore",
        r#"{ readonly kind: "check-ignore"; readonly paths: readonly string[] }"#,
    ),
];

/// Natural TypeScript projections for executor actions, results, and outputs.
pub const GIT_COMPAT_ACTION_TYPESCRIPT_TYPES: &[(&str, &str)] = &[
    (
        "CaptureCommit",
        r#"{ readonly CaptureCommit: { readonly workspace_tree: GitTreeRef; readonly head_tree: GitTreeRef | undefined; readonly head_workspace_tree: GitTreeRef | undefined; readonly tracked_paths: readonly string[]; readonly message: string; readonly author: string; readonly authored_at_seconds: number; readonly expected_head: GitCommitIdentity | undefined } }"#,
    ),
    (
        "ForkBranch",
        r#"{ readonly ForkBranch: { readonly branch: string; readonly source_tree: GitTreeRef; readonly head: GitCommitIdentity | undefined; readonly switch: boolean } }"#,
    ),
    (
        "SwitchWorkspace",
        r#"{ readonly SwitchWorkspace: { readonly workspace_id: WorkspaceIdentity } }"#,
    ),
    (
        "Diff",
        r#"{ readonly Diff: { readonly from: GitTreeRef | undefined; readonly to: GitTreeRef; readonly tracked_paths: readonly string[] } }"#,
    ),
    (
        "RestoreGeneration",
        r#"{ readonly RestoreGeneration: { readonly tree: GitTreeRef; readonly paths: readonly string[] | undefined } }"#,
    ),
    (
        "RestorePaths",
        r#"{ readonly RestorePaths: { readonly tree: GitTreeRef; readonly paths: readonly string[] } }"#,
    ),
    (
        "Join",
        r#"{ readonly Join: { readonly target_tree: GitTreeRef; readonly source_workspace: WorkspaceIdentity; readonly rebase: boolean; readonly tracked_paths: readonly string[] } }"#,
    ),
    (
        "ApplyCommit",
        r#"{ readonly ApplyCommit: { readonly commit: GitCommitIdentity; readonly reverse: boolean; readonly base: GitTreeRef | undefined; readonly source: GitTreeRef | undefined; readonly paths: readonly string[]; readonly tracked_paths: readonly string[] } }"#,
    ),
    (
        "Blame",
        r#"{ readonly Blame: { readonly path: string; readonly commits: readonly GitCommit[] } }"#,
    ),
    (
        "Grep",
        r#"{ readonly Grep: { readonly pattern: string; readonly path: string | undefined; readonly tree: GitTreeRef } }"#,
    ),
    (
        "Clean",
        r#"{ readonly Clean: { readonly dry_run: boolean; readonly tree: GitTreeRef; readonly tracked_paths: readonly string[] } }"#,
    ),
    (
        "Archive",
        r#"{ readonly Archive: { readonly tree: GitTreeRef } }"#,
    ),
    (
        "ApplyPatch",
        r#"{ readonly ApplyPatch: { readonly patch: Uint8Array } }"#,
    ),
    (
        "CheckIgnore",
        r#"{ readonly CheckIgnore: { readonly paths: readonly string[]; readonly tree: GitTreeRef } }"#,
    ),
];

/// Natural TypeScript projections for executor result variants.
pub const GIT_COMPAT_RESULT_TYPESCRIPT_TYPES: &[(&str, &str)] = &[
    (
        "Captured",
        r#"{ readonly Captured: { readonly tree: GitTreeRef; readonly tracked_paths: readonly string[]; readonly proof: GitCaptureProof | undefined } }"#,
    ),
    (
        "Forked",
        r#"{ readonly Forked: { readonly workspace_id: WorkspaceIdentity } }"#,
    ),
    (
        "Applied",
        r#"{ readonly Applied: { readonly tree: GitTreeRef | undefined; readonly tracked_paths: readonly string[] | undefined } }"#,
    ),
    (
        "Data",
        r#"{ readonly Data: { readonly kind: string; readonly value: unknown } }"#,
    ),
];

/// Natural TypeScript projections for command output variants.
pub const GIT_COMPAT_OUTPUT_TYPESCRIPT_TYPES: &[(&str, &str)] = &[
    ("Status", r#"{ readonly Status: GitCompatStatus }"#),
    ("Commits", r#"{ readonly Commits: readonly GitCommit[] }"#),
    (
        "Branches",
        r#"{ readonly Branches: { readonly current: string; readonly branches: readonly GitBranch[] } }"#,
    ),
    (
        "Tags",
        r#"{ readonly Tags: Readonly<Record<string, GitCommitIdentity>> }"#,
    ),
    ("Committed", r#"{ readonly Committed: GitCommit }"#),
    ("Bisect", r#"{ readonly Bisect: GitBisectResult }"#),
    ("Action", r#"{ readonly Action: GitFilesystemAction }"#),
    (
        "Prepared",
        r#"{ readonly Prepared: { readonly transition: OperationIdentity; readonly action: GitFilesystemAction } }"#,
    ),
    (
        "Filesystem",
        r#"{ readonly Filesystem: GitFilesystemResult }"#,
    ),
    ("Text", r#"{ readonly Text: string }"#),
    ("Paths", r#"{ readonly Paths: readonly string[] }"#),
];

/// Shared nested TypeScript projections used by the variant declarations.
pub const GIT_COMPAT_NESTED_TYPESCRIPT_TYPES: &[(&str, &str)] = &[
    (
        "GitCompatStatus",
        r#"export interface GitCompatStatus { readonly branch: string; readonly head: GitCommitIdentity | undefined; readonly workspace: GitTreeRef; readonly dirty: GitDirtyState; readonly allChangesStaged: true; }"#,
    ),
    (
        "GitBisectResult",
        r#"export interface GitBisectResult { readonly active: boolean; readonly good: GitCommitIdentity | undefined; readonly bad: GitCommitIdentity | undefined; readonly current: GitCommitIdentity | undefined; readonly remaining: number; readonly first_bad: GitCommitIdentity | undefined; }"#,
    ),
    (
        "GitGenerationRef",
        r#"export interface GitGenerationRef { readonly workspace_id: WorkspaceIdentity; readonly generation: GenerationIdentity; }"#,
    ),
    (
        "GitTreeRefExact",
        r#"export type GitTreeRefExact = { readonly kind: "exact"; readonly workspace_id: WorkspaceIdentity; readonly generation: GenerationIdentity };"#,
    ),
    (
        "GitTreeRefLazy",
        r#"export type GitTreeRefLazy = { readonly kind: "lazy"; readonly id: Uint8Array; readonly workspace_id: WorkspaceIdentity; readonly authored_generation: GenerationIdentity; readonly source: GitLazySource; readonly overlay: Uint8Array; readonly shadows: Uint8Array };"#,
    ),
    (
        "GitLazySource",
        r#"export interface GitLazySource { readonly identity: Uint8Array; readonly epoch: bigint; }"#,
    ),
    (
        "GitTreeRef",
        r#"export type GitTreeRef = GitTreeRefExact | GitTreeRefLazy;"#,
    ),
    (
        "GitCaptureProof",
        r#"export interface GitCaptureProof { readonly fork_parent: GitTreeRef; readonly initial_generation: GenerationIdentity; readonly operation_id: OperationIdentity; }"#,
    ),
    (
        "GitCommit",
        r#"export interface GitCommit { readonly id: GitCommitIdentity; readonly tree: GitTreeRef; readonly workspace_tree: GitTreeRef; readonly capture_proof: GitCaptureProof | undefined; readonly tracked_paths: readonly string[]; readonly parents: readonly GitCommitIdentity[]; readonly author: string; readonly authored_at_seconds: number; readonly message: string; }"#,
    ),
    (
        "GitBranch",
        r#"export interface GitBranch { readonly name: string; readonly workspace_id: WorkspaceIdentity; readonly head: GitCommitIdentity | undefined; readonly tracked_paths: readonly string[]; }"#,
    ),
];

git_compat_enum_contract! {
    GitFilesystemAction, GIT_COMPAT_ACTION_VARIANTS;
    [
        "CaptureCommit" => GitFilesystemAction::CaptureCommit { .. },
        "ForkBranch" => GitFilesystemAction::ForkBranch { .. },
        "SwitchWorkspace" => GitFilesystemAction::SwitchWorkspace { .. },
        "Diff" => GitFilesystemAction::Diff { .. },
        "RestoreGeneration" => GitFilesystemAction::RestoreGeneration { .. },
        "RestorePaths" => GitFilesystemAction::RestorePaths { .. },
        "Join" => GitFilesystemAction::Join { .. },
        "ApplyCommit" => GitFilesystemAction::ApplyCommit { .. },
        "Blame" => GitFilesystemAction::Blame { .. },
        "Grep" => GitFilesystemAction::Grep { .. },
        "Clean" => GitFilesystemAction::Clean { .. },
        "Archive" => GitFilesystemAction::Archive { .. },
        "ApplyPatch" => GitFilesystemAction::ApplyPatch { .. },
        "CheckIgnore" => GitFilesystemAction::CheckIgnore { .. },
    ];
    []
}

git_compat_enum_contract! {
    GitFilesystemResult, GIT_COMPAT_RESULT_VARIANTS;
    [
        "Captured" => GitFilesystemResult::Captured { .. },
        "Forked" => GitFilesystemResult::Forked { .. },
        "Applied" => GitFilesystemResult::Applied { .. },
        "Data" => GitFilesystemResult::Data { .. },
    ];
    []
}

git_compat_enum_contract! {
    GitCommandOutput, GIT_COMPAT_OUTPUT_VARIANTS;
    [
        "Status" => GitCommandOutput::Status(_),
        "Commits" => GitCommandOutput::Commits(_),
        "Branches" => GitCommandOutput::Branches { .. },
        "Tags" => GitCommandOutput::Tags(_),
        "Committed" => GitCommandOutput::Committed(_),
        "Bisect" => GitCommandOutput::Bisect(_),
        "Action" => GitCommandOutput::Action(_),
        "Prepared" => GitCommandOutput::Prepared { .. },
        "Filesystem" => GitCommandOutput::Filesystem(_),
        "Text" => GitCommandOutput::Text(_),
        "Paths" => GitCommandOutput::Paths(_),
    ];
    ["NoOp" => GitCommandOutput::NoOp]
}
