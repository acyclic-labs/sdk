import type {
  GitFilesystemAction,
  GitTreeRef,
  WorkspaceIdentity,
} from "../generated/git-compat-contract.js";

const workspace = new Uint8Array(16) as WorkspaceIdentity;
const generation = new Uint8Array(32);
const pinnedTree: GitTreeRef = {
  kind: "exact",
  workspace_id: workspace,
  generation,
};

/** A generated declaration consumer can pin the source tree for a Join. */
export const pinnedJoin = {
  Join: {
    target_tree: pinnedTree,
    source_workspace: workspace,
    source_tree: pinnedTree,
    rebase: false,
    tracked_paths: ["src/index.ts"],
  },
} satisfies GitFilesystemAction;

/** The generated GitTreeRef union rejects an unrecognized tree reference. */
export const malformedJoin = {
  Join: {
    target_tree: pinnedTree,
    source_workspace: workspace,
    // @ts-expect-error malformed tree references must not enter the public ABI
    source_tree: { kind: "malformed", workspace_id: workspace, generation },
    rebase: false,
    tracked_paths: [],
  },
} satisfies GitFilesystemAction;

/** Omitting source_tree remains valid for a join that uses the source head. */
export const headJoin = {
  Join: {
    target_tree: pinnedTree,
    source_workspace: workspace,
    rebase: true,
    tracked_paths: [],
  },
} satisfies GitFilesystemAction;
