"""A copy of Cargo.lock that crate_universe may write.

crate_universe splices a workspace with one synthetic member for the pinned crates and writes the lockfile it
resolved back to its `cargo_lockfile`. Given the source Cargo.lock it would rewrite it; given this copy it rewrites
the copy, and Cargo.lock stays exactly what Cargo writes.
"""

def _cargo_lock_impl(rctx):
    rctx.file("Cargo.lock", rctx.read(rctx.attr.src))
    rctx.file("BUILD.bazel", 'exports_files(["Cargo.lock"])\n')

cargo_lock = repository_rule(
    implementation = _cargo_lock_impl,
    attrs = {"src": attr.label(allow_single_file = True, mandatory = True)},
)
