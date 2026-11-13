# Standalone transport feature control

Run the locked `acyclic-fs-wire` all-target check on the named source SHA,
without any other package selected. This compiles its real generated native
gRPC client and server; feature unification with consumers must not hide its
own missing transport/codegen declaration.

Then apply `standalone-wire.patch` only to that job's source copy and rerun the
same single-package check. Require a compiler failure specifically identifying
missing tonic codegen or transport, not dependency availability, setup or a
lockfile error. Reverse the patch before any subsequent proof.

The patch reproduces the previous bare workspace declaration while the shared
tonic default features are disabled. The normal manifest retains the former
native feature selection; wasm dependencies remain codegen-only.

Use the build image's POSIX `grep` for the diagnostic assertion; do not add a
ripgrep prerequisite to this proof:

```sh
grep -Eq '^error\[E043[23]\]:.*(codegen|transport)' "$negative_log"
```

The error must come from the real generated client/server compile after the
feature-removal patch. A missing verifier executable, dependency/lockfile
failure, or a successful compile is not negative evidence.
