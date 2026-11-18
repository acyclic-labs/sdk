# CI and SDK releases

Every PR and main push runs `SDK Static`, one required Linux job that checks
manifest and lockfile consistency without compiling. It also supports manual
dispatch with `gh workflow run static.yml --ref <branch>`. Run
`node scripts/check-cargo-lock.mjs` locally to reproduce its manifest and lockfile
check. The checker compares the lockfile with Cargo's own canonical output from
`cargo update --workspace`, preserving locked dependency versions while avoiding
dependency source downloads for metadata. If it changes the file, review and
commit the result. The job has a one-minute timeout. Documentation changes receive
the same required check. Superseded static runs are cancelled.

Formatting, whitespace, JSON and JavaScript syntax, pinned Actions, runner policy,
compatibility digests, builds, tests, generator checks, and native integrations
run in Foreman on Bazel. SDK Release builds and assembles the deploy artifacts.
Whitespace checks cover changes since the previous `acyclic-v*` release tag, or
the tracked tree for the first release. Tags and GitHub Release events start no
workflows.

Workflow validation requires actionlint 1.7.7 and an executable ShellCheck on
PATH. Use the same wrapper as release policy qualification locally:

```sh
bash scripts/check-workflows.sh
```

For a scoped check, pass the actionlint executable and workflow paths:

```sh
bash scripts/check-workflows.sh actionlint .github/workflows/release-acyclic.yml
```

The wrapper fails when ShellCheck is missing and passes its resolved executable
to actionlint explicitly, so a successful check includes shell validation.

## Coordinated release

Commit the same version in all publishable npm, Cargo, and plugin manifests, then
dispatch **SDK Release** (`release.yml`) from main with that version:

```sh
gh workflow run release.yml --repo acyclic-labs/sdk --ref main \
  -f version=0.2.0 -f publish=false
```

`publish=false` runs the complete release graph without publishing. For the live
release, dispatch with `publish=true`. The dispatch freezes main's SHA. Every
checkout, qualification receipt, plugin archive, and publisher uses that source;
later main commits cannot change the release.

The graph builds the supported plugin platforms with Bazel, runs native mount
qualification and generator checks, and assembles and attests the universal
plugin. Package qualification and publication inputs come from the Foreman
Bazel graph; Cargo is not a build path. The registry jobs retain their `npmjs` and `crates-io`
environments. Both registries must trust the calling workflow `release.yml`:
reusable publisher filenames do not preserve their former caller identities.
Configure that workflow for every publishable package and crate before enabling
publication. The validation dispatch requires no registry publication credentials.
Registry version conflicts and mismatching artifact bytes
fail instead of replacing a version. Existing registry integrity and installation
checks remain part of the release.

The same platform builds also produce standalone GraphCoder executables through
`bazel build -c opt //plugin:graphcoder`. Assembly admits the complete target
inventory using the shared native-image verifier. The binaries and version-pinned
shell/PowerShell installers join the existing attested `SHA256SUMS` and GitHub
publisher; the SBOM includes GraphCoder's dependency closure. Native Linux, macOS
and Windows release jobs exercise the offline installer with their built binary,
including replacement and refusal controls. See [Install GraphCoder](graphcoder-install.md).

## Interrupted publication

npm, Cargo, and GitHub cannot commit a release atomically. The run summary reports
each channel's result. A failed publisher can leave some packages published, so
inspect the run before retrying.

Use **Re-run failed jobs** on that same release run. Successful qualification and
build jobs retain their original artifacts and outputs; the remaining publishers
resume those inputs. Registry publishers compare existing packages with the
qualified inputs. Plugin asset uploads accept identical existing assets and
reject conflicting contents. The GitHub Release remains a draft until every
plugin asset has uploaded successfully.

Do not change the version, dispatch a new source, or rerun all build jobs to
recover a partially published release. Qualification artifacts expire after
their configured retention period; missing or expired artifacts fail recovery
instead of silently rebuilding. Qualification retries before publication may
retain successful platform lanes from earlier attempts of the same run. The
assembler freezes their exact artifact names and publishers still validate their
source and contents.

For diagnostics, dispatch **SDK Release** from main with `publish=false` using
the command above. This runs qualification, native mounts, agent hosts, and
generator checks through the release graph without publishing. Those reusable
workflows have no independent manual triggers.

## Release build caches

`release-acyclic.yml` builds every release target with Bazel. GitHub runners have
no cloud cache identity, so the Bazel disk cache (`~/.cache/bazel-disk`) and
repository cache (`~/.cache/bazel-repository-cache`) persist through
`actions/cache`. Only main saves them. The caches are content-addressed compiler,
action and external-repository results: a changed input is rebuilt rather than
served stale. Architecture/runtime ABI checks, installed executable checks,
native mounts and assembly still run after the builds.

## Dependency preservation

The PR335 JavaScript updates apply to the current workspace, without replacing
the corrected qualification and coordinated publication workflows:

- `@bufbuild/protoc-gen-es`, protobuf and its generator plugin: 2.16.0.
- `@napi-rs/cli`: 3.10.8; Node types: 26.6.4.
- Connect, Connect Node and Connect Web: 2.2.0.
- `bun-types`: 1.4.2; `fake-indexeddb`: 6.2.5.
- The existing React/Svelte dependencies, Pi host 1.1.0 pin, Cargo graph and
  source-generation policy are unchanged by these JavaScript updates.

Lock resolver entries and integrity strings come from the
[PR335 lock delta](https://github.com/acyclic-labs/sdk/pull/335/files), including
`undici-types` 8.9.0 and the nested `bun-types` Node types 26.6.5. The published
[@napi-rs/cli 3.10.8 artifact](https://registry.npmjs.org/@napi-rs/cli/-/cli-3.10.8.tgz)
still constructs `Error` in `dist/index.cjs`, `dist/index.js` and
`src/api/templates/js-binding.ts`. Its new `createLoadErrorChain` preserves
messages in `cause`, not original errors in `AggregateError.errors`. Therefore
the existing three-line patch is retained and rebased to that exact artifact;
it must not be removed merely because upstream now provides a cause chain.

Maintained workflows and composite actions use Checkout 7.0.1, Setup Node
7.1.0, Upload Artifact 7.0.2, Download Artifact 8.0.2, Cache 6.1.0 and Setup Go
7.0.0. Workflow names, callers, qualification lanes and retained-artifact
contracts are preserved; no historical publication workflow is restored.

After integrating source changes, resolve the lock with
`bun install --lockfile-only`, then install with `bun install --frozen-lockfile`.
Run `bun run generate` and inspect generated changes before
`bun run check:generated`, `bun run test` and `bun run test:contracts:browser`;
the last command now builds Actors, Objects and Stream WASM before compiling
and exercising the Actors browser client. These are integration qualification
steps, not evidence of checks executed while porting the dependency updates.
