# CI and SDK releases

Every PR and main push runs `SDK Static`, one required job on standard
GitHub-hosted Ubuntu. It runs `cargo update --workspace --locked` to check
manifest/lockfile consistency without compiling. The job has a one-minute timeout;
the initial GitHub run completed in seven seconds. Documentation changes receive
the same required check. Superseded static runs are cancelled.

Formatting, whitespace, JSON and JavaScript syntax, pinned Actions, runner policy,
compatibility digests, builds, tests, generator checks, and native integrations
run only through **SDK Release**. Whitespace checks cover changes since the previous
`acyclic-v*` release tag, or the tracked tree for the first release. Tags and GitHub
Release events start no workflows.

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

The graph runs full SDK qualification once, supported plugin platform builds,
native mount and host integration, and generator checks. It assembles and attests
the universal plugin and passes explicit retained artifact references to the npm
and Cargo publishers. The registry jobs retain their `npmjs` and `crates-io`
environments. Both registries must trust the calling workflow `release.yml`:
reusable publisher filenames do not preserve their former caller identities.
Configure that workflow for every publishable package and crate before enabling
publication. The validation dispatch requires no registry publication credentials.
Registry version conflicts and mismatching artifact bytes
fail instead of replacing a version. Existing registry integrity and installation
checks remain part of the release.

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
