# Install GraphCoder

GraphCoder's prebuilt executables are published in this repository's
[GitHub Releases](https://github.com/acyclic-labs/sdk/releases). No Rust toolchain
or package manager is needed. Linux and macOS support x86_64 and arm64; Windows
supports x64 and arm64. Linux assets include glibc and musl builds.

Linux or macOS (curl and the system SHA-256 tool):

```sh
curl --fail --silent --show-error --location --proto '=https' --proto-redir '=https' https://github.com/acyclic-labs/sdk/releases/latest/download/install-graphcoder.sh | sh
```

Windows, in PowerShell 7:

```powershell
Invoke-RestMethod https://github.com/acyclic-labs/sdk/releases/latest/download/install-graphcoder.ps1 | Invoke-Expression
```

The installer prints the installation directory. Add it to `PATH`, then run
`graphcoder --version` and `graphcoder --help`. The default is `~/.local/bin` on
Linux/macOS and `%LOCALAPPDATA%/GraphCoder/bin` on Windows. Set
`GRAPHCODER_INSTALL_DIR` to select another directory. Installation needs write
access there and does not require administrator privileges.

Each installer pins its downloads to its release's `acyclic-vVERSION` tag,
verifies the binary against that release's `SHA256SUMS`, and checks its version
before replacing an existing installation with a same-filesystem rename.
Download or verification failure leaves the previous executable intact. On
Windows, close GraphCoder before replacing a running executable.

To pin a release, replace `latest/download` in the command with
`download/acyclic-vVERSION`. To update, run the latest installer again. To remove
GraphCoder, delete the installed executable; your session files stay available.

For offline installation, download the installer, `SHA256SUMS` and the matching
`graphcoder-TARGET` executable (`.exe` on Windows) from the same release. Set
`GRAPHCODER_ASSET_DIR` to that directory and run the downloaded installer. This
uses the same verification path, and a missing local file fails immediately.
Checksums establish integrity against the selected release inventory. Release
assets also carry GitHub build provenance attestations.

## Stock Docker and sandboxes without FUSE

GraphCoder defaults to `--workspace-mounting auto`. Before opening a workspace,
it queries the native mounting capability. If native mounting is unavailable,
it selects plain directories (`physical`) and prints the capability reason.
This mode needs neither `/dev/fuse`, a fusectl mount, a FUSE helper nor privileged
container execution. It uses the same session, stream fork and merge machinery;
filesystem changes are captured at tool boundaries.

Use `graphcoder --workspace-mounting physical doctor` to inspect an explicitly
selected no-FUSE deployment, adding the same provider and workspace arguments
as the session. `--workspace-mounting native` requires native capability and
refuses when it is unavailable. A failure after native startup begins remains
an error; it never silently changes an active workspace's mounting mode.

The deterministic stock-container acceptance command is:

```sh
docker pull node:24-trixie
node scripts/qualify-docker-no-fuse.mjs node:24-trixie \
  /absolute/path/to/graphcoder /absolute/path/to/harness-mock-model \
  /absolute/path/to/fresh-evidence
```

Run this on Linux with Docker available and executables built from the checked
out source. The qualifier pins the selected image ID, transfers a committed
acceptance snapshot and runs the shipping session cases with both automatic
and explicit physical selection. It asserts a child's shell write, stream fork
and completed merge back into the admitted checkout while preserving the original
files. Its scripted model needs no credentials or external network. The broader
file-tool, messaging, capture, reopen and crash-recovery cases remain available
as explicit case-name arguments after the evidence directory.

The container runs as the caller's UID/GID with no network, extra capabilities,
devices or security overrides. Evidence must show Docker's seccomp filter,
no effective or bounding `CAP_SYS_ADMIN`, no `/dev/fuse`, no FUSE helper and no
mounted fusectl. PASS is published only after the container is stopped,
evidence is retrieved and the container is removed. Retain `docker.json`,
`container.json`, the acceptance matrix and transcripts with the exact source
SHA and executable digests. An earlier source's PASS does not qualify a new head.

Maintainers build the shipped executable with
`node scripts/build-product.mjs graphcoder`, using the shared `dist` build
recipe. The existing release workflow verifies each target's native image,
exercises installation with real binaries on Linux, macOS and Windows, and
includes all GraphCoder binaries and installers in the attested `SHA256SUMS`.
The offline scenario is `node scripts/test-install-release-asset.mjs`, with
`GRAPHCODER_TEST_BINARY` set to an absolute source-qualified executable.
