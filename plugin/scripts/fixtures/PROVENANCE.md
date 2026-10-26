
Pinned fixture provenance (Codex, 2026-10-25): npm12.2.1, pnpm12.15.0 and
Bun1.4.4 were read from actual npm registry metadata in bb2 job
1792944379701-1087781-99568d63 (source clock
8acff66fd52f8ce8cd1e91db2782c6b24cf146ba6e0e516986c46331038f94cb).
Metadata archive SHA-256:
d776b024bdce00c6b858b0c79e6986aed8c32fee837518680ffe47a9ad697b22.
The lockfile is the unedited output of npm install --package-lock-only
--prefix plugin/scripts/fixtures --ignore-scripts --no-audit --no-fund in
bb2 job1792944498445-1088798-419921f2; archive
457c4f18b2ed0d80dfcf7be4540b533aa191a9cc58590e2b66633c7cfe7f6098
also retains actual installed package manifests and native payload paths.
No command stdout, OS output or provider stream is fabricated. Runtime
qualification records each actual --version output and executable digest.
Run with Node24.15.0 or a newer supported Node version, as required by npm.
Install only via npm ci --prefix plugin/scripts/fixtures --ignore-scripts
--no-audit --no-fund; the tests execute the pinned native payloads directly.

pnpm build approval uses exact local-tarball dependency paths in
pnpm-workspace.yaml, as documented at: https://pnpm.io/cli/add#--allow-build and
https://pnpm.io/settings/build#allowbuilds (read2026-10-25). The old
package.json pnpm.onlyBuiltDependencies input is deleted, not translated.
The actual native12.15.0 refusal is retained in bb2 producer
1792944768176-1093101-7391b06c, metadata archive
af64d45b9fc1999f55e0daf6a61912c725c153d5fe1154e000026faf4d7c2b3e.
That failed run reached all20 cases with zero skips (21 including parent):
19pass2fail; pnpm refusal plus parent failure,8.103760seconds. It is not a pass.
The bare-name --allow-build alternative was also refused by the real native
pnpm; job1792944930289-1093724-e483c7b5, archive
2ae9fdda30f2dfc968bd03a5409ba60312f86a328a098818be66c0ac31c81ddd.
The complete five-file command reached26 tests:24pass2fail,zero skips,
8.821056seconds; the only failing case was pnpm plus its parent. The final
fixture grants only the two exact immutable local release tarball paths.
