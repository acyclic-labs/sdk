"""The actual pinned Cargo/Rustdoc tools used by SDK artifact conformance."""

load("//:xwin_repo.bzl", "WINDOWS_LIB_DIRS")

# Rustup itself, which the producers run to select a toolchain, by host: the triple and the SHA-256 that
# static.rust-lang.org/rustup/archive/<version>/<triple>/rustup-init[.exe].sha256 publishes.
_RUSTUP = "1.28.2"
_HOSTS = {
    "linux-x86_64": ("x86_64-unknown-linux-gnu", "20a06e644b0d9bd2fbdbfd52d42540bdde820ea7df86e92e533c073da0cdd43c"),
    "macos-aarch64": ("aarch64-apple-darwin", "20ef5516c31b1ac2290084199ba77dbbcaa1406c45c1d978ca68558ef5964ef5"),
    "windows-x86_64": ("x86_64-pc-windows-msvc", "88d8258dcf6ae4f7a80c7d1088e1f36fa7025a1cfd1343731b4ee6f385121fc0"),
}

# Each toolchain's channel manifest, with the SHA-256 the release publishes beside it as <manifest>.sha256. The
# manifest pins every component archive of every host, so a version bump is a new manifest and its digest.
_STABLE = "1.98.1"
_TOOLCHAINS = {
    _STABLE: ("https://static.rust-lang.org/dist/channel-rust-1.98.1.toml", "a7c8774a5fd8441c997d94c029776cbc5eb111e9d72ab5d256fa69866644347e"),
    "nightly-2026-07-01": ("https://static.rust-lang.org/dist/2026-07-01/channel-rust-nightly.toml", "ae1119807b7f8c262670732ecf022793eba90f778ef3a6c9ea3560ea7a8b925d"),
}

def _archives(manifest, wanted):
    """Return {(package, target): (xz_url, xz_hash)} for the wanted packages of a channel manifest."""
    found = {}
    section = None
    for line in manifest.splitlines():
        if line.startswith("["):
            section = line.strip("[]")
            continue
        if section not in wanted or not line.startswith("xz_"):
            continue
        key, _, value = line.partition(" = ")
        found.setdefault(wanted[section], {})[key] = value.strip('"')
    for component in wanted.values():
        if "xz_url" not in found.get(component, {}) or "xz_hash" not in found.get(component, {}):
            fail("the channel manifest publishes no archive of %s for %s" % component)
    return {component: (entry["xz_url"], entry["xz_hash"]) for component, entry in found.items()}

def _repository_impl(ctx):
    os, _ = ctx.attr.platform.split("-", 1)
    host, digest = _HOSTS[ctx.attr.platform]
    suffix = ".exe" if os == "windows" else ""
    rustup = "tools/bin/rustup" + suffix

    # rustup-init is the rustup binary, which acts as rustup under that name.
    ctx.download(
        url = "https://static.rust-lang.org/rustup/archive/%s/%s/rustup-init%s" % (_RUSTUP, host, ".exe" if os == "windows" else ""),
        output = rustup,
        sha256 = digest,
        executable = True,
    )

    # Each toolchain is the archives its manifest pins, laid out as `rustup toolchain install` lays them out,
    # so every download goes through Bazel's verified, cached downloader and none through rustup's own.
    for toolchain, (manifest_url, manifest_digest) in _TOOLCHAINS.items():
        root = "rustup/toolchains/%s-%s" % (toolchain, host)
        manifest = root + "/lib/rustlib/multirust-channel-manifest.toml"
        ctx.download(url = manifest_url, output = manifest, sha256 = manifest_digest)
        components = [("rustc", host), ("cargo", host), ("rust-std", host)]
        if toolchain.startswith("nightly"):
            components.append(("rust-std", "wasm32-unknown-unknown"))
            if os == "linux":
                # Bazel produces the Windows test's Rustdoc JSON on this host.
                components.append(("rust-std", _HOSTS["windows-x86_64"][0]))

        # On macOS the rustc component's rust-objcopy, which rustc runs to strip debug info, loads libLLVM from
        # rustlib/<host>/lib, and only llvm-tools ships it there: without it every strip aborts and warns on stderr.
        if os == "macos":
            components.append(("llvm-tools-preview", host))
        archives = _archives(ctx.read(manifest), {"pkg.%s.target.%s" % component: component for component in components})
        for package, target in components:
            url, sha256 = archives[(package, target)]
            directory = {"rust-std": "rust-std-" + target}.get(package, package)
            ctx.download_and_extract(
                url = url,
                output = root,
                sha256 = sha256,
                strip_prefix = "%s/%s" % (url.rpartition("/")[2].removesuffix(".tar.xz"), directory),
            )
            ctx.delete(root + "/manifest.in")
        ctx.file(root + "/lib/rustlib/rust-installer-version", "3\n")
        ctx.file(root + "/lib/rustlib/components", "".join(["%s-%s\n" % component for component in components]))
        ctx.file(root + "/lib/rustlib/multirust-config.toml", 'config_version = "1"\n' + "".join([
            '\n[[components]]\npkg = "%s"\ntarget = "%s"\nis_extension = %s\n' % (package, target, "false" if (package, target) in components[:3] else "true")
            for package, target in components
        ]))
    ctx.file("rustup/settings.toml", 'version = "12"\ndefault_toolchain = "%s-%s"\nprofile = "minimal"\n\n[overrides]\n' % (_STABLE, host))
    bin = "rustup/toolchains/%s-%s/bin/" % (_STABLE, host)
    tools = {"rustup": rustup, "cargo": bin + "cargo" + suffix, "rustc": bin + "rustc" + suffix}
    files = 'glob(["tools/**", "rustup/**"])'
    if os == "windows":
        xwin = Label("@xwin//:native_libs")
        runfiles = xwin.workspace_root.removeprefix("external/")
        ctx.file("tools/msvc-lib-paths", "\n".join([runfiles + "/" + path for path in WINDOWS_LIB_DIRS]) + "\n")
        files += " + [%r]" % str(xwin)
    lines = [
        'package(default_visibility = ["//visibility:public"])',
        "TOOLCHAIN_FILES = " + files,
    ]
    for name, executable in tools.items():
        lines.append('filegroup(name = %r, srcs = [%r], data = TOOLCHAIN_FILES)' % (name, executable))
    ctx.file("BUILD.bazel", "\n".join(lines) + "\n")
    return ctx.repo_metadata(reproducible = True)

_repository = repository_rule(
    implementation = _repository_impl,
    attrs = {"platform": attr.string(mandatory = True, values = _HOSTS.keys())},
)

def _repository_name(platform):
    return "sdk_compilers_" + platform.replace("-", "_")

def _hub_impl(ctx):
    lines = ['package(default_visibility = ["//visibility:public"])']
    for platform in _HOSTS:
        os, cpu = platform.split("-", 1)
        lines.append('config_setting(name = %r, constraint_values = ["@platforms//os:%s", "@platforms//cpu:%s"])' % (platform, os, cpu))
    for tool in ["rustup", "cargo", "rustc"]:
        choices = {":" + platform: "@%s//:%s" % (_repository_name(platform), tool) for platform in _HOSTS}
        lines.append('alias(name = %r, actual = select(%r))' % (tool, choices))
    ctx.file("BUILD.bazel", "\n".join(lines) + "\n")
    return ctx.repo_metadata(reproducible = True)

_hub = repository_rule(implementation = _hub_impl)

def _extension_impl(_ctx):
    for platform in _HOSTS:
        _repository(name = _repository_name(platform), platform = platform)
    _hub(name = "sdk_compilers")

sdk_compilers = module_extension(implementation = _extension_impl)
