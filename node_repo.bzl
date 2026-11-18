"""The Node.js runtime Bazel tests start, from the official release for the target platform.

The version and its archive digests are pinned here, so a test's node comes from runfiles
and its action key names no host tool directory. `@nodejs//:node` selects the release for the
platform the test runs on, never the host that builds it: a test cross-built for Windows runs
the Windows node.
"""

_VERSION = "24.22.2"

# platform -> (release name, archive suffix, sha256, node path in the archive)
_RELEASES = {
    "linux_x86_64": ("linux-x64", "tar.xz", "48cc0e21e048790e54c0cf5749c2725005313d652651c40b7a21feb9697aaf75", "bin/node"),
    "macos_aarch64": ("darwin-arm64", "tar.gz", "aa9549349e47dffaa42dcf95d6e06ade8c4bdafc980979883b88767670df385a", "bin/node"),
    "macos_x86_64": ("darwin-x64", "tar.gz", "d5e336d588c45be0c37dbd52cc29895593009ca4997a77a1fd02815131ed0e52", "bin/node"),
    "windows_x86_64": ("win-x64", "zip", "ca1bff5d8ead13c9f2ea404a176db46bd430704d5b5113734049fa365e7799e2", "node.exe"),
}

_DEFS = """\
def _tool_impl(ctx):
    node = ctx.file.node
    executable = ctx.actions.declare_file(ctx.label.name + ("." + node.extension if node.extension else ""))
    ctx.actions.symlink(output = executable, target_file = node, is_executable = True)
    return [DefaultInfo(
        executable = executable,
        files = depset([executable]),
        runfiles = ctx.runfiles(files = [executable, node]),
    )]

tool = rule(
    implementation = _tool_impl,
    attrs = {"node": attr.label(allow_single_file = True, mandatory = True)},
    executable = True,
)
"""

_RELEASE_BUILD = """\
load("//:defs.bzl", "tool")

package(default_visibility = ["//visibility:public"])

tool(
    name = "node",
    node = "%s",
)
"""

def _node_release_impl(ctx):
    stem = "node-v%s-%s" % (ctx.attr.version, ctx.attr.release)
    ctx.download_and_extract(
        url = "https://nodejs.org/dist/v%s/%s.%s" % (ctx.attr.version, stem, ctx.attr.suffix),
        sha256 = ctx.attr.sha256,
        stripPrefix = stem,
    )
    ctx.file("defs.bzl", _DEFS)
    ctx.file("BUILD.bazel", _RELEASE_BUILD % ctx.attr.node)
    return ctx.repo_metadata(reproducible = True)

_node_release = repository_rule(
    implementation = _node_release_impl,
    attrs = {
        "version": attr.string(mandatory = True),
        "release": attr.string(mandatory = True),
        "suffix": attr.string(mandatory = True),
        "sha256": attr.string(mandatory = True),
        "node": attr.string(mandatory = True),
    },
)

def _hub_build():
    lines = ['package(default_visibility = ["//visibility:public"])', ""]
    for key in _RELEASES:
        os, cpu = key.split("_", 1)
        lines.append('config_setting(name = "%s", constraint_values = ["@platforms//os:%s", "@platforms//cpu:%s"])' % (key, os, cpu))
    lines.append("")
    lines.append("alias(")
    lines.append('    name = "node",')
    lines.append("    actual = select({")
    for key in _RELEASES:
        lines.append('        ":%s": "@nodejs_%s//:node",' % (key, key))
    lines.append("    }),")
    lines.append(")")
    return "\n".join(lines) + "\n"

def _node_hub_impl(ctx):
    ctx.file("BUILD.bazel", _hub_build())

_node_hub = repository_rule(implementation = _node_hub_impl)

_node = tag_class(attrs = {"version": attr.string(default = _VERSION)})

def _node_extension_impl(ctx):
    version = _VERSION
    for module in ctx.modules:
        for tag in module.tags.node:
            version = tag.version
    for key, (release, suffix, sha256, node) in _RELEASES.items():
        _node_release(
            name = "nodejs_" + key,
            version = version,
            release = release,
            suffix = suffix,
            sha256 = sha256,
            node = node,
        )
    _node_hub(name = "nodejs")

node = module_extension(
    implementation = _node_extension_impl,
    tag_classes = {"node": _node},
)
