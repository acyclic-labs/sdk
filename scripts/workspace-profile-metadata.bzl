# Shared staging and cargo-free profile metadata for declared Bazel workspaces.

def stage_workspace(name, directory, inputs, files):
    sources = {source.removeprefix(directory + "/"): source for source in inputs}
    sources.update(files)
    staging = "$(@D)/" + name + ".source"
    commands = ["rm -rf '%s' && mkdir -p '%s'" % (staging, staging)]
    for relative, source in sources.items():
        parent = relative.rsplit("/", 1)[0] if "/" in relative else "."
        commands.extend([
            "mkdir -p '%s/%s'" % (staging, parent),
            "cp '$(location %s)' '%s/%s'" % (source, staging, relative),
        ])
    return (sources, staging, commands)

def workspace_profile_metadata(name, directory, inputs, files = {}, include_dependencies = False, workspace = ".", additional_inputs = [], patch_config = None, lock_file = None, universe_file = None):
    """Capture the shared workspace model as a declared build output."""
    sources, staging, commands = stage_workspace(name, directory, inputs, files)
    for source in additional_inputs:
        if source not in sources:
            fail("profile graph input is not declared: " + source)
    for source in [patch_config, lock_file, universe_file]:
        if source != None and source not in sources:
            fail("profile graph input is not declared: " + source)
    if patch_config != None and lock_file == None:
        fail("a retained patch config requires its selected lock")
    if workspace.startswith("/") or ".." in workspace.split("/"):
        fail("workspace escapes its declared source root")
    selection = "all" if include_dependencies else "workspace"
    arguments = " ".join(["'%s'" % source for source in additional_inputs])
    commands.append("'$(location @nodejs//:node)' '$(location //:scripts/workspace-profile-metadata.mjs)' '%s' %s '%s' '%s' '%s' '%s' %s > $@" % (staging, selection, workspace, patch_config or "-", lock_file or "-", universe_file or "-", arguments))
    native.genrule(
        name = name + "_metadata",
        srcs = depset(sources.values()).to_list() + ["//:scripts/workspace-profile-metadata.mjs", "//:scripts/workspace-model.mjs", "//:scripts/policy-core.mjs"],
        outs = [name + ".metadata.json"],
        tools = ["@nodejs//:node"],
        cmd = " && ".join(commands),
        visibility = ["//visibility:public"],
    )
