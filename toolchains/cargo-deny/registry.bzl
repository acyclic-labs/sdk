"""A Cargo local registry made only from locked archives and pinned index entries."""

_CRATES_IO = "registry+https://github.com/rust-lang/crates.io-index"
_HEX = "0123456789abcdef"

def _digest(value):
    return len(value) == 64 and all([c in _HEX for c in value.elems()])

def _index_path(name):
    name = name.lower()
    if len(name) == 1:
        return "1/" + name
    if len(name) == 2:
        return "2/" + name
    if len(name) == 3:
        return "3/" + name[0] + "/" + name
    return name[:2] + "/" + name[2:4] + "/" + name

def _lock_packages(text):
    packages = []
    package = None
    for raw in text.splitlines():
        line = raw.strip()
        if line == "[[package]]":
            package = {}
            packages.append(package)
        elif package != None and " = " in line:
            key, value = line.split(" = ", 1)
            if key in ["name", "version", "source", "checksum"]:
                if key in package:
                    fail("duplicate Cargo.lock package field " + key)
                package[key] = json.decode(value)
    return packages

def _flush(pending):
    for download in pending:
        download.wait()
    pending.clear()

def _registry_impl(ctx):
    pinned = json.decode(ctx.read(ctx.attr.index_lock))
    if len(pinned.get("commit", "")) != 40:
        fail("invalid cargo-deny index commit")
    packages = _lock_packages(ctx.read(ctx.attr.cargo_lock))
    pending = []
    names = {}
    for package in packages:
        source = package.get("source")
        if source == None:
            continue
        if source != _CRATES_IO:
            fail("cargo-deny offline registry cannot resolve locked source " + source)
        name, version, checksum = package.get("name", ""), package.get("version", ""), package.get("checksum", "")
        if not name or not version or not _digest(checksum):
            fail("invalid registry package in Cargo.lock: " + repr(package))
        names[name] = True
        archive = "%s-%s.crate" % (name, version)
        pending.append(ctx.download(
            url = "https://static.crates.io/crates/%s/%s" % (name, archive),
            output = archive,
            sha256 = checksum,
            block = False,
        ))
        if len(pending) >= 16:
            _flush(pending)
    for name in sorted(names):
        digest = pinned.get("packages", {}).get(name, "")
        if not _digest(digest):
            fail("missing pinned index entry for %s; run scripts/pin-cargo-deny-index.mjs" % name)
        index_path = _index_path(name)
        pending.append(ctx.download(
            url = "https://raw.githubusercontent.com/rust-lang/crates.io-index/%s/%s" % (pinned["commit"], index_path),
            output = "index/" + index_path,
            sha256 = digest,
            block = False,
        ))
        if len(pending) >= 16:
            _flush(pending)
    _flush(pending)
    ctx.file("index/config.json", json.encode({"dl": "https://static.crates.io/crates", "api": "https://crates.io"}))
    ctx.file("BUILD.bazel", "filegroup(name = \"data\", srcs = glob([\"index/**\", \"*.crate\"]), visibility = [\"//visibility:public\"])\n")

registry = repository_rule(
    implementation = _registry_impl,
    attrs = {
        "cargo_lock": attr.label(mandatory = True, allow_single_file = True),
        "index_lock": attr.label(mandatory = True, allow_single_file = True),
    },
)
