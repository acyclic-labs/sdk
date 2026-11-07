//! Native extension publication over the same source fence and process pump.
use super::*;

const WORKSPACE_IMPORT: &str =
    "import { workspace, nativeSession } from \"../shared/workspace.mjs\";";

fn compose(package: &Path, source: &str, executable: &Path, recovery: &str) -> Result<String> {
    let read = |path: &str| {
        fs::read_to_string(package.join(path)).map_err(|error| {
            format!("step=admit native coordination asset {path}: {error}; {recovery}")
        })
    };
    let asset = source;
    let mut source = read(asset)?;
    for (import, path) in [
        (
            "import { McpClient } from \"../shared/mcp-client.mjs\";",
            "shared/mcp-client.mjs",
        ),
        (WORKSPACE_IMPORT, "shared/workspace.mjs"),
    ] {
        if asset == "opencode/index.mjs" && path == "shared/mcp-client.mjs" {
            continue;
        }
        let (before, after) = source.split_once(import).ok_or_else(|| {
            format!("step=admit native coordination closure {asset}: required import {path} absent; {recovery}")
        })?;
        source = format!("{before}{}{after}", read(path)?);
    }
    Ok(source.replace(
        "\"__ACYCLIC_EXECUTABLE__\"",
        &serde_json::to_string(executable)?,
    ))
}

pub(super) fn install_extension(
    host: &str,
    path: &Path,
    package: &Path,
    executable: &Path,
    source: &str,
    install: bool,
) -> Result<()> {
    let recovery = format!(
        "recovery argv: {}",
        serde_json::to_string(&(
            executable,
            "install",
            host,
            "--plugin-root",
            package,
            "--settings",
            path,
        ))?
    );
    let original = optional(path)?;
    if original
        .as_ref()
        .is_some_and(|bytes| !bytes.starts_with(b"// Acyclic native workspace shim"))
    {
        return Err(
            format!("step=admit {host} extension: foreign file retained; {recovery}").into(),
        );
    }
    if install {
        let source = compose(package, source, executable, &recovery)?;
        write_config(host, path, source.as_bytes(), original.as_deref())?;
    } else if original.is_some() {
        fs::remove_file(path)
            .map_err(|error| format!("step=remove {host} extension: {error}; {recovery}"))?;
    }
    Ok(())
}

pub(super) fn install_opencode(
    args: &[String],
    package: &Path,
    executable: &Path,
    install: bool,
) -> Result<()> {
    let path = option(args, "--settings").map_or_else(
        || {
            let directory = env::var_os("OPENCODE_CONFIG_DIR")
                .filter(|path| !path.is_empty())
                .map(PathBuf::from)
                .map_or_else(
                    || {
                        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                            host_directory("XDG_CONFIG_HOME", ".config")?.join("opencode"),
                        )
                    },
                    Ok,
                )?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(directory.join("plugins/acyclic.mjs"))
        },
        Ok,
    )?;
    install_extension(
        "opencode",
        &path,
        package,
        executable,
        "opencode/index.mjs",
        install,
    )
}

pub(super) fn install_agy(
    args: &[String],
    package: &Path,
    executable: &Path,
    install: bool,
) -> Result<()> {
    if option(args, "--settings").is_some() {
        return Err("step=select Antigravity installation: --settings is not a plugin profile; recovery=acyclic install agy --plugin-root PATH using the native HOME profile".into());
    }
    if !install {
        native_output("agy", &["plugin", "uninstall", "acyclic"])?;
        return Ok(());
    }
    let recovery = format!(
        "recovery argv: {}",
        serde_json::to_string(&(executable, "install", "agy", "--plugin-root", package,))?
    );
    // The native manager copies this complete immutable closure into its own
    // profile. Only installation needs staging; no product operation pays it.
    let stage = tempfile::Builder::new()
        .prefix("acyclic-agy-")
        .tempdir()
        .map_err(|error| format!("step=stage Antigravity plugin: {error}; {recovery}"))?;
    // Native hooks run in the installed plugin directory. A relative executable
    // keeps the complete closure valid after the native manager copies it, and
    // needs no shell quoting even when the profile path contains spaces.
    let member = if cfg!(windows) {
        "acyclic.exe"
    } else {
        "acyclic"
    };
    let directory = stage.path().join("bin");
    fs::create_dir(&directory).map_err(|error| {
        format!("step=stage Antigravity executable directory: {error}; {recovery}")
    })?;
    fs::copy(executable, directory.join(member))
        .map_err(|error| format!("step=stage Antigravity executable: {error}; {recovery}"))?;
    let command = if cfg!(windows) {
        "bin\\acyclic.exe"
    } else {
        "./bin/acyclic"
    };
    let command = serde_json::to_string(command)?;
    for name in ["plugin.json", "hooks.json", "mcp_config.json"] {
        let source = fs::read_to_string(package.join("agy").join(name))
            .map_err(|error| format!("step=admit Antigravity asset {name}: {error}; {recovery}"))?;
        let source = source.replace(
            "\"__ACYCLIC_EXECUTABLE__\"",
            &serde_json::to_string(executable)?,
        );
        let source = source.replace("__ACYCLIC_COMMAND__", command.trim_matches('"'));
        fs::write(stage.path().join(name), source)
            .map_err(|error| format!("step=stage Antigravity asset {name}: {error}; {recovery}"))?;
    }
    let path = stage.path().to_str().ok_or_else(|| {
        format!("step=admit Antigravity plugin path: non-Unicode path; {recovery}")
    })?;
    native_output("agy", &["plugin", "validate", path, "--strict", "--json"])?;
    native_output("agy", &["plugin", "install", path])?;
    Ok(())
}
