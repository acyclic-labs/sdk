//! Compiles one crate from two checkouts, as a sandbox stages each: the crate source copied and
//! clippy.toml a symlink into that checkout. Arguments: process wrapper, clippy-driver, sysroot,
//! crate source, clippy.toml, scratch output directory.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::{env, fs, io};

/// How clippy-driver is handed the configuration.
#[derive(Clone, Copy)]
enum Conf {
    /// The process wrapper stages it, as every compile action does.
    Staged,
    /// clippy reads the symlink in place: the class the staging removes.
    InPlace,
}

struct Inputs {
    wrapper: PathBuf,
    driver: PathBuf,
    sysroot: PathBuf,
    source: PathBuf,
    conf: PathBuf,
    out: PathBuf,
}

#[cfg(unix)]
fn symlink(target: &Path, link: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn symlink(target: &Path, link: &Path) -> io::Result<()> {
    std::os::windows::fs::symlink_file(target, link)
}

fn context<T>(what: &str, result: io::Result<T>) -> Result<T, String> {
    result.map_err(|error| format!("{what}: {error}"))
}

/// The metadata of the probe crate compiled in a fresh checkout named `name`.
fn compile(inputs: &Inputs, name: &str, conf: Conf) -> Result<Vec<u8>, String> {
    let checkout = inputs.out.join(format!("{name}-checkout"));
    let exec = inputs.out.join(format!("{name}-exec"));
    context("create checkout", fs::create_dir_all(&checkout))?;
    context("create execution root", fs::create_dir_all(&exec))?;
    let conf_file = checkout.join("clippy.toml");
    context("copy clippy.toml", fs::copy(&inputs.conf, &conf_file))?;
    context(
        "copy source",
        fs::copy(&inputs.source, exec.join("probe.rs")),
    )?;
    context(
        "link clippy.toml",
        symlink(&conf_file, &exec.join("clippy.toml")),
    )?;

    let mut command = Command::new(&inputs.wrapper);
    command.current_dir(&exec).args(["--subst", "pwd=${pwd}"]);
    match conf {
        Conf::Staged => command.args(["--clippy-conf", "clippy.toml"]),
        Conf::InPlace => command.env("CLIPPY_CONF_DIR", "."),
    };
    command
        .arg("--")
        .arg(&inputs.driver)
        .args([
            "probe.rs",
            "--crate-type=rlib",
            "--crate-name=probe",
            "--edition=2024",
            "--emit=metadata",
            "--out-dir=.",
            "--remap-path-prefix=${pwd}=.",
        ])
        .arg(format!("--sysroot={}", inputs.sysroot.display()));
    let status = context("run the process wrapper", command.status())?;
    if !status.success() {
        return Err(format!("compiling in {} failed: {status}", exec.display()));
    }
    let metadata = context("read metadata", fs::read(exec.join("libprobe.rmeta")))?;
    // The output directory holds only what the check leaves on success: nothing.
    context("remove execution root", fs::remove_dir_all(&exec))?;
    context("remove checkout", fs::remove_dir_all(&checkout))?;
    Ok(metadata)
}

fn main() -> Result<(), String> {
    let args: Vec<PathBuf> = env::args_os().skip(1).map(PathBuf::from).collect();
    let [wrapper, driver, sysroot, source, conf, out] = <[PathBuf; 6]>::try_from(args)
        .map_err(|args| format!("expected 6 arguments, got {}", args.len()))?;
    let absolute = |path: PathBuf| context("resolve input", std::path::absolute(path));
    let inputs = Inputs {
        wrapper: absolute(wrapper)?,
        driver: absolute(driver)?,
        sysroot: absolute(sysroot)?,
        source: absolute(source)?,
        conf: absolute(conf)?,
        out: absolute(out)?,
    };
    if compile(&inputs, "staged-a", Conf::Staged)? != compile(&inputs, "staged-b", Conf::Staged)? {
        return Err("one crate compiled from two checkouts has two crate hashes".to_owned());
    }
    let in_place_a = compile(&inputs, "in-place-a", Conf::InPlace)?;
    if in_place_a == compile(&inputs, "in-place-b", Conf::InPlace)? {
        return Err(
            "clippy reading clippy.toml in place no longer reaches the checkout path: \
                    delete the staging and this check"
                .to_owned(),
        );
    }
    Ok(())
}
