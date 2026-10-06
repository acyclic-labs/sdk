use sdk_docs::{build_data, write_bundle, BuildInput, Channel, Error};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::collections::HashSet;

fn main() {
    if let Err(error) = run() {
        eprintln!("sdk-docs: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Error> {
    let args: Vec<String> = env::args().skip(1).collect();
    validate_arguments(&args)?;
    let rustdoc = required_path(&args, "--rustdoc-json")?;
    let output = required_path(&args, "--output-dir")?;
    let repository = required_path(&args, "--repo-root")?;
    let version = required_value(&args, "--version")?;
    let revision = required_value(&args, "--revision")?;
    let channel = match optional_value(&args, "--channel")?
        .as_deref()
        .unwrap_or("release")
    {
        "release" => Channel::Release,
        "preview" => Channel::Preview,
        value => {
            return Err(Error::Invalid(format!(
                "unsupported channel {value:?}; use release or preview"
            )))
        }
    };
    let source_state =
        optional_value(&args, "--source-state")?.unwrap_or_else(|| "captured-snapshot".into());
    let mark_latest = args.iter().any(|arg| arg == "--latest");
    let rustdoc_files = discover_rustdoc_files(&rustdoc)?;
    let input = BuildInput {
        version,
        channel,
        revision,
        source_state,
        source_sha256: None,
        repository_root: repository,
        rustdoc_files,
        mark_latest,
    };
    let data = build_data(&input)?;
    write_bundle(&data, &output, mark_latest)
}

fn validate_arguments(args: &[String]) -> Result<(), Error> {
    let value_flags = [
        "--rustdoc-json",
        "--output-dir",
        "--repo-root",
        "--version",
        "--revision",
        "--channel",
        "--source-state",
    ];
    let mut seen = HashSet::new();
    let mut index = 0;
    while index < args.len() {
        let flag = &args[index];
        if !seen.insert(flag.as_str()) {
            return Err(Error::Invalid(format!("duplicate argument {flag}")));
        }
        if flag == "--latest" {
            index += 1;
        } else if value_flags.contains(&flag.as_str()) {
            if index + 1 >= args.len() || args[index + 1].starts_with("--") {
                return Err(Error::Invalid(format!("missing value after {flag}")));
            }
            index += 2;
        } else {
            return Err(Error::Invalid(format!("unknown argument {flag}")));
        }
    }
    Ok(())
}

fn required_path(args: &[String], name: &str) -> Result<PathBuf, Error> {
    required_value(args, name).map(PathBuf::from)
}
fn required_value(args: &[String], name: &str) -> Result<String, Error> {
    optional_value(args, name)?
        .ok_or_else(|| Error::Invalid(format!("missing required argument {name} VALUE")))
}
fn optional_value(args: &[String], name: &str) -> Result<Option<String>, Error> {
    let Some(index) = args.iter().position(|arg| arg == name) else {
        return Ok(None);
    };
    let value = args
        .get(index + 1)
        .ok_or_else(|| Error::Invalid(format!("missing value after {name}")))?;
    if value.starts_with("--") {
        return Err(Error::Invalid(format!("missing value after {name}")));
    }
    Ok(Some(value.clone()))
}

fn discover_rustdoc_files(path: &Path) -> Result<Vec<PathBuf>, Error> {
    if let Ok(metadata) = fs::symlink_metadata(path) {
        if is_reparse_or_symlink(&metadata) {
            return Err(Error::Invalid(format!(
                "rustdoc input is a reparse point or symlink: {}",
                path.display()
            )));
        }
    }
    let mut files = Vec::new();
    if path.is_file() {
        files.push(path.to_owned());
    } else if path.is_dir() {
        collect_json(path, &mut files)?;
    } else {
        return Err(Error::Invalid(format!(
            "rustdoc input does not exist: {}",
            path.display()
        )));
    }
    files.sort();
    if files.is_empty() {
        return Err(Error::Invalid(format!(
            "no JSON files found under {}",
            path.display()
        )));
    }
    Ok(files)
}
fn collect_json(root: &Path, files: &mut Vec<PathBuf>) -> Result<(), Error> {
    for entry in fs::read_dir(root)? {
        let path = entry?.path();
        let metadata = fs::symlink_metadata(&path)?;
        if is_reparse_or_symlink(&metadata) {
            return Err(Error::Invalid(format!(
                "rustdoc input contains a reparse point or symlink: {}",
                path.display()
            )));
        }
        if metadata.is_dir() {
            collect_json(&path, files)?;
        } else if metadata.is_file()
            && path
            .extension()
            .is_some_and(|extension| extension == "json")
        {
            files.push(path);
        }
    }
    Ok(())
}

fn is_reparse_or_symlink(metadata: &fs::Metadata) -> bool {
    if metadata.file_type().is_symlink() {
        return true;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        return metadata.file_attributes() & 0x400 != 0;
    }
    #[cfg(not(windows))]
    {
        false
    }
}
