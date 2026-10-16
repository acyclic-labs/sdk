use super::*;

pub(super) fn canonical_repository_root(path: &Path) -> Result<PathBuf, Error> {
    reject_reparse_ancestors(path)?;
    let root = path.canonicalize().map_err(|error| {
        Error::Invalid(format!(
            "cannot resolve rustdoc source root {}: {error}",
            path.display()
        ))
    })?;
    reject_reparse_ancestors(&root)?;
    Ok(root)
}

pub(super) fn attest_generated_sources(
    repository_root: &Path,
    sources: &[GeneratedSource],
) -> Result<HashMap<PathBuf, GeneratedSource>, Error> {
    let mut attested = HashMap::new();
    let mut logical_paths = HashMap::<String, String>::new();
    let mut physical_paths = HashSet::new();
    for source in sources {
        let logical_path = normalize_generated_logical_path(&source.logical_path)?;
        let logical_key = if cfg!(windows) {
            logical_path.to_ascii_lowercase()
        } else {
            logical_path.clone()
        };
        validate_generated_sha256(&source.sha256)?;
        let digest = source
            .sha256
            .trim_start_matches("sha256:")
            .to_ascii_lowercase();
        if let Some(previous) = logical_paths.insert(logical_key, digest.clone()) {
            if previous != digest {
                return Err(Error::Invalid(format!(
                    "generated source logical path has conflicting digests: {logical_path}"
                )));
            }
        }
        let repository_path = repository_root.join(&logical_path);
        reject_reparse_ancestors(&repository_path)?;
        match fs::symlink_metadata(&repository_path) {
            Ok(_) => {
                return Err(Error::Invalid(format!(
                    "generated source logical path collides with a repository path: {logical_path}"
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(Error::Invalid(format!(
                    "cannot inspect generated source logical path {logical_path}: {error}"
                )));
            }
        }
        reject_reparse_ancestors(&source.physical_path)?;
        let physical_path = source.physical_path.canonicalize().map_err(|error| {
            Error::Invalid(format!(
                "cannot resolve generated source {}: {error}",
                source.physical_path.display()
            ))
        })?;
        reject_reparse_ancestors(&physical_path)?;
        let metadata = fs::symlink_metadata(&physical_path)?;
        if !metadata.is_file() {
            return Err(Error::Invalid(format!(
                "generated source is not a regular file: {}",
                physical_path.display()
            )));
        }
        let physical_key = if cfg!(windows) {
            physical_path.to_string_lossy().to_ascii_lowercase()
        } else {
            physical_path.to_string_lossy().into_owned()
        };
        if !physical_paths.insert(physical_key) {
            return Err(Error::Invalid(format!(
                "generated source physical path is duplicated: {}",
                physical_path.display()
            )));
        }
        let bytes = fs::read(&physical_path)?;
        validate_generated_sha256(&source.sha256)?;
        if !generated_sha256_matches(&source.sha256, &bytes) {
            return Err(Error::Invalid(format!(
                "generated source digest does not match {}",
                physical_path.display()
            )));
        }
        attested.insert(
            physical_path.clone(),
            GeneratedSource {
                physical_path,
                logical_path: PathBuf::from(logical_path),
                sha256: source.sha256.clone(),
            },
        );
    }
    Ok(attested)
}

pub(super) fn normalize_generated_logical_path(path: &Path) -> Result<String, Error> {
    let raw = path.to_string_lossy().replace('\\', "/");
    if raw.is_empty() || raw.contains(':') {
        return Err(Error::Invalid(format!(
            "generated source logical path must be relative: {raw:?}"
        )));
    }
    let mut components = Vec::new();
    for component in Path::new(&raw).components() {
        match component {
            Component::Normal(component) => {
                let component = component.to_string_lossy().into_owned();
                if component.is_empty()
                    || !component.bytes().all(|byte| {
                        byte.is_ascii()
                            && !byte.is_ascii_control()
                            && !matches!(byte, b'<' | b'>' | b'"' | b'|' | b'?' | b'*')
                    })
                    || component.ends_with('.')
                    || component.ends_with(' ')
                    || is_windows_reserved_segment(&component)
                {
                    return Err(Error::Invalid(format!(
                        "generated source logical path contains a non-portable segment: {raw:?}"
                    )));
                }
                components.push(component)
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(Error::Invalid(format!(
                    "generated source logical path escapes its bundle root: {raw:?}"
                )));
            }
        }
    }
    if components.is_empty() {
        return Err(Error::Invalid(format!(
            "generated source logical path must not be empty: {raw:?}"
        )));
    }
    Ok(components.join("/"))
}

pub(super) fn normalized_rustdoc_digest_bytes(
    bytes: &std::rc::Rc<[u8]>,
    repository_root: &Path,
) -> Result<std::rc::Rc<[u8]>, Error> {
    crate::render_cache::normalized(bytes, repository_root, || {
        let mut value = (*crate::render_cache::json(bytes)?).clone();
        normalize_rustdoc_filenames(&mut value, repository_root);
        // External-crate load paths describe the physical Cargo output, not API
        // content. Omit only these paths from the digest copy; retain the original
        // typed Rustdoc JSON and receipts for artifact/provenance verification.
        if let Some(crates) = value
            .get_mut("external_crates")
            .and_then(serde_json::Value::as_object_mut)
        {
            for external in crates.values_mut() {
                if let Some(record) = external.as_object_mut() {
                    record.remove("path");
                }
            }
        }
        Ok(serde_json::to_vec(&value)?)
    })
}

pub(super) fn normalize_rustdoc_filenames(value: &mut serde_json::Value, repository_root: &Path) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map {
                if key == "filename" {
                    if let serde_json::Value::String(filename) = child {
                        *filename = stable_rustdoc_filename(filename, repository_root);
                        continue;
                    }
                }
                normalize_rustdoc_filenames(child, repository_root);
            }
        }
        serde_json::Value::Array(values) => {
            for child in values {
                normalize_rustdoc_filenames(child, repository_root);
            }
        }
        _ => {}
    }
}

pub(super) fn stable_rustdoc_filename(filename: &str, repository_root: &Path) -> String {
    crate::path_identity::stable_filename(filename, &repository_root.to_string_lossy())
}

pub(super) fn validate_generated_sha256(value: &str) -> Result<(), Error> {
    let digest = value.strip_prefix("sha256:").unwrap_or(value);
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::Invalid(format!(
            "generated source digest must be a SHA-256 value: {value:?}"
        )));
    }
    Ok(())
}

pub(super) fn generated_sha256_matches(expected: &str, bytes: &[u8]) -> bool {
    expected
        .strip_prefix("sha256:")
        .unwrap_or(expected)
        .eq_ignore_ascii_case(&sha256_hex(bytes))
}

// Scoped to one family projection. Resolve each filename once, then repeat
// every check before returning the family; no filesystem result survives it.
// Keep raw spellings: Path equality aliases file.rs and file.rs/, unlike I/O.
pub(super) fn cached_source_span(
    repository_root: &Path,
    rustdoc_source_root: &Path,
    generated_sources: &HashMap<PathBuf, GeneratedSource>,
    paths: &mut HashMap<std::ffi::OsString, String>,
    span: &rustdoc_types::Span,
) -> Result<SourceSpan, Error> {
    let path = match paths.get(span.filename.as_os_str()) {
        Some(path) => path.clone(),
        None => {
            let resolved = source_span_from_base(
                repository_root,
                rustdoc_source_root,
                generated_sources,
                span,
            )?;
            paths.insert(span.filename.as_os_str().to_owned(), resolved.path.clone());
            resolved.path
        }
    };
    Ok(SourceSpan {
        path,
        begin_line: span.begin.0,
        begin_column: span.begin.1,
        end_line: span.end.0,
        end_column: span.end.1,
    })
}

pub(super) fn validate_source_paths(
    repository_root: &Path,
    rustdoc_source_root: &Path,
    generated_sources: &HashMap<PathBuf, GeneratedSource>,
    paths: &HashMap<std::ffi::OsString, String>,
) -> Result<(), Error> {
    for (filename, expected) in paths {
        let span = rustdoc_types::Span {
            filename: PathBuf::from(filename),
            begin: (0, 0),
            end: (0, 0),
        };
        let actual = source_span_from_base(
            repository_root,
            rustdoc_source_root,
            generated_sources,
            &span,
        )?;
        if actual.path != *expected {
            return Err(Error::Invalid(format!(
                "Rustdoc source mapping changed during projection: {}",
                Path::new(filename).display()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
thread_local! { pub(super) static SOURCE_PATH_CHECKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

pub(super) fn source_span_from_base(
    repository_root: &Path,
    rustdoc_source_root: &Path,
    generated_sources: &HashMap<PathBuf, GeneratedSource>,
    span: &rustdoc_types::Span,
) -> Result<SourceSpan, Error> {
    let mut scoped_span = span.clone();
    if !scoped_span.filename.is_absolute() {
        scoped_span.filename = rustdoc_source_root.join(&scoped_span.filename);
    }
    source_span_at_root(repository_root, generated_sources, &scoped_span)
}

pub(super) fn source_span_at_root(
    repository_root: &Path,
    generated_sources: &HashMap<PathBuf, GeneratedSource>,
    span: &rustdoc_types::Span,
) -> Result<SourceSpan, Error> {
    #[cfg(test)]
    SOURCE_PATH_CHECKS.set(SOURCE_PATH_CHECKS.get() + 1);
    let source_path = if span.filename.is_absolute() {
        span.filename.clone()
    } else {
        repository_root.join(&span.filename)
    };
    reject_reparse_ancestors(&source_path)?;
    let source_path = match source_path.canonicalize() {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let basename = span.filename.file_name().ok_or_else(|| {
                Error::Invalid(format!(
                    "rustdoc source span has no filename: {}",
                    span.filename.display()
                ))
            })?;
            let matches = generated_sources
                .values()
                .filter(|generated| generated.physical_path.file_name() == Some(basename))
                .collect::<Vec<_>>();
            let [generated] = matches.as_slice() else {
                let reason = if matches.is_empty() {
                    "no attested generated source matches"
                } else {
                    "attested generated source basename is ambiguous"
                };
                return Err(Error::Invalid(format!(
                    "cannot resolve rustdoc source span {}: {reason}",
                    span.filename.display()
                )));
            };
            return Ok(SourceSpan {
                path: normalize_path(&generated.logical_path),
                begin_line: span.begin.0,
                begin_column: span.begin.1,
                end_line: span.end.0,
                end_column: span.end.1,
            });
        }
        Err(error) => {
            return Err(Error::Invalid(format!(
                "cannot resolve rustdoc source span {}: {error}",
                span.filename.display()
            )))
        }
    };
    let path = if let Some(generated) = generated_sources.get(&source_path) {
        normalize_path(&generated.logical_path)
    } else if let Ok(relative) = source_path.strip_prefix(repository_root) {
        normalize_path(relative)
    } else {
        return Err(Error::Invalid(format!(
            "rustdoc source span escapes its source root without an attested generated source: {}",
            span.filename.display()
        )));
    };
    Ok(SourceSpan {
        path,
        begin_line: span.begin.0,
        begin_column: span.begin.1,
        end_line: span.end.0,
        end_column: span.end.1,
    })
}

pub(super) fn item_name(item: &Item) -> Option<String> {
    item.name.clone().or_else(|| match &item.inner {
        ItemEnum::Use(use_) => Some(use_.name.clone()),
        _ => None,
    })
}

#[cfg(test)]
#[path = "tests/source_paths.rs"]
mod tests;
