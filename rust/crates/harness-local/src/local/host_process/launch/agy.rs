//! Resolve native relative file permissions in the workspace of this invocation.
use super::*;

pub(super) fn settings(value: &str, cwd: &Path) -> Result<OsString> {
    let bytes = if value.trim_start().starts_with('{') {
        std::borrow::Cow::Borrowed(value.as_bytes())
    } else {
        std::borrow::Cow::Owned(std::fs::read(value).map_err(|error| {
            invalid(format!(
                "step=read Antigravity session settings: {error}; recovery: acyclic host agy -- --sandbox --mode accept-edits --settings '{{}}'"
            ))
        })?)
    };
    let mut settings: serde_json::Value = serde_json::from_slice(&bytes).map_err(|error| {
        invalid(format!(
            "step=decode Antigravity session settings: {error}; recovery: acyclic host agy -- --sandbox --mode accept-edits --settings '{{}}'"
        ))
    })?;
    if let Some(permissions) = settings.get_mut("permissions") {
        for list in ["allow", "ask", "deny"] {
            let Some(rules) = permissions
                .get_mut(list)
                .and_then(serde_json::Value::as_array_mut)
            else {
                continue;
            };
            for rule in rules {
                let Some(text) = rule.as_str() else {
                    continue;
                };
                for action in ["read_file(", "write_file("] {
                    let Some(target) = text.strip_prefix(action).and_then(|s| s.strip_suffix(')'))
                    else {
                        continue;
                    };
                    // Leave native wildcard and home expansion to the host.
                    if !target.is_empty()
                        && target != "*"
                        && !target.starts_with('~')
                        && Path::new(target).is_relative()
                    {
                        if !cwd.is_absolute()
                            || Path::new(target).components().any(|component| {
                                !matches!(
                                    component,
                                    std::path::Component::Normal(_) | std::path::Component::CurDir
                                )
                            })
                        {
                            return Err(invalid(
                                "step=resolve Antigravity file permission: relative targets require an absolute admitted cwd and cannot contain '..', roots or drive prefixes; recovery: select a path within the invocation's admitted cwd",
                            ));
                        }
                        let mut path =
                            PathBuf::with_capacity(cwd.as_os_str().len() + target.len() + 1);
                        path.push(cwd);
                        // Native sandbox grants distinguish a view from view/.
                        // Parent components are refused above, never widened to an ancestor.
                        path.extend(
                            Path::new(target)
                                .components()
                                .filter(|component| *component != std::path::Component::CurDir),
                        );
                        let path = path.to_str().ok_or_else(|| {
                            invalid("step=resolve Antigravity file permission: non-Unicode workspace; recovery: acyclic host agy -- --sandbox --mode accept-edits from a Unicode workspace")
                        })?;
                        *rule = format!("{action}{path})").into();
                    }
                    break;
                }
            }
        }
    }
    serde_json::to_string(&settings)
        .map(Into::into)
        .map_err(storage)
}
