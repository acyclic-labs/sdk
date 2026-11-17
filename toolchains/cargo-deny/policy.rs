use semver::{Version, VersionReq};
use serde_json::Value;
use std::collections::BTreeSet;

struct Expression<'a> {
    tokens: Vec<&'a str>,
    position: usize,
    allowed: &'a BTreeSet<String>,
}

impl<'a> Expression<'a> {
    fn atom(&mut self) -> Result<bool, String> {
        let token = self
            .tokens
            .get(self.position)
            .copied()
            .ok_or("missing license")?;
        self.position += 1;
        if token == "(" {
            let value = self.or()?;
            if self.tokens.get(self.position) != Some(&")") {
                return Err("missing closing parenthesis".into());
            }
            self.position += 1;
            return Ok(value);
        }
        if matches!(token, ")" | "AND" | "OR" | "WITH") {
            return Err(format!("unexpected license token {token}"));
        }
        let mut license = token.to_owned();
        if self.tokens.get(self.position) == Some(&"WITH") {
            self.position += 1;
            let exception = self
                .tokens
                .get(self.position)
                .copied()
                .ok_or("missing license exception")?;
            if matches!(exception, "(" | ")" | "AND" | "OR" | "WITH") {
                return Err("invalid license exception".into());
            }
            self.position += 1;
            license.push_str(" WITH ");
            license.push_str(exception);
        }
        Ok(self.allowed.contains(&license))
    }
    fn and(&mut self) -> Result<bool, String> {
        let mut result = self.atom()?;
        while self.tokens.get(self.position) == Some(&"AND") {
            self.position += 1;
            let next = self.atom()?;
            result &= next;
        }
        Ok(result)
    }
    fn or(&mut self) -> Result<bool, String> {
        let mut result = self.and()?;
        while self.tokens.get(self.position) == Some(&"OR") {
            self.position += 1;
            let next = self.and()?;
            result |= next;
        }
        Ok(result)
    }
}

fn license_allowed(expression: &str, allowed: &BTreeSet<String>) -> Result<bool, String> {
    // Older Cargo manifests used a slash for dual licensing; Cargo interprets it as OR.
    let padded = expression
        .replace('/', " OR ")
        .replace('(', " ( ")
        .replace(')', " ) ");
    let mut parser = Expression {
        tokens: padded.split_whitespace().collect(),
        position: 0,
        allowed,
    };
    let result = parser.or()?;
    if parser.position != parser.tokens.len() {
        return Err("unexpected trailing license tokens".into());
    }
    Ok(result)
}

fn strings(value: &toml::Value) -> Result<BTreeSet<String>, String> {
    value
        .as_array()
        .ok_or("expected array")?
        .iter()
        .map(|v| {
            v.as_str()
                .map(str::to_owned)
                .ok_or("expected string".into())
        })
        .collect()
}

pub fn check(entry: &Value, registry: &Value, policy: &toml::Value) -> Result<Vec<String>, String> {
    let name = entry["name"].as_str().ok_or("missing crate name")?;
    let version = Version::parse(entry["version"].as_str().ok_or("missing crate version")?)
        .map_err(|e| e.to_string())?;
    let Some(repository) = entry.get("repository").filter(|value| !value.is_null()) else {
        if name == "direct-cargo-bazel-deps" {
            return Ok(vec![]);
        }
        return Err(format!("{name} {version}: missing source"));
    };
    let sources = policy.get("sources").ok_or("missing sources policy")?;
    if sources
        .get("unknown-registry")
        .and_then(toml::Value::as_str)
        != Some("deny")
        || sources.get("unknown-git").and_then(toml::Value::as_str) != Some("deny")
    {
        return Err("unsupported source policy".into());
    }
    let http = repository
        .get("Http")
        .ok_or_else(|| format!("{name} {version}: unapproved source {repository}"))?;
    let url = http["url"].as_str().ok_or("missing registry URL")?;
    let expected_url = format!("https://static.crates.io/crates/{name}/{version}/download");
    if url != expected_url {
        return Err(format!("{name} {version}: unapproved registry URL {url}"));
    }
    if policy
        .get("advisories")
        .and_then(|v| v.get("yanked"))
        .and_then(toml::Value::as_str)
        != Some("deny")
    {
        return Err("unsupported yanked policy".into());
    }
    let record = registry["crates"]
        .get(name)
        .and_then(|v| v.get(version.to_string()))
        .ok_or_else(|| format!("{name} {version}: missing pinned registry record"))?;
    if record["checksum"] != http["sha256"] {
        return Err(format!(
            "{name} {version}: pinned registry checksum mismatch"
        ));
    }
    let mut failures = Vec::new();
    if record["yanked"].as_bool().ok_or("missing yanked status")? {
        failures.push(format!("{name} {version}: yanked"));
    }
    let licenses = policy.get("licenses").ok_or("missing license policy")?;
    let mut allowed = strings(licenses.get("allow").ok_or("missing allowed licenses")?)?;
    for exception in licenses
        .get("exceptions")
        .and_then(toml::Value::as_array)
        .into_iter()
        .flatten()
    {
        if exception.get("name").and_then(toml::Value::as_str) == Some(name) {
            let req = VersionReq::parse(
                exception
                    .get("version")
                    .and_then(toml::Value::as_str)
                    .ok_or("exception version")?,
            )
            .map_err(|e| e.to_string())?;
            if req.matches(&version) {
                allowed.extend(strings(
                    exception.get("allow").ok_or("exception licenses")?,
                )?);
            }
        }
    }
    let license = entry["license"]
        .as_str()
        .ok_or_else(|| format!("{name} {version}: no license expression"))?;
    if !license_allowed(license, &allowed)
        .map_err(|error| format!("{name} {version}: {license}: {error}"))?
    {
        failures.push(format!("{name} {version}: disallowed license {license}"));
    }
    let bans = policy.get("bans").ok_or("missing bans policy")?;
    for ban in bans
        .get("deny")
        .and_then(toml::Value::as_array)
        .into_iter()
        .flatten()
    {
        if ban.get("crate").and_then(toml::Value::as_str) == Some(name) {
            failures.push(format!("{name} {version}: banned dependency"));
        }
    }
    Ok(failures)
}

pub fn check_manifest(file: &str, text: &str, policy: &toml::Value) -> Result<Vec<String>, String> {
    let manifest: toml::Value = toml::from_str(text).map_err(|e| format!("{file}: {e}"))?;
    let bans = policy.get("bans").ok_or("missing bans policy")?;
    if bans.get("wildcards").and_then(toml::Value::as_str) != Some("deny") {
        return Err("unsupported wildcard policy".into());
    }
    let allow_paths = bans
        .get("allow-wildcard-paths")
        .and_then(toml::Value::as_bool)
        .unwrap_or(false);
    let private = manifest
        .get("package")
        .and_then(|p| p.get("publish"))
        .is_some_and(|p| p.as_bool() == Some(false) || p.as_array().is_some_and(Vec::is_empty));
    let mut failures = Vec::new();
    fn visit(
        value: &toml::Value,
        file: &str,
        private: bool,
        allow_paths: bool,
        failures: &mut Vec<String>,
    ) {
        let Some(table) = value.as_table() else {
            return;
        };
        for (key, value) in table {
            if matches!(
                key.as_str(),
                "dependencies" | "dev-dependencies" | "build-dependencies"
            ) {
                if let Some(deps) = value.as_table() {
                    for (name, dep) in deps {
                        let requirement = dep
                            .as_str()
                            .or_else(|| dep.get("version").and_then(toml::Value::as_str));
                        let wildcard = requirement.is_some_and(|v| {
                            v.contains('*') || v.split('.').any(|part| part == "x" || part == "X")
                        });
                        let path_only = dep.get("path").and_then(toml::Value::as_str).is_some();
                        if wildcard
                            && !(allow_paths && path_only && (private || key == "dev-dependencies"))
                        {
                            failures.push(format!("{file}: wildcard dependency {name}"));
                        }
                    }
                }
            } else {
                visit(value, file, private, allow_paths, failures);
            }
        }
    }
    visit(&manifest, file, private, allow_paths, &mut failures);
    Ok(failures)
}

#[test]
fn license_expressions_enforce_and_or_and_exceptions() {
    let allowed = ["MIT", "Apache-2.0 WITH LLVM-exception"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    assert!(license_allowed("MIT OR GPL-3.0-only", &allowed).unwrap());
    assert!(license_allowed("GPL-3.0-only / MIT", &allowed).unwrap());
    assert!(!license_allowed("MIT AND GPL-3.0-only", &allowed).unwrap());
    assert!(license_allowed("Apache-2.0 WITH LLVM-exception", &allowed).unwrap());
    assert!(license_allowed("MIT OR (GPL-3.0-only AND MIT)", &allowed).unwrap());
    assert!(license_allowed("MIT OR", &allowed).is_err());
}

#[test]
fn wildcard_paths_are_restricted_to_private_or_dev_dependencies() {
    let policy: toml::Value =
        toml::from_str("[bans]\nwildcards='deny'\nallow-wildcard-paths=true\n").unwrap();
    let manifest = "[package]\nname='public'\n[dependencies]\na={version='*',path='../a'}\n[dev-dependencies]\nb={version='*',path='../b'}\nc='*'\n";
    let failures = check_manifest("Cargo.toml", manifest, &policy).unwrap();
    assert_eq!(failures.len(), 2);
    assert!(failures.iter().any(|f| f.ends_with("dependency a")));
    assert!(failures.iter().any(|f| f.ends_with("dependency c")));
}
