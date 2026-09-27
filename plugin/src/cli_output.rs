//! CLI response rendering.

use super::*;

pub(crate) fn print_cli_response(response: Value) -> Result<i32, String> {
    if let Ok(output) = serde_json::from_value::<GitCommandOutput>(response.clone()) {
        return print_git_output(output);
    }
    if let Some(agents) = response.get("agents").and_then(Value::as_array) {
        for agent in agents {
            let reference = agent
                .get("ref")
                .and_then(Value::as_str)
                .unwrap_or("agents/?");
            let state = agent
                .get("state")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let parent = agent.get("parent").and_then(Value::as_str).unwrap_or("?");
            let depth = agent
                .get("depth")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let leases = agent
                .get("activeLeases")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let conflicts = agent
                .get("conflicts")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let work = agent
                .get("workRemaining")
                .and_then(Value::as_u64)
                .map_or_else(|| "busy".to_owned(), |work| work.to_string());
            println!(
                "{reference}\t{state}\tparent={parent}\tdepth={depth}\tleases={leases}\tconflicts={conflicts}\twork={work}"
            );
            if let Some(paths) = agent.get("changedPaths").and_then(Value::as_array) {
                for path in paths.iter().filter_map(Value::as_str) {
                    println!("  changed={path}");
                }
            }
            if let Some(roots) = agent.get("roots").and_then(Value::as_array) {
                for root in roots {
                    println!(
                        "  root={} route={} generation={} published={}{}",
                        root.get("id").and_then(Value::as_str).unwrap_or("?"),
                        root.get("route").and_then(Value::as_str).unwrap_or("?"),
                        root.get("generation")
                            .and_then(Value::as_str)
                            .unwrap_or("?"),
                        root.get("publishedGeneration")
                            .and_then(Value::as_str)
                            .unwrap_or("never"),
                        if root.get("unpublished").and_then(Value::as_bool) == Some(true) {
                            " pending"
                        } else {
                            ""
                        }
                    );
                }
            }
        }
        return Ok(0);
    }
    match response {
        Value::Null => Ok(0),
        Value::String(text) => {
            println!("{text}");
            Ok(0)
        }
        value => {
            println!("{}", serde_json::to_string_pretty(&value).map_err(display)?);
            Ok(0)
        }
    }
}

pub(crate) fn print_doctor_response(response: &Value) -> Result<i32, String> {
    let checks = response
        .get("checks")
        .and_then(Value::as_array)
        .ok_or_else(|| "doctor response has no checks".to_owned())?;
    for check in checks {
        let status = check
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("fail");
        let name = check
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let detail = check.get("detail").and_then(Value::as_str).unwrap_or("");
        println!("{status:<4} {name:<24} {detail}");
    }
    let ok = response.get("ok").and_then(Value::as_bool) == Some(true);
    println!("{}", if ok { "ready" } else { "not ready" });
    Ok(i32::from(!ok))
}

pub(crate) fn print_git_output(output: GitCommandOutput) -> Result<i32, String> {
    let mut exit_code = 0;
    match output {
        GitCommandOutput::NoOp => {}
        GitCommandOutput::Text(text) => println!("{text}"),
        GitCommandOutput::Paths(paths) => {
            for path in paths {
                println!("{path}");
            }
        }
        GitCommandOutput::Status(status) => {
            println!("On branch {}", status.branch);
            match status.dirty {
                GitDirtyState::Clean => println!("nothing to commit, working tree clean"),
                GitDirtyState::Dirty => {
                    println!("Changes to be committed:");
                    println!("  (all eligible workspace changes are staged automatically)");
                }
                GitDirtyState::Unknown => println!(
                    "working tree contains unresolved lazy paths; run an exact command to scan"
                ),
            }
        }
        GitCommandOutput::Branches { current, branches } => {
            for branch in branches {
                println!(
                    "{} {}",
                    if branch.name == current { "*" } else { " " },
                    branch.name
                );
            }
        }
        GitCommandOutput::Tags(tags) => {
            for tag in tags.keys() {
                println!("{tag}");
            }
        }
        GitCommandOutput::Commits(commits) => {
            for commit in commits {
                println!("commit {}", commit.id.to_hex());
                println!("Author: {}", commit.author);
                println!();
                println!("    {}", commit.message.replace('\n', "\n    "));
                println!();
            }
        }
        GitCommandOutput::Committed(commit) => {
            let id = commit.id.to_hex();
            println!("[{}] {}", id.get(..12).unwrap_or(&id), commit.message);
        }
        GitCommandOutput::Filesystem(GitFilesystemResult::Data { kind, value })
            if kind == "check-ignore" =>
        {
            let paths = value
                .get("paths")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>();
            exit_code = i32::from(paths.is_empty());
            for path in paths {
                println!("{path}");
            }
        }
        GitCommandOutput::Filesystem(GitFilesystemResult::Data { kind, value })
            if kind == "grep" =>
        {
            let matches = value
                .get("matches")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            exit_code = i32::from(matches.is_empty());
            for matched in matches {
                if let (Some(path), Some(text)) = (
                    matched.get("path").and_then(Value::as_str),
                    matched.get("text").and_then(Value::as_str),
                ) {
                    println!("{path}:{text}");
                }
            }
        }
        GitCommandOutput::Filesystem(GitFilesystemResult::Data { value, .. }) => {
            println!("{}", serde_json::to_string_pretty(&value).map_err(display)?);
        }
        other => println!("{}", serde_json::to_string_pretty(&other).map_err(display)?),
    }
    Ok(exit_code)
}
