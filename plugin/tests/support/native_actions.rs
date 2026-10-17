//! Real hook attachment, then the same hook handler on one retained connection.
use super::native_hooks::{self, Result};
use acyclic_harness::{
    TaskId,
    external_executor::{ExternalHost, handle_hook, local_hook_volumes, request},
    task_volume::{Component, Query, TaskVolume},
};
use acyclic_stream::grpc::Client;
use serde_json::{Value, json};
use std::{path::Path, sync::Arc};

pub struct Tools<'a> {
    cwd: &'a Path,
    pub host: ExternalHost,
    pub session: String,
    caller: Option<String>,
    pub task: TaskId,
    pub provider: Arc<Client>,
    requests: TaskVolume<Client>,
    results: TaskVolume<Client>,
    timings: bool,
}

impl<'a> Tools<'a> {
    pub async fn hosts(cwd: &'a Path) -> Result<[Self; 3]> {
        let codex = Self::open(cwd, ExternalHost::Codex, None).await?;
        let (claude, pi) = tokio::try_join!(
            Self::open(
                cwd,
                ExternalHost::ClaudeCode,
                Some(Arc::clone(&codex.provider))
            ),
            Self::open(cwd, ExternalHost::Pi, Some(Arc::clone(&codex.provider))),
        )?;
        Ok([codex, claude, pi])
    }

    pub async fn open(
        cwd: &'a Path,
        host: ExternalHost,
        provider: Option<Arc<Client>>,
    ) -> Result<Self> {
        let started = std::time::Instant::now();
        let timings = std::env::var_os("ACYCLIC_NATIVE_TIMINGS").is_some();
        let session = uuid::Uuid::new_v4().to_string();
        let event = if host == ExternalHost::Pi {
            "session_start"
        } else {
            "SessionStart"
        };
        let input = json!({"session_id":session,"cwd":cwd});
        let task = request(host, event, input.clone())?.task_id();
        native_hooks::hook(cwd, host.name(), event, input).await?;
        let provider = match provider {
            Some(provider) => provider,
            None => Arc::new(native_hooks::client(cwd).await?),
        };
        if timings {
            eprintln!(
                "native attach {}: {:.3} s",
                host.name(),
                started.elapsed().as_secs_f64()
            );
        }
        let attached = Self::attached(cwd, host, session, provider).await?;
        assert_eq!(attached.task, task);
        Ok(attached)
    }

    /// Reuses an authenticated provider after a real native SessionStart.
    pub async fn attached(
        cwd: &'a Path,
        host: ExternalHost,
        session: String,
        provider: Arc<Client>,
    ) -> Result<Self> {
        let event = if host == ExternalHost::Pi {
            "session_start"
        } else {
            "SessionStart"
        };
        let task = request(host, event, json!({"session_id":session,"cwd":cwd}))?.task_id();
        let (requests, results) = local_hook_volumes(Arc::clone(&provider), task).await?;
        Ok(Self {
            cwd,
            host,
            session,
            caller: None,
            task,
            provider,
            requests,
            results,
            timings: std::env::var_os("ACYCLIC_NATIVE_TIMINGS").is_some(),
        })
    }

    pub async fn child(&self, caller: &str) -> Result<Self> {
        let event = if self.host == ExternalHost::Pi {
            "subagent_start"
        } else {
            "SubagentStart"
        };
        let input = json!({"session_id":self.session,"agent_id":caller,
            "parent_agent_id":self.caller,"cwd":self.cwd});
        handle_hook(
            self.host,
            event,
            input,
            &self.requests,
            &self.results,
            Component::new(self.task.to_string())?,
        )
        .await?;
        let event = if self.host == ExternalHost::Pi {
            "session_start"
        } else {
            "SessionStart"
        };
        let input = json!({"session_id":self.session,"agent_id":caller,"cwd":self.cwd});
        let task = request(self.host, event, input.clone())?.task_id();
        let (requests, results) = local_hook_volumes(Arc::clone(&self.provider), task).await?;
        handle_hook(
            self.host,
            event,
            input,
            &requests,
            &results,
            Component::new(task.to_string())?,
        )
        .await?;
        Ok(Self {
            cwd: self.cwd,
            host: self.host,
            session: self.session.clone(),
            caller: Some(caller.to_owned()),
            task,
            provider: Arc::clone(&self.provider),
            requests,
            results,
            timings: self.timings,
        })
    }

    pub async fn reconnect(&mut self) -> Result {
        let provider = Arc::new(native_hooks::client(self.cwd).await?);
        let (requests, results) = local_hook_volumes(Arc::clone(&provider), self.task).await?;
        self.provider = provider;
        self.requests = requests;
        self.results = results;
        Ok(())
    }

    pub async fn discover_tasks(&self, known: &[TaskId]) -> Result<Vec<TaskId>> {
        use acyclic_stream::StreamProvider as _;
        let page = self
            .provider
            .children_page(acyclic_stream::ChildrenPageRequest {
                parent: Some(acyclic_stream::StreamPath::new("external/requests")?),
                after: None,
                hierarchy_version: None,
                limit: 64,
            })
            .await?;
        assert!(
            page.next_after.is_none(),
            "the fixture has at most twelve admitted tasks"
        );
        let mut tasks = Vec::new();
        for child in page.children {
            let task = TaskId::parse(
                child
                    .path
                    .as_str()
                    .rsplit('/')
                    .next()
                    .ok_or("missing discovered task")?,
            )?;
            if !known.contains(&task) {
                tasks.push(task);
            }
        }
        Ok(tasks)
    }

    async fn outcome(&self, id: &str, name: &str, arguments: Value) -> Result<(Value, bool)> {
        let event = if self.host == ExternalHost::Pi {
            "tool_call"
        } else {
            "PreToolUse"
        };
        let mut input = json!({"session_id":self.session,"agent_id":self.caller,"tool_use_id":id,
            "tool_name":name,"tool_input":arguments});
        let call = request(self.host, event, input.clone())?;
        if self.host != ExternalHost::Pi {
            input["tool_name"] = json!(format!("mcp__acyclic__{name}"));
        }
        let started = std::time::Instant::now();
        let output = handle_hook(
            self.host,
            event,
            input,
            &self.requests,
            &self.results,
            Component::new(self.task.to_string())?,
        )
        .await;
        let handled = started.elapsed();
        let key = call.result_key(Component::new(self.task.to_string())?)?;
        let snapshot = self
            .results
            .snapshot(&Query::File(key), 1)
            .await
            .map_err(|error| format!("{} {name} {id} result query: {error}", self.host.name()))?;
        if self.timings {
            eprintln!(
                "native call {} {name} {id}: {:.3} s handler, {:.3} s result query",
                self.host.name(),
                handled.as_secs_f64(),
                (started.elapsed() - handled).as_secs_f64()
            );
        }
        assert_eq!(
            snapshot.files.len(),
            1,
            "{} {name}: {output:?}",
            self.host.name(),
        );
        Ok((
            serde_json::from_slice(&snapshot.files[0].bytes)?,
            output.is_ok(),
        ))
    }

    pub async fn call(&self, id: &str, name: &str, arguments: Value) -> Result<Value> {
        let (outcome, success) = self.outcome(id, name, arguments).await?;
        assert!(success, "{} {name}: {outcome}", self.host.name());
        outcome
            .get("Succeeded")
            .cloned()
            .ok_or_else(|| format!("{name}: {outcome}").into())
    }

    pub async fn reject(&self, id: &str, name: &str, arguments: Value) -> Result<Value> {
        let (outcome, success) = self.outcome(id, name, arguments).await?;
        assert!(!success, "{} {name}: {outcome}", self.host.name());
        assert!(outcome.get("Failed").is_some(), "{outcome}");
        Ok(outcome)
    }
}
