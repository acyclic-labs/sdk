use acyclic_harness_client::AgentPageRequest;
use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

/// Encoding of requests supplied through standard input.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum InputFormat {
    /// Read one task as plain text.
    #[default]
    Text,
    /// Read one request per JSON line.
    Jsonl,
}

/// Encoding of conversation output.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    /// Render answers as plain text.
    #[default]
    Text,
    /// Emit versioned events as JSON lines.
    Jsonl,
}

/// How task views reach the host filesystem.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum Mounting {
    /// Native when this host admits it, else physical with a notice on stderr.
    #[default]
    Auto,
    /// FUSE, `ProjFS` or NFS projection; refuses to start without it.
    Native,
    /// Plain directories; needs no device, capability or mount helper.
    Physical,
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum Template {
    #[default]
    Graphcoder,
}

/// Shared agent frontend command-line arguments.
#[derive(Debug, Parser)]
#[command(
    name = "acyclic",
    version,
    about = "Coding agent on the local harness's agents or an endpoint's"
)]
pub struct Args {
    /// Enable or disable the question plugin at session start. Defaults to on
    /// only for an attached interactive user surface; never changes on attach.
    #[arg(global = true, long, num_args = 0..=1, default_missing_value = "true", action = clap::ArgAction::Set)]
    pub questions: Option<bool>,

    /// Force delegation: single, team:N (N total workers, depth 1), or unbounded swarm.
    #[arg(global = true, long, conflicts_with_all = ["host_workspace", "endpoint"])]
    pub topology: Option<String>,

    /// Internal detached owner, started through the shared readiness protocol.
    #[arg(long, hide = true)]
    pub local_service: bool,
    #[arg(long, hide = true, requires = "local_service")]
    pub signal_ready: bool,
    /// Select the shipped agent preset; configuration remains ordinary files.
    #[arg(global = true, long, value_enum, default_value_t)]
    pub template: Template,
    /// Own workspace files for native coding hosts, without a provider or agent loop.
    /// Emits a `workspace-owner-ready` JSONL event on stdout after request events are installed.
    #[arg(global = true, long, requires = "host_state_directory", conflicts_with_all = ["prompt", "model", "catalog", "credentials", "operation", "settings_root", "home_file", "default_config", "usage_jsonl", "transcripts", "resume", "agent", "continue_session", "new_session"])]
    pub host_workspace: bool,
    /// Explicit durable storage for the local owner or a selected agent.
    #[arg(global = true, long, conflicts_with = "endpoint")]
    pub host_state_directory: Option<PathBuf>,
    /// Root directory of the selected workspace.
    #[arg(global = true, long, default_value = ".")]
    pub workspace: PathBuf,
    /// How the checkout's task views reach the host filesystem. `physical`
    /// runs every agent in a plain directory and captures its changes at each
    /// tool boundary, so it needs no /dev/fuse, capability or mount helper.
    #[arg(global = true, long, value_enum, default_value_t, conflicts_with_all = ["host_workspace", "endpoint"])]
    pub workspace_mounting: Mounting,
    /// Instruction scope relative to the workspace root.
    #[arg(global = true, long)]
    pub directory: Option<String>,
    /// Explicit model selection, overriding configured selection.
    #[arg(global = true, long, env = "ACYCLIC_MODEL")]
    pub model: Option<String>,
    /// Model catalog file to load before dispatch.
    #[arg(global = true, long, env = "ACYCLIC_CATALOG")]
    pub catalog: Option<PathBuf>,
    /// Explicit provider credentials file.
    #[arg(global = true, long)]
    pub credentials: Option<PathBuf>,
    /// Read provider API keys only from the host process environment, including
    /// <PROVIDER>_API_KEY for custom catalog entries. No keychain or login reads.
    #[arg(global = true, long, conflicts_with = "credentials")]
    pub environment_credentials: bool,
    /// Root directory for user settings discovery.
    #[arg(global = true, long)]
    pub settings_root: Option<PathBuf>,
    /// Initialize a non-secret private home file before settings and role selection.
    /// Repeat for multiple files. Reopening keeps admitted edits and deletions.
    #[arg(global = true, long, conflicts_with = "endpoint", value_name = "VOLUME_PATH=HOST_FILE", value_parser = home_file)]
    pub home_file: Vec<(String, PathBuf)>,
    /// Use shipped defaults and explicit inputs only; ignore user settings,
    /// native credential discovery and environment model/catalog selection. Signing
    /// custody uses private files in this run's state directory.
    #[arg(global = true, long, conflicts_with = "endpoint")]
    pub default_config: bool,
    /// Send ordinary turns without automatic compaction or input-token preflight.
    /// The provider enforces its context window.
    #[arg(global = true, long, conflicts_with_all = ["endpoint", "host_workspace"])]
    pub no_compaction: bool,
    /// Opt this session out of seeing, sending to and receiving from peers.
    #[arg(global = true, long)]
    pub no_peer: bool,
    /// Export committed call usage for this conversation tree after headless
    /// settlement. A path is replaced with this snapshot; '-' writes JSONL to
    /// stdout and sends ordinary answers to stderr. Missing metrics stay null.
    #[arg(global = true, long, value_name = "PATH|-")]
    pub usage_jsonl: Option<PathBuf>,
    /// Explicit version-one JSONL price records for token estimates. The
    /// interactive client retains the selection in its session prices.jsonl.
    #[arg(long, value_name = "FILE", conflicts_with = "host_workspace")]
    pub prices: Option<PathBuf>,
    /// Follow every agent's own journal to DIR/<stable-agent-id>.jsonl.
    /// Uses the root event output's version-one execution and payload envelopes.
    /// Also writes DIR/agents.jsonl, appending once on discovery:
    /// {"version":1,"id":"<file stem>","agent_path":"<usage.agent_path>","parent":null,
    /// "role":"<role>","model":"<provider/model>","topology":{"version":1,"mode":"swarm"}}.
    /// parent is the parent file-stem id, or null for a root; agent_path is
    /// byte-identical to --usage-jsonl's usage.agent_path. Files are replaced on replay.
    #[arg(global = true, long, value_name = "DIR")]
    pub transcripts: Option<PathBuf>,
    /// Explicit number of retained diagnostic text records; no default credit.
    #[arg(global = true, long, requires = "diagnostic_record_bytes")]
    pub diagnostic_records: Option<usize>,
    /// Explicit bytes reserved per diagnostic record, allocated only for enabled logging.
    #[arg(global = true, long, requires = "diagnostic_records")]
    pub diagnostic_record_bytes: Option<usize>,
    /// JSON approval policy; defaults to the shipped `GraphCoder` policy data.
    /// With `--host-workspace` it answers only native permission prompts for
    /// coordination tools, and defaults to leaving them to the host.
    #[arg(global = true, long)]
    pub approval_policy: Option<PathBuf>,
    /// One task supplied directly on the command line.
    #[arg(
        global = true,
        short = 'p',
        long = "prompt",
        conflicts_with = "input_format"
    )]
    pub prompt: Option<String>,
    /// Request encoding for standard input.
    #[arg(
        global = true,
        long,
        value_enum,
        default_value_t,
        requires_if("jsonl", "operation")
    )]
    pub input_format: InputFormat,
    /// Project every committed event with source cursors as versioned JSONL.
    #[arg(global = true, long, value_enum, default_value_t)]
    pub output_format: OutputFormat,
    /// Read source cursors here and replace the file after a complete flushed acknowledgement.
    /// Reuse the same operation; an interrupted observation may replay its payload chunks.
    #[arg(global = true, long, requires = "operation")]
    pub event_cursor: Option<PathBuf>,
    /// Validate final JSON artifacts against this schema before reporting success.
    #[arg(global = true, long)]
    pub output_schema: Option<PathBuf>,
    /// Atomically replace this file with the last successfully validated output.
    #[arg(global = true, long)]
    pub output: Option<PathBuf>,
    /// Message identity: rerunning an operation replays its settled answers
    /// instead of sending its prompts again.
    #[arg(global = true, long)]
    pub operation: Option<String>,
    /// Continue the latest session for this checkout.
    #[arg(short = 'c', long = "continue", global = true, conflicts_with_all = ["resume", "new_session", "agent", "fork_from", "host_workspace"])]
    pub continue_session: bool,
    /// Resume a session by id or name, or open the paged picker when omitted.
    #[arg(short = 'r', long, global = true, num_args = 0..=1, value_name = "SESSION", conflicts_with_all = ["new_session", "agent", "fork_from"])]
    pub resume: Option<Option<String>>,
    /// Start an independent session for this checkout.
    #[arg(long, global = true, conflicts_with_all = ["agent", "fork_from", "host_workspace"])]
    pub new_session: bool,
    /// Select an existing session by its canonical agent path.
    #[arg(global = true, long)]
    pub agent: Option<String>,
    /// Fork a decider's retained conversation event and workspace generation.
    #[arg(global = true, long, value_name = "AGENT_PATH@EVENT_OFFSET", conflicts_with_all = ["agent", "resume", "continue_session", "new_session", "host_workspace"])]
    pub fork_from: Option<String>,
    /// Version-one continuation JSON: {"version":1,"override_item":...}.
    #[arg(long = "override", global = true, value_name = "FILE")]
    pub override_file: Option<PathBuf>,
    /// Render the interactive conversation without a full-screen terminal.
    #[arg(global = true, long)]
    pub inline: bool,
    /// Drive the agent tree an endpoint serves instead of this machine's
    /// harness: the given URL, else `ACYCLIC_ENDPOINT`, else the managed
    /// default, with `ACYCLIC_API_KEY` as the credential. `ACYCLIC_CA_FILE`
    /// names a PEM CA bundle that replaces the platform roots.
    #[arg(global = true, long, num_args = 0..=1, conflicts_with = "host_workspace")]
    pub endpoint: Option<Option<String>>,
    /// Optional operation in place of the default conversation.
    #[command(subcommand)]
    pub command: Option<Command>,
}

impl Args {
    /// Normalize the exec shorthand before validating its input identity.
    pub fn prepare_input(&mut self) -> Result<(), &'static str> {
        if matches!(
            self.command,
            Some(Command::Run { goal: Some(_), .. } | Command::Serve(_))
        ) {
            if self.credentials.is_some() {
                return Err(
                    "unattended execution reads provider keys from the process environment; remove --credentials",
                );
            }
            self.environment_credentials = true;
        }
        if matches!(
            self.command,
            Some(Command::Run { goal: Some(_), .. } | Command::Supervise { .. })
        ) && (self.prompt.is_some() || self.input_format != InputFormat::Text)
        {
            return Err("goal execution and supervision accept no prompt or JSONL input");
        }
        if matches!(self.command, Some(Command::Send { .. })) {
            if self.endpoint.is_none() || self.operation.is_none() {
                return Err(
                    "send requires --endpoint and --operation for an existing authenticated session",
                );
            }
            if self.agent.is_some()
                || self.prompt.is_some()
                || self.no_peer
                || self.resume.is_some()
                || self.continue_session
                || self.new_session
                || self.host_workspace
                || self.local_service
                || self.input_format != InputFormat::Text
            {
                return Err(
                    "send selects its sender and recipient explicitly and accepts no execution or session-creation input",
                );
            }
        }
        if self.no_peer
            && (self.local_service
                || self.host_workspace
                || matches!(self.command, Some(Command::Serve(_))))
        {
            return Err("--no-peer selects a client session, not a serving process");
        }
        if let Some(Command::Inspect(inspect)) = &self.command {
            if self.prompt.is_some()
                || self.operation.is_some()
                || self.host_workspace
                || self.resume.is_some()
                || self.continue_session
                || self.new_session
                || self.input_format != InputFormat::Text
                || self.usage_jsonl.is_some()
            {
                return Err(
                    "inspect selects existing streams without execution input; use acyclic inspect SESSION [--agent PATH]",
                );
            }
            if !inspect.children && inspect.page.cursor.is_some() {
                return Err("inspect --cursor requires --children");
            }
        }
        if self.override_file.is_some() && self.fork_from.is_none() {
            return Err("--override requires --fork-from AGENT_PATH@EVENT_OFFSET");
        }
        if self.fork_from.is_some()
            && !matches!(
                self.command,
                None | Some(Command::Run { .. } | Command::Exec { .. })
            )
        {
            return Err(
                "--fork-from requires an execution; use acyclic --fork-from AGENT_PATH@EVENT_OFFSET exec 'Continue'",
            );
        }
        if matches!(self.command, Some(Command::Serve(_)))
            && (self.prompt.is_some()
                || self.endpoint.is_some()
                || self.host_workspace
                || self.usage_jsonl.is_some()
                || self.transcripts.is_some()
                || self.input_format != InputFormat::Text
                || self.operation.is_some()
                || self.agent.is_some()
                || self.resume.is_some()
                || self.continue_session
                || self.new_session)
        {
            return Err(
                "serve input: execution inputs cannot select a server; recovery: acyclic serve ADDRESS --workspace PATH",
            );
        }
        if let Some(Command::Attach { path }) = &self.command {
            if self.agent.is_some()
                || self.prompt.is_some()
                || self.operation.is_some()
                || self.input_format != InputFormat::Text
                || self.host_workspace
            {
                return Err(
                    "attach input: select one existing agent without execution input; recovery: acyclic attach AGENT",
                );
            }
            self.agent = Some(path.clone());
        }
        if let Some(Command::Exec { prompt }) = &self.command {
            if self.prompt.is_some() {
                return Err("exec input: task supplied twice; recovery: acyclic run -p 'TASK'");
            }
            self.prompt = prompt.clone();
            if prompt.is_none() {
                self.input_format = InputFormat::Jsonl;
            }
        }
        if self.host_workspace && (self.prompt.is_some() || self.input_format == InputFormat::Jsonl)
        {
            return Err(
                "workspace owner cannot execute input; recovery: remove --host-workspace to execute a task",
            );
        }
        if self.input_format == InputFormat::Jsonl && self.operation.is_none() {
            return Err(
                "JSONL input: --operation is required; recovery: acyclic run --operation OPERATION --input-format jsonl",
            );
        }
        if self.output_format == OutputFormat::Jsonl
            && self.input_format == InputFormat::Text
            && self.prompt.is_none()
            && matches!(self.command, None | Some(Command::Run { goal: None, .. }))
        {
            return Err(
                "event input: no task or JSONL input; recovery: acyclic run --output-format jsonl --operation OPERATION --input-format jsonl",
            );
        }
        let event_options = self.output_format == OutputFormat::Jsonl
            || self.event_cursor.is_some()
            || self.output_schema.is_some()
            || self.output.is_some();
        if self.transcripts.is_some()
            && !matches!(
                self.command,
                None | Some(Command::Run { .. } | Command::Exec { .. } | Command::Doctor)
            )
        {
            return Err(
                "transcripts require an execution; recovery: acyclic --transcripts DIR exec 'TASK'",
            );
        }
        // `doctor` takes a run's exact flags; its output flags are inert.
        if event_options
            && (self.host_workspace
                || !matches!(
                    self.command,
                    None | Some(Command::Run { .. } | Command::Exec { .. } | Command::Doctor)
                ))
        {
            return Err(
                "output projection: requires an execution; recovery: acyclic run --output-format jsonl -p 'TASK'",
            );
        }
        if event_options && self.output_format != OutputFormat::Jsonl {
            return Err(
                "output projection: requires JSONL output; recovery: acyclic run --output-format jsonl -p 'TASK'",
            );
        }
        if self
            .output
            .as_ref()
            .is_some_and(|path| path.as_os_str() == "-")
        {
            return Err(
                "output artifact: stdout is reserved for events; recovery: acyclic run --output-format jsonl --output final-output.json -p 'TASK'",
            );
        }
        if self.output_format == OutputFormat::Jsonl
            && self
                .usage_jsonl
                .as_ref()
                .is_some_and(|path| path.as_os_str() == "-")
        {
            return Err(
                "output projection: usage export also selects stdout; recovery: acyclic run --usage-jsonl usage.jsonl --output-format jsonl -p 'TASK'",
            );
        }
        let files = [&self.output, &self.event_cursor, &self.usage_jsonl];
        let files = files
            .iter()
            .filter_map(|path| path.as_deref())
            .filter(|path| path.as_os_str() != "-");
        if self
            .transcripts
            .as_ref()
            .is_some_and(|directory| files.clone().any(|path| path.starts_with(directory)))
        {
            return Err(
                "transcript directory is reserved for agent files; recovery: place other outputs outside --transcripts DIR",
            );
        }
        self.check_output_paths()
    }

    // The same collision rule applies before I/O and after physical path
    // resolution. Admission must not accept two writers for one destination.
    fn check_output_paths(&self) -> Result<(), &'static str> {
        let files = [&self.output, &self.event_cursor, &self.usage_jsonl];
        let files = files
            .iter()
            .filter_map(|path| path.as_deref())
            .filter(|path| path.as_os_str() != "-");
        if files
            .clone()
            .enumerate()
            .any(|(index, path)| files.clone().skip(index + 1).any(|other| other == path))
        {
            return Err(
                "output files: --output, --event-cursor and --usage-jsonl must name distinct files; recovery: acyclic --output-format jsonl --output final.json --event-cursor cursor.json exec 'TASK'",
            );
        }
        Ok(())
    }

    /// Resolve the output directory once, before any export or conversation
    /// opens. Atomic replacement addresses a directory entry, not the inode
    /// currently at that name, so retain the final component unchanged.
    pub fn prepare_outputs(&mut self) -> std::io::Result<()> {
        let report = match &mut self.command {
            Some(Command::Study(study)) => Some(&mut study.report),
            _ => None,
        };
        for path in [
            &mut self.output,
            &mut self.event_cursor,
            &mut self.usage_jsonl,
        ]
        .into_iter()
        .flatten()
        .chain(report)
        .filter(|path| path.as_os_str() != "-")
        {
            let name = path.file_name().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "output files: destination has no file name; recovery: acyclic --output-format jsonl --output final.json exec 'TASK'",
                )
            })?;
            let parent = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| std::path::Path::new("."));
            let mut resolved = std::fs::canonicalize(parent)
                .map_err(|error| {
                    std::io::Error::new(
                        error.kind(),
                        format!("output files: resolve {}: {error}; recovery: acyclic --output-format jsonl --output final.json exec 'TASK'", parent.display()),
                    )
                })?;
            resolved.push(name);
            match std::fs::symlink_metadata(&resolved) {
                Ok(metadata) if metadata.is_dir() => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "output files: {} is a directory; recovery: acyclic --output-format jsonl --output final.json exec 'TASK'",
                            resolved.display()
                        ),
                    ));
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(std::io::Error::new(
                        error.kind(),
                        format!(
                            "output files: inspect {}: {error}; recovery: acyclic --output-format jsonl --output final.json exec 'TASK'",
                            resolved.display()
                        ),
                    ));
                }
            }
            *path = resolved;
        }
        // Every writer uses the resolved directory spelling when comparing
        // destinations and later replacing its entry.
        self.check_output_paths()
            .map_err(|message| std::io::Error::new(std::io::ErrorKind::InvalidInput, message))
    }
}

/// Operations accepted by the shipping command-line parser.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the shared file-driven goal continuation until goal or terminal signal.
    Run {
        /// UTF-8 Markdown goal; plain Markdown starts active.
        #[arg(long, requires = "policy", conflicts_with = "prompt")]
        goal: Option<PathBuf>,
        /// Version-one supervision policy with owner-configured reservations.
        #[arg(long, requires = "goal")]
        policy: Option<PathBuf>,
    },
    /// Follow every admitted session's lifecycle and supervision source stream.
    Supervise {
        /// Version-one JSONL events on stdout, one original event per line.
        #[arg(long, required = true)]
        json: bool,
    },
    /// Attach the terminal to an existing canonical agent path.
    Attach { path: String },
    /// Execute through the same conversation driver used by prompt mode.
    Exec {
        /// One task; omitted reads JSONL requests from stdin.
        #[arg(id = "task")]
        prompt: Option<String>,
    },
    /// Append attributed peer input to an existing session without running a client loop.
    Send {
        /// Existing sending session path, id or exact alias.
        #[arg(long)]
        from: String,
        /// Existing recipient session path, id or exact alias.
        recipient: String,
        /// Peer content; never expanded as a user command.
        message: String,
    },
    /// Read one lazy page of this checkout's root sessions.
    Sessions(Sessions),
    /// Inspect an existing session through its original authenticated streams.
    Inspect(Inspect),
    /// Create a session and optionally author its title.
    #[command(name = "new")]
    NewSession { title: Option<String> },
    /// Rename the selected session by conditionally writing ui/title.
    Rename { title: String },
    /// Give the selected session a stable alias for -r NAME.
    Name { name: String },
    /// Freeze the selected session and release its owned resources.
    Archive,
    /// Read one immutable page of the selected session's saved turns.
    History(Page),
    /// Run a paired study: the arms file's source task once, then every
    /// assigned arm and replicate as a fork of that settled turn. Report
    /// rows (JSONL) replace REPORT once the whole study has settled.
    /// Reusing `--operation` replays settled work instead of paying again.
    Study(Study),
    /// Serve this harness's agent tree through the harness server
    /// (`acyclic_harness::server::serve`), as a managed host does; prints its
    /// endpoint, credential and principal as one JSON line.
    Serve(Serve),
    /// Check provider keys and network, keychain, terminal, shell, native
    /// filesystem mode and state policy without opening storage or authority.
    /// Lists every missing prerequisite with its recovery. Pass -p TASK or
    /// JSONL flags to check headless prerequisites.
    Doctor,
}

#[derive(Debug, clap::Args)]
pub struct Page {
    /// Return this opaque continuation unchanged from the preceding page.
    #[arg(long)]
    pub cursor: Option<String>,
    /// Entries per page; this chunks discovery and never limits session count.
    #[arg(long, default_value_t = <AgentPageRequest>::default().items())]
    pub page_size: u32,
    /// Encoded reply credit; increase it if one row and its continuation cannot fit.
    #[arg(long, default_value_t = <AgentPageRequest>::default().bytes())]
    pub page_bytes: u32,
}

#[derive(Debug, clap::Args)]
pub struct Inspect {
    /// Existing canonical path, stable id or exact session alias.
    pub session: String,
    /// Follow committed changes, without periodic reads.
    #[arg(long, conflicts_with = "children")]
    pub follow: bool,
    /// Enumerate the tree lazily and emit each agent's current inspection.
    #[arg(long, conflicts_with_all = ["children", "follow"])]
    pub tree: bool,
    /// Read one direct-child page rather than the selected agent's measurements.
    #[arg(long)]
    pub children: bool,
    #[command(flatten)]
    pub page: Page,
}

#[derive(Debug, clap::Args)]
pub struct Sessions {
    /// List participating peer sessions using read-only discovery authority.
    #[arg(long)]
    pub peers: bool,
    #[command(flatten)]
    pub page: Page,
    /// Case-insensitive title or root-id substring within each requested page.
    #[arg(long)]
    pub title: Option<String>,
    /// Match these states (repeat or comma-separate): idle, running,
    /// waiting-approval, waiting, done, archived.
    #[arg(long, value_delimiter = ',')]
    pub state: Vec<acyclic_harness::local::sessions::State>,
    /// Inclusive UTC creation bound in Unix microseconds.
    #[arg(long)]
    pub created_after: Option<u64>,
    /// Exclusive UTC creation bound in Unix microseconds.
    #[arg(long)]
    pub created_before: Option<u64>,
    /// Inclusive UTC status-commit bound in Unix microseconds.
    #[arg(long)]
    pub updated_after: Option<u64>,
}

#[derive(Debug, clap::Args)]
pub struct Study {
    /// Version-1 arms file; the files it names are relative to it.
    pub arms: PathBuf,
    /// Report destination, replaced whole; '-' writes stdout.
    #[arg(long)]
    pub report: PathBuf,
}

#[derive(Debug, clap::Args)]
pub struct Serve {
    /// The address to listen on. Without a TLS identity, only a loopback
    /// address is served.
    pub address: std::net::SocketAddr,
    /// PEM certificate chain of the server's TLS identity.
    #[arg(long, requires_all = ["tls_key", "url"])]
    pub tls_certificate: Option<PathBuf>,
    /// PEM private key of the server's TLS identity.
    #[arg(long, requires = "tls_certificate")]
    pub tls_key: Option<PathBuf>,
    /// The `https` URL clients reach the server at, which its certificate
    /// names; the listening address need not be reachable.
    #[arg(long, requires = "tls_certificate")]
    pub url: Option<String>,
}

fn home_file(value: &str) -> Result<(String, PathBuf), String> {
    let (path, source) = value
        .split_once('=')
        .filter(|(path, source)| !path.is_empty() && !source.is_empty())
        .ok_or("expected VOLUME_PATH=HOST_FILE")?;
    Ok((path.to_owned(), PathBuf::from(source)))
}

#[cfg(test)]
mod tests {
    #[test]
    fn peer_send_requires_existing_endpoint_and_explicit_intent() {
        use clap::Parser as _;
        for options in [
            vec![],
            vec!["--endpoint", "http://localhost:9000"],
            vec!["--operation", "message-1"],
            vec![
                "--endpoint",
                "http://localhost:9000",
                "--operation",
                "message-1",
                "--no-peer",
            ],
        ] {
            let mut argv = vec!["acyclic"];
            argv.extend(options);
            argv.extend(["send", "--from", "sender", "recipient", "status please"]);
            let mut args = super::Args::try_parse_from(argv).expect("valid command syntax");
            assert!(args.prepare_input().is_err());
        }
        let mut args = super::Args::try_parse_from([
            "acyclic",
            "--endpoint",
            "http://localhost:9000",
            "--operation",
            "message-1",
            "send",
            "--from",
            "sender",
            "recipient",
            "status please",
        ])
        .expect("valid peer command");
        assert!(args.prepare_input().is_ok());
    }

    use super::*;

    #[test]
    fn fork_override_is_accepted_after_exec_and_run() {
        for argv in [
            vec![
                "graphcoder",
                "--fork-from",
                "agents/original@4",
                "exec",
                "Continue",
                "--override",
                "continuation.json",
            ],
            vec![
                "acyclic",
                "run",
                "--fork-from",
                "agents/original@4",
                "--override",
                "continuation.json",
                "-p",
                "Continue",
            ],
        ] {
            let mut args = Args::try_parse_from(argv).unwrap();
            assert!(args.prepare_input().is_ok());
            assert_eq!(
                args.override_file.as_deref(),
                Some(std::path::Path::new("continuation.json"))
            );
        }
    }

    #[test]
    fn product_commands_reject_execution_inputs_before_attach_or_serve() {
        for argv in [
            vec!["graphcoder", "--local-service", "--no-peer"],
            vec!["graphcoder", "--no-peer", "--local-service"],
            vec!["graphcoder", "--local-service", "exec", "task", "--no-peer"],
        ] {
            let mut args = Args::try_parse_from(&argv).unwrap();
            assert!(args.prepare_input().is_err(), "{argv:?}");
        }
        let mut run = Args::try_parse_from([
            "acyclic",
            "run",
            "--template",
            "graphcoder",
            "--workspace",
            "source",
            "--workspace-mounting",
            "physical",
            "--no-compaction",
            "--no-peer",
            "-p",
            "task",
        ])
        .unwrap();
        run.prepare_input().unwrap();
        assert_eq!(run.workspace, PathBuf::from("source"));
        assert_eq!(run.workspace_mounting, Mounting::Physical);
        assert!(run.no_compaction);
        assert!(run.no_peer);
        assert_eq!(run.prompt.as_deref(), Some("task"));
        assert!(
            !run.environment_credentials,
            "ordinary runs retain credential selection"
        );
        let mut credentialed =
            Args::try_parse_from(["acyclic", "run", "--credentials", "keys.json", "-p", "task"])
                .unwrap();
        credentialed.prepare_input().unwrap();
        assert!(!credentialed.environment_credentials);
        let mut goal = Args::try_parse_from([
            "acyclic",
            "run",
            "--goal",
            "GOAL.md",
            "--policy",
            "policy.json",
        ])
        .unwrap();
        goal.prepare_input().unwrap();
        assert!(goal.environment_credentials);
        for command in [
            vec!["run", "--goal", "GOAL.md", "--policy", "policy.json"],
            vec!["supervise", "--json"],
        ] {
            for input in [vec!["-p", "discarded"], vec!["--input-format", "jsonl"]] {
                match Args::try_parse_from(
                    ["acyclic"]
                        .into_iter()
                        .chain(command.iter().copied())
                        .chain(input),
                ) {
                    Ok(mut args) => assert!(args.prepare_input().is_err()),
                    Err(_) => {}
                }
            }
        }

        let mut attach =
            Args::try_parse_from(["acyclic", "attach", "harness/agents/root"]).unwrap();
        attach.prepare_input().unwrap();
        assert_eq!(attach.agent.as_deref(), Some("harness/agents/root"));

        for command in [
            vec!["attach", "harness/agents/root"],
            vec!["serve", "127.0.0.1:0"],
        ] {
            for input in [vec!["-p", "new task"], vec!["--operation", "new-operation"]] {
                let mut args = Args::try_parse_from(
                    ["acyclic"]
                        .into_iter()
                        .chain(command.iter().copied())
                        .chain(input),
                )
                .unwrap();
                assert!(args.prepare_input().is_err());
            }
        }
        for selection in [
            vec!["-r"],
            vec!["-r", "previous"],
            vec!["-c"],
            vec!["--new-session"],
            vec!["--no-peer"],
        ] {
            let mut args = Args::try_parse_from(
                ["acyclic", "serve", "127.0.0.1:0"]
                    .into_iter()
                    .chain(selection),
            )
            .unwrap();
            assert!(args.prepare_input().is_err());
        }
    }

    #[test]
    fn native_workspace_owner_requires_explicit_state_and_excludes_agent_inputs() {
        assert!(Args::try_parse_from(["graphcoder", "--host-workspace"]).is_err());
        let base = [
            "graphcoder",
            "--host-workspace",
            "--host-state-directory",
            "fixture-state",
        ];
        assert!(Args::try_parse_from(base).is_ok());
        for option in [
            "--prompt",
            "--model",
            "--catalog",
            "--credentials",
            "--operation",
            "--settings-root",
        ] {
            assert!(
                Args::try_parse_from(base.into_iter().chain([option, "fixture"])).is_err(),
                "{option}"
            );
        }
    }

    #[test]
    fn native_workspace_owner_rejects_normalized_positional_task() {
        for task in [Some("TASK"), None] {
            let arguments = [
                "graphcoder",
                "--host-workspace",
                "--host-state-directory",
                "fixture-state",
                "exec",
            ];
            let mut args = Args::try_parse_from(arguments.into_iter().chain(task)).unwrap();
            assert_eq!(
                args.prepare_input(),
                Err(
                    "workspace owner cannot execute input; recovery: remove --host-workspace to execute a task"
                )
            );
        }
        let mut owner = Args::try_parse_from([
            "graphcoder",
            "--host-workspace",
            "--host-state-directory",
            "fixture-state",
        ])
        .unwrap();
        assert!(owner.prepare_input().is_ok());
        let mut execution = Args::try_parse_from(["graphcoder", "exec", "TASK"]).unwrap();
        assert!(execution.prepare_input().is_ok());
        assert_eq!(execution.prompt.as_deref(), Some("TASK"));
    }

    #[test]
    fn headless_modes_use_explicit_jsonl_identity_and_reject_mixed_formats() {
        assert!(Args::try_parse_from(["graphcoder", "-p", "hello"]).is_ok());
        assert!(Args::try_parse_from(["graphcoder", "--search-config", "search.json"]).is_err());
        assert!(Args::try_parse_from(["graphcoder", "--input-format", "jsonl"]).is_err());
        assert!(
            Args::try_parse_from([
                "graphcoder",
                "--input-format",
                "jsonl",
                "--operation",
                "00000000-0000-0000-0000-000000000001"
            ])
            .is_ok()
        );
        assert!(
            Args::try_parse_from([
                "graphcoder",
                "-p",
                "hello",
                "--input-format",
                "jsonl",
                "--operation",
                "operation"
            ])
            .is_err()
        );
    }

    #[test]
    fn serve_takes_an_address_and_a_complete_tls_identity() {
        assert!(Args::try_parse_from(["graphcoder", "serve", "127.0.0.1:0"]).is_ok());
        assert!(Args::try_parse_from(["graphcoder", "serve"]).is_err());
        assert!(
            Args::try_parse_from(["graphcoder", "serve", "0.0.0.0:443", "--tls-key", "key.pem"])
                .is_err()
        );
    }
}
