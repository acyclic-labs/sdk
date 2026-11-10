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
    #[arg(global = true, long, requires = "host_state_directory", conflicts_with_all = ["prompt", "model", "catalog", "credentials", "operation", "settings_root", "default_config", "usage_jsonl", "transcripts", "resume", "agent", "continue_session", "new_session"])]
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
    /// Root directory for user settings discovery.
    #[arg(global = true, long)]
    pub settings_root: Option<PathBuf>,
    /// Use shipped defaults and explicit inputs only; ignore user settings,
    /// native credential discovery and environment model/catalog selection. Signing
    /// custody uses private files in this run's state directory.
    #[arg(global = true, long, conflicts_with = "endpoint")]
    pub default_config: bool,
    /// Send ordinary turns without automatic compaction or input-token preflight.
    /// The provider enforces its context window.
    #[arg(global = true, long, conflicts_with_all = ["endpoint", "host_workspace"])]
    pub no_compaction: bool,
    /// Export committed call usage for this conversation tree after headless
    /// settlement. A path is replaced with this snapshot; '-' writes JSONL to
    /// stdout and sends ordinary answers to stderr. Missing metrics stay null.
    #[arg(global = true, long, value_name = "PATH|-")]
    pub usage_jsonl: Option<PathBuf>,
    /// Follow every agent's own journal to DIR/<stable-agent-id>.jsonl.
    /// Uses the root event output's version-one execution and payload envelopes.
    /// Also writes DIR/agents.jsonl, appending once on discovery:
    /// {"version":1,"id":"<file stem>","agent_path":"<usage.agent_path>","parent":null}.
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
    /// default, with `ACYCLIC_API_KEY` as the credential.
    #[arg(global = true, long, num_args = 0..=1, conflicts_with = "host_workspace")]
    pub endpoint: Option<Option<String>>,
    /// Optional operation in place of the default conversation.
    #[command(subcommand)]
    pub command: Option<Command>,
}

impl Args {
    /// Normalize the exec shorthand before validating its input identity.
    pub fn prepare_input(&mut self) -> Result<(), &'static str> {
        if self.override_file.is_some() && self.fork_from.is_none() {
            return Err("--override requires --fork-from AGENT_PATH@EVENT_OFFSET");
        }
        if self.fork_from.is_some()
            && !matches!(
                self.command,
                None | Some(Command::Run | Command::Exec { .. })
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
            && matches!(self.command, None | Some(Command::Run))
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
                None | Some(Command::Run | Command::Exec { .. } | Command::Doctor)
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
                    None | Some(Command::Run | Command::Exec { .. } | Command::Doctor)
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
    /// Run the GraphCoder preset through the shared conversation driver.
    Run,
    /// Attach the terminal to an existing canonical agent path.
    Attach { path: String },
    /// Execute through the same conversation driver used by prompt mode.
    Exec {
        /// One task; omitted reads JSONL requests from stdin.
        #[arg(id = "task")]
        prompt: Option<String>,
    },
    /// Read one lazy page of this checkout's root sessions.
    Sessions(Sessions),
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
    #[arg(long, default_value_t = AgentPageRequest::default().items())]
    pub page_size: u32,
    /// Encoded reply credit; increase it if one row and its continuation cannot fit.
    #[arg(long, default_value_t = AgentPageRequest::default().bytes())]
    pub page_bytes: u32,
}

#[derive(Debug, clap::Args)]
pub struct Sessions {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fork_override_is_accepted_after_exec() {
        let mut args = Args::try_parse_from([
            "graphcoder",
            "--fork-from",
            "agents/original@4",
            "exec",
            "Continue",
            "--override",
            "continuation.json",
        ])
        .unwrap();
        assert!(args.prepare_input().is_ok());
        assert_eq!(
            args.override_file.as_deref(),
            Some(std::path::Path::new("continuation.json"))
        );
    }

    #[test]
    fn product_commands_reject_execution_inputs_before_attach_or_serve() {
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
            "-p",
            "task",
        ])
        .unwrap();
        run.prepare_input().unwrap();
        assert_eq!(run.workspace, PathBuf::from("source"));
        assert_eq!(run.workspace_mounting, Mounting::Physical);
        assert!(run.no_compaction);
        assert_eq!(run.prompt.as_deref(), Some("task"));

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
