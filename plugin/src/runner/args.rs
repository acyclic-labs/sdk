use acyclic_harness::client::AgentPageRequest;
use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum InputFormat {
    #[default]
    Text,
    Jsonl,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    #[default]
    Text,
    Jsonl,
}

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum Template {
    #[default]
    Graphcoder,
}

#[derive(Debug, Parser)]
#[command(
    name = "acyclic",
    version,
    about = "Coding agent on the local harness's agents or an endpoint's"
)]
pub struct Args {
    /// Select the shipped agent preset; configuration remains ordinary files.
    #[arg(global = true, long, value_enum, default_value_t)]
    pub template: Template,
    /// Own workspace files for native coding hosts, without a provider or agent loop.
    /// Emits a `workspace-owner-ready` JSONL event on stdout after request events are installed.
    #[arg(global = true, long, requires = "host_state_directory", conflicts_with_all = ["prompt", "model", "catalog", "credentials", "operation", "settings_root", "default_config", "usage_jsonl", "resume", "agent"])]
    pub host_workspace: bool,
    /// Explicit durable storage, opened only by this trusted harness owner.
    #[arg(global = true, long, requires = "host_workspace")]
    pub host_state_directory: Option<PathBuf>,
    #[arg(global = true, long, default_value = ".")]
    pub workspace: PathBuf,
    /// Instruction scope relative to the workspace root.
    #[arg(global = true, long)]
    pub directory: Option<String>,
    #[arg(global = true, long, env = "ACYCLIC_MODEL")]
    pub model: Option<String>,
    #[arg(global = true, long)]
    pub catalog: Option<PathBuf>,
    #[arg(global = true, long)]
    pub credentials: Option<PathBuf>,
    #[arg(global = true, long)]
    pub settings_root: Option<PathBuf>,
    /// Use shipped defaults and explicit inputs only; ignore user settings,
    /// native credential discovery and environment model selection. Signing
    /// custody uses private files in this run's state directory.
    #[arg(global = true, long, conflicts_with = "endpoint")]
    pub default_config: bool,
    /// Export committed call usage for this conversation tree after headless
    /// settlement. A path is replaced with this snapshot; '-' writes JSONL to
    /// stdout and sends ordinary answers to stderr. Missing metrics stay null.
    #[arg(global = true, long, value_name = "PATH|-")]
    pub usage_jsonl: Option<PathBuf>,
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
    #[arg(
        global = true,
        short = 'p',
        long = "prompt",
        conflicts_with = "input_format"
    )]
    pub prompt: Option<String>,
    #[arg(
        global = true,
        long,
        value_enum,
        default_value_t,
        requires_if("jsonl", "operation")
    )]
    pub input_format: InputFormat,
    /// Project every committed event with source cursors as versioned JSONL.
    #[arg(long, global = true, value_enum, default_value_t)]
    pub output_format: OutputFormat,
    /// Read source cursors here and replace the file after a complete flushed acknowledgement.
    /// Reuse the same operation; an interrupted observation may replay its payload chunks.
    #[arg(global = true, long, requires = "operation")]
    pub event_cursor: Option<PathBuf>,
    /// Validate final JSON artifacts against this schema before reporting success.
    #[arg(long, global = true)]
    pub output_schema: Option<PathBuf>,
    /// Atomically replace this file with the last successfully validated output.
    #[arg(long, global = true)]
    pub output: Option<PathBuf>,
    /// Message identity: rerunning an operation replays its settled answers
    /// instead of sending its prompts again.
    #[arg(long, global = true)]
    pub operation: Option<String>,
    /// Reopen the last interactive conversation, including after a normal quit.
    #[arg(long, global = true)]
    pub resume: bool,
    /// Select an existing session by its canonical agent path.
    #[arg(long, global = true)]
    pub agent: Option<String>,
    #[arg(global = true, long)]
    pub inline: bool,
    /// Drive the agent tree an endpoint serves instead of this machine's
    /// harness: the given URL, else `ACYCLIC_ENDPOINT`, else the managed
    /// default, with `ACYCLIC_API_KEY` as the credential.
    #[arg(global = true, long, num_args = 0..=1, conflicts_with = "host_workspace")]
    pub endpoint: Option<Option<String>>,
    #[command(subcommand)]
    pub command: Option<Command>,
}

impl Args {
    /// Normalize the exec shorthand before validating its input identity.
    pub fn prepare_input(&mut self) -> Result<(), &'static str> {
        if matches!(self.command, Some(Command::Serve(_)))
            && (self.prompt.is_some()
                || self.endpoint.is_some()
                || self.host_workspace
                || self.usage_jsonl.is_some()
                || self.input_format != InputFormat::Text
                || self.operation.is_some()
                || self.agent.is_some()
                || self.resume)
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
        if event_options
            && (self.host_workspace
                || !matches!(
                    self.command,
                    None | Some(Command::Run | Command::Exec { .. })
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
        // Each writer replaces its own file whole; one shared destination
        // would let a later writer erase an earlier one's committed content.
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
                "output files: --output, --event-cursor and --usage-jsonl must name distinct files; recovery: acyclic run --output-format jsonl --operation OPERATION --output final.json --event-cursor cursor.json -p 'TASK'",
            );
        }
        Ok(())
    }
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the GraphCoder preset through the shared conversation driver.
    Run,
    /// Attach the terminal to an existing canonical agent path.
    Attach { path: String },
    /// Execute through the same conversation driver used by prompt mode.
    Exec {
        /// One task; omitted reads JSONL requests from stdin.
        prompt: Option<String>,
    },
    /// Read one lazy page of the selected session's direct children.
    Sessions(Page),
    /// Read one immutable page of the selected session's saved turns.
    History(Page),
    /// Serve this harness's agent tree through the harness server
    /// (`acyclic_harness::server::serve`), as a managed host does; prints its
    /// endpoint, credential and principal as one JSON line.
    Serve(Serve),
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
    fn product_commands_reject_execution_inputs_before_attach_or_serve() {
        let mut run = Args::try_parse_from([
            "acyclic",
            "run",
            "--template",
            "graphcoder",
            "--workspace",
            "source",
            "-p",
            "task",
        ])
        .unwrap();
        run.prepare_input().unwrap();
        assert_eq!(run.workspace, PathBuf::from("source"));
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
        assert!(
            Args::try_parse_from(["graphcoder", "--host-state-directory", "fixture-state"])
                .is_err()
        );
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

    #[test]
    fn removed_host_and_fixture_entrypoints_cannot_be_selected() {
        for option in [
            "--session",
            "--prepare-scenario",
            "--fixture",
            "--headless",
            "--resume",
        ] {
            assert!(
                Args::try_parse_from(["graphcoder", option, "unused"]).is_err(),
                "{option}"
            );
        }
    }
}
