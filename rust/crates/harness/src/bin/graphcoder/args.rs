use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum InputFormat {
    #[default]
    Text,
    Jsonl,
}

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Coding agent on the local harness's agents or an endpoint's"
)]
pub struct Args {
    /// Own workspace files for native coding hosts, without a provider or agent loop.
    #[arg(long, requires = "host_state_directory", conflicts_with_all = ["prompt", "model", "catalog", "credentials", "operation", "settings_root", "default_config", "usage_jsonl"])]
    pub host_workspace: bool,
    /// Explicit durable storage, opened only by this trusted harness owner.
    #[arg(long, requires = "host_workspace")]
    pub host_state_directory: Option<PathBuf>,
    #[arg(long, default_value = ".")]
    pub workspace: PathBuf,
    /// Instruction scope relative to the workspace root.
    #[arg(long)]
    pub directory: Option<String>,
    #[arg(long, env = "GRAPHCODER_MODEL")]
    pub model: Option<String>,
    #[arg(long)]
    pub catalog: Option<PathBuf>,
    #[arg(long)]
    pub credentials: Option<PathBuf>,
    #[arg(long)]
    pub settings_root: Option<PathBuf>,
    /// Use shipped defaults and explicit inputs only; ignore user settings,
    /// native credential discovery and environment model selection. Signing
    /// custody uses private files in this run's state directory.
    #[arg(long, conflicts_with = "endpoint")]
    pub default_config: bool,
    /// Export committed call usage for this conversation tree after headless
    /// settlement. A path is replaced with this snapshot; '-' writes JSONL to
    /// stdout and sends ordinary answers to stderr. Missing metrics stay null.
    #[arg(long, value_name = "PATH|-")]
    pub usage_jsonl: Option<PathBuf>,
    /// JSON approval policy; defaults to the shipped `GraphCoder` policy data.
    #[arg(long)]
    pub approval_policy: Option<PathBuf>,
    #[arg(short = 'p', long = "prompt", conflicts_with = "input_format")]
    pub prompt: Option<String>,
    #[arg(long, value_enum, default_value_t, requires_if("jsonl", "operation"))]
    pub input_format: InputFormat,
    /// Message identity: rerunning an operation replays its settled answers
    /// instead of sending its prompts again.
    #[arg(long)]
    pub operation: Option<String>,
    /// Reopen the last interactive conversation, including after a normal quit.
    #[arg(long, conflicts_with_all = ["prompt", "input_format", "host_workspace"])]
    pub resume: bool,
    #[arg(long)]
    pub inline: bool,
    /// Drive the agent tree an endpoint serves instead of this machine's
    /// harness: the given URL, else `ACYCLIC_ENDPOINT`, else the managed
    /// default, with `ACYCLIC_API_KEY` as the credential.
    #[arg(long, num_args = 0..=1, conflicts_with = "host_workspace")]
    pub endpoint: Option<Option<String>>,
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Serve this harness's agent tree through the harness server
    /// (`acyclic_harness::server::serve`), as a managed host does; prints its
    /// endpoint, credential and principal as one JSON line.
    Serve(Serve),
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
