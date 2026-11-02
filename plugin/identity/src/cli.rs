//! The command schema shared by execution, discovery and help.

use clap::{Args, CommandFactory as _, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "acyclic", disable_help_subcommand = true)]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
pub(super) enum Command {
    /// Sign in with browser approval.
    Login(Identity),
    /// Revoke the saved identity session.
    Logout(Identity),
    /// Register an agent identity.
    Signup(Signup),
    /// Show the current identity.
    Whoami(Identity),
    /// List organizations available to the current identity.
    Orgs(Identity),
    /// Renew the saved identity session.
    Refresh(Identity),
    #[command(hide = true)]
    Bearer(Bearer),
    #[command(hide = true)]
    IdentityMcp(Identity),
    /// Manage secrets through the issuing host's operation context.
    Secrets(Secrets),
}

#[derive(Args)]
pub(super) struct Identity {
    /// Public Edge origin (otherwise ACYCLIC_API_URL).
    #[arg(long)]
    pub endpoint: Option<String>,
    /// Private identity store (otherwise ACYCLIC_IDENTITY_FILE or the home store).
    #[arg(long)]
    pub credentials: Option<PathBuf>,
    /// Emit machine-readable results.
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub(super) struct Signup {
    #[command(flatten)]
    pub identity: Identity,
    /// Register an agent; browser identities use login.
    #[arg(long = "agent", required = true)]
    _agent: bool,
}

#[derive(Args)]
pub(super) struct Bearer {
    #[command(flatten)]
    pub identity: Identity,
    #[arg(long)]
    pub org: String,
    #[arg(long)]
    pub output: PathBuf,
}

#[derive(Args)]
pub(super) struct Secrets {
    #[command(subcommand)]
    pub operation: SecretCommand,
}

#[derive(Subcommand)]
pub(super) enum SecretCommand {
    /// Store a plain or protected secret.
    Put(Put),
    /// List one generation-fenced page.
    List(List),
    /// Delete a secret at its expected generation.
    Delete(Mutation),
}

#[derive(Args)]
pub(super) struct SecretScope {
    /// Public Edge origin (otherwise ACYCLIC_API_URL).
    #[arg(long)]
    pub endpoint: Option<String>,
    /// Owner-only bearer input file.
    #[arg(long)]
    pub bearer_file: PathBuf,
    #[arg(long)]
    pub scope: String,
    /// Emit machine-readable results.
    #[arg(long = "json")]
    _json: bool,
}

#[derive(Args)]
pub(super) struct Mutation {
    #[command(flatten)]
    pub scope: SecretScope,
    #[arg(long)]
    pub name: String,
    #[arg(long)]
    pub expected_generation: u64,
}

#[derive(Args)]
pub(super) struct Put {
    #[command(flatten)]
    pub mutation: Mutation,
    #[arg(long, value_enum)]
    pub kind: Kind,
    /// Read the value from this file instead of stdin.
    #[arg(long)]
    pub value_file: Option<PathBuf>,
}

#[derive(Clone, Copy, ValueEnum)]
pub(super) enum Kind {
    Plain,
    Protected,
}

#[derive(Args)]
pub(super) struct List {
    #[command(flatten)]
    pub scope: SecretScope,
    #[arg(long, requires = "after")]
    pub cursor_generation: Option<u64>,
    #[arg(long, requires = "cursor_generation")]
    pub after: Option<String>,
    /// Requested page size; zero uses the service's page size.
    #[arg(long, default_value_t = 0)]
    pub page_size: u32,
}

/// Identity's owning command schema, for composition into native CLI help.
///
/// Constructing or rendering this schema performs no environment, credential,
/// endpoint or network access. Hidden commands are internal discovery entries;
/// their presence does not authorize credential export.
#[must_use]
pub fn command() -> clap::Command {
    Arguments::command()
}

pub(super) fn is_command(name: &str) -> bool {
    Command::has_subcommand(name)
}

pub(super) fn parse(arguments: &[String]) -> Result<Command, clap::Error> {
    Arguments::try_parse_from(
        std::iter::once("acyclic").chain(arguments.iter().map(String::as_str)),
    )
    .map(|arguments| arguments.command)
}
