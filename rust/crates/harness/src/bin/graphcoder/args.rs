use clap::{Parser, ValueEnum};
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum InputFormat {
    #[default]
    Text,
    Jsonl,
}

#[derive(Debug, Parser)]
#[command(version, about = "Coding agent over the harness client")]
pub struct Args {
    #[arg(long, default_value = ".")]
    pub workspace: PathBuf,
    #[arg(long, env = "GRAPHCODER_MODEL", default_value = "openai/gpt-5")]
    pub model: String,
    #[arg(long)]
    pub catalog: Option<PathBuf>,
    #[arg(long)]
    pub credentials: Option<PathBuf>,
    #[arg(long)]
    pub settings_root: Option<PathBuf>,
    #[arg(short = 'p', long = "prompt", conflicts_with = "input_format")]
    pub prompt: Option<String>,
    #[arg(long, value_enum, default_value_t, requires_if("jsonl", "operation"))]
    pub input_format: InputFormat,
    #[arg(long)]
    pub operation: Option<String>,
    #[arg(long)]
    pub inline: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn headless_modes_use_explicit_jsonl_identity_and_reject_mixed_formats() {
        assert!(Args::try_parse_from(["graphcoder", "-p", "hello"]).is_ok());
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
    fn removed_host_and_fixture_entrypoints_cannot_be_selected() {
        for option in ["--session", "--prepare-scenario", "--fixture", "--headless"] {
            assert!(
                Args::try_parse_from(["graphcoder", option, "unused"]).is_err(),
                "{option}"
            );
        }
    }
}
