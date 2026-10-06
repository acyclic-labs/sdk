#[path = "../qualification_receipt.rs"]
mod qualification_receipt;

use qualification_receipt::{Options, write};
use std::env;
use std::path::PathBuf;

fn main() {
    match parse().and_then(|options| write(&options)) {
        Ok(path) => println!("wrote Rust-owned qualification receipt {}", path.display()),
        Err(error) => {
            eprintln!("sdk-qualification-receipt: {error}");
            std::process::exit(2);
        }
    }
}

fn parse() -> Result<Options, String> {
    let mut values = env::args().skip(1);
    let mut source_root = None;
    let mut output = None;
    let mut scenario_log = None;
    let mut language = None;
    let mut tool = None;
    let mut suite = None;
    while let Some(flag) = values.next() {
        let slot = match flag.as_str() {
            "--source-root" => &mut source_root,
            "--output" => &mut output,
            "--scenario-log" => &mut scenario_log,
            "--language" => &mut language,
            "--tool" => &mut tool,
            "--suite" => &mut suite,
            "--help" | "-h" => return Err(usage()),
            other => return Err(format!("unknown argument {other}\n{}", usage())),
        };
        *slot = Some(values.next().ok_or_else(usage)?);
    }
    Ok(Options {
        source_root: PathBuf::from(source_root.ok_or_else(usage)?),
        output: PathBuf::from(output.ok_or_else(usage)?),
        scenario_log: PathBuf::from(scenario_log.ok_or_else(usage)?),
        language: language.ok_or_else(usage)?,
        tool: tool.ok_or_else(usage)?,
        suite: suite.ok_or_else(usage)?,
    })
}

fn usage() -> String {
    "usage: sdk-qualification-receipt --source-root PATH --output PATH --scenario-log PATH --language ID --tool NAME --suite NAME".into()
}
