//! Native filesystem operations for confined, cross-platform test fixtures.

use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;

use serde::Deserialize;

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Operation {
    LineCount {
        path: PathBuf,
        prefix: String,
    },
    Rename {
        from: PathBuf,
        to: PathBuf,
    },
    RemoveFile {
        path: PathBuf,
    },
    RemoveDirectory {
        path: PathBuf,
    },
    Write {
        path: PathBuf,
        text: String,
    },
    Fill {
        path: PathBuf,
        byte: u8,
        count: usize,
        offset: Option<u64>,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let plan = std::env::args_os().nth(1).ok_or("missing operation plan")?;
    let operations: Vec<Operation> = serde_json::from_reader(File::open(plan)?)?;
    for operation in operations {
        match operation {
            Operation::LineCount { path, prefix } => {
                println!(
                    "{prefix}{}",
                    fs::read_to_string(path)?.trim().lines().count()
                );
            }
            Operation::Rename { from, to } => fs::rename(from, to)?,
            Operation::RemoveFile { path } => fs::remove_file(path)?,
            Operation::RemoveDirectory { path } => fs::remove_dir(path)?,
            Operation::Write { path, text } => fs::write(path, text)?,
            Operation::Fill {
                path,
                byte,
                count,
                offset,
            } => {
                let mut file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(offset.is_none())
                    .open(path)?;
                file.seek(SeekFrom::Start(offset.unwrap_or(0)))?;
                file.write_all(&vec![byte; count])?;
            }
        }
    }
    Ok(())
}
