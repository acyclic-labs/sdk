use loom_lock_scan::scan;
use std::{
    io::{Read, Write},
    process::ExitCode,
};

fn main() -> ExitCode {
    let mut input = String::new();
    if std::io::stdin().read_to_string(&mut input).is_err() {
        return ExitCode::FAILURE;
    }
    let mut fields = input.split('\0');
    let mut output = String::new();
    while let (Some(path), Some(source)) = (fields.next(), fields.next()) {
        let verdict = match scan(source) {
            Ok(found) => u8::from(found).to_string(),
            Err(error) => format!("!{}", error.to_string().replace('\n', " ")),
        };
        output.push_str(&format!("{path}\t{verdict}\n"));
    }
    match std::io::stdout().write_all(output.as_bytes()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(_) => ExitCode::FAILURE,
    }
}
