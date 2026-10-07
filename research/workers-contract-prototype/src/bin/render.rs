use std::{env, io, path::PathBuf};

fn main() -> io::Result<()> {
    let output = env::args_os().nth(1).map(PathBuf::from).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "usage: render <output-root>")
    })?;
    workers_contract_prototype::render_proto_files(output)
}
