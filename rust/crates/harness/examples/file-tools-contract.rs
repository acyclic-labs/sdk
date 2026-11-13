//! Prints the native file-tool contract for current file-tool consumers.

#[path = "../tests/file_tools_contract.rs"]
mod file_tools_contract;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", file_tools_contract::contract()?);
    Ok(())
}
