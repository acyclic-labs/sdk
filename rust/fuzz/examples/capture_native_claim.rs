//! Capture persisted claim corpus bytes through the production writer.
#[path = "../../crates/native-runtime/tests/claim_codec/oracle.rs"]
mod native_claim_record;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = std::env::args_os().nth(1).ok_or(
        "capture-native-claim: missing corpus directory; recovery: pass the destination directory",
    )?;
    std::fs::create_dir_all(&directory)?;
    for (name, bytes) in native_claim_record::seeds() {
        native_claim_record::check(&bytes);
        native_claim_record::check_prefixes(&bytes);
        std::fs::write(std::path::Path::new(&directory).join(name), bytes)?;
    }
    Ok(())
}
