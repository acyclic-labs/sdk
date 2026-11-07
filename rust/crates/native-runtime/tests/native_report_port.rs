//! Replay the original native report writer through the shared decoder oracle.
#[path = "report_port/oracle.rs"]
mod oracle;

#[test]
fn original_report_writer_and_every_prefix_keep_one_admission() -> std::io::Result<()> {
    let request = include_bytes!("report_port/corpus/request");
    for end in 0..=request.len() {
        oracle::check(&request[..end])?;
    }
    // Repeated original line delimiters leave unread input after admission.
    // The observer must retire its writer rather than wait for a socket drain.
    oracle::check(&vec![
        *request.last().ok_or_else(
            || std::io::Error::other("captured report writer is empty")
        )?;
        4096
    ])
}
