#[test]
fn readiness_callback_can_issue_synchronous_nfs_io() -> Result<(), Box<dyn std::error::Error>> {
    if super::skip_if_network_volume_data_denied() {
        return Ok(());
    }
    assert_eq!(super::mounted_io::mounted_io()?, 0);
    Ok(())
}
