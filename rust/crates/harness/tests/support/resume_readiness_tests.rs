use crate::TestResult;
use std::path::Path;

#[tokio::test(start_paused = true)]
async fn child_readiness_follows_the_childs_progress_not_the_parents_idle_budget() -> TestResult {
    use tokio::io::AsyncWriteExt as _;
    // A healthy child whose startup and journal I/O keep progressing for far
    // longer than any idle budget, then reports readiness and keeps running.
    let slow = |mut writer: tokio::io::DuplexStream| {
        super::child_turn(async move {
            for _ in 0..60 {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            writer
                .write_all(format!("noise\n{}\n", super::JOURNALED).as_bytes())
                .await?;
            std::future::pending().await
        })
    };
    let (writer, reader) = tokio::io::duplex(128);
    let mut child = std::pin::pin!(slow(writer));
    tokio::select! {
        ready = super::journaled(reader) => assert!(ready?, "the healthy child closed its stdout"),
        ended = &mut child => return Err(format!("killed a healthy child: {ended:?}").into()),
    }
    // Negative control: the former parent-side idle guard over the same pipe
    // reports the healthy child as stalled long before its readiness.
    let (writer, reader) = tokio::io::duplex(128);
    let mut child = std::pin::pin!(slow(writer));
    let former = crate::stall::guarded(
        Path::new("resume child"),
        crate::stall::waiting("journaled prefix", super::journaled(reader)),
    );
    tokio::select! {
        former = former => assert_eq!(
            former.expect_err("the former guard accepted the slow child"),
            "stalled: resume child at journaled prefix"
        ),
        ended = &mut child => return Err(format!("the slow child ended: {ended:?}").into()),
    }
    // A hung child is rejected by its own guard; its exit is the parent's EOF.
    let (writer, reader) = tokio::io::duplex(128);
    let hung = super::child_turn(async move {
        let _stdout = writer;
        std::future::pending().await
    });
    let (ready, ended) = tokio::join!(super::journaled(reader), hung);
    assert!(!ready?, "a hung child reported readiness");
    assert_eq!(
        ended.expect_err("a hung child kept running").to_string(),
        "stalled: resume child at admitted"
    );
    // A dead child closes its stdout without readiness.
    let (writer, reader) = tokio::io::duplex(128);
    drop(writer);
    assert!(!super::journaled(reader).await?);
    Ok(())
}
