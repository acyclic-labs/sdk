use super::{
    TestResult, matchers,
    notification_history::{self, Projection},
};
use acyclic_harness::client::write_terminal_notifications;

include!("../support/schema/notifications.rs");

impl Notifications {
    pub(super) async fn verify_history(&self) -> Vec<u8> {
        notification_history::verify(
            self.records
                .iter()
                .cloned()
                .map(|record| (record, false))
                .chain(
                    self.remove
                        .iter()
                        .map(|index| (self.records[*index].clone(), true)),
                ),
            (self.mode, self.protocol, self.focused),
            false,
        )
        .await
    }
    pub(super) async fn run(self) -> TestResult {
        let mut projection = Projection::new().await;
        for record in &self.records {
            projection.apply(record, false)?;
        }
        for index in self.remove {
            projection.apply(&self.records[index], true)?;
        }
        let mut output = Vec::new();
        let presented = write_terminal_notifications(
            projection.view.notifications(),
            &mut output,
            self.mode,
            self.protocol,
            self.focused,
        )?;
        if output != self.expect.output.as_bytes() {
            return Err(format!(
                "output: expected {:?}, got {:?}",
                self.expect.output, output
            )
            .into());
        }
        if presented != self.expect.presented {
            return Err(format!(
                "presented: expected {}, got {presented}",
                self.expect.presented
            )
            .into());
        }
        if let Some(expected) = self.expect.view {
            let actual = serde_json::to_value(projection.records())?;
            if !matchers::subset(&expected, &actual) {
                return Err(format!("view: expected {expected}, got {actual}").into());
            }
        }
        Ok(())
    }
}
