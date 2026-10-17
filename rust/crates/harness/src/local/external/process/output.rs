//! Process output files are accepted under the process task's current fence.
use super::*;
use crate::task_volume::ToolFile;

pub(super) struct Output {
    pub owner: Arc<crate::durable_host::TaskJournalOwner<LocalStream>>,
    pub volumes: Arc<Volumes>,
    pub call: OperationId,
    pub sequence: (u64, u64),
    pub runtime: tokio::runtime::Handle,
    pub failed: Option<Error>,
}

impl Output {
    pub fn key(&self, file: ToolFile) -> Result<FileKey> {
        Ok(FileKey::Task {
            task: Component::new(self.owner.task_id().to_string()).map_err(storage)?,
            file: TaskFile::Tool {
                turn: 0,
                call: Component::new(self.call.to_string()).map_err(storage)?,
                file,
            },
        })
    }

    pub async fn publish(
        &self,
        file: ToolFile,
        bytes: impl Into<acyclic_stream::OwnedBytes>,
        settlement: bool,
    ) -> Result<()> {
        let file = crate::task_volume::FileValue::new(self.key(file)?, bytes);
        crate::external_executor::publication::publish(
            &self.volumes.results,
            &file,
            self.owner
                .condition(
                    if settlement {
                        crate::distributed::JournalWrite::Settlement
                    } else {
                        crate::distributed::JournalWrite::Fresh
                    },
                    &mut None,
                )
                .await?,
            None,
        )
        .await
    }

    pub fn observe(
        &mut self,
        chunk: acyclic_native_runtime::ProcessOutput<'_>,
    ) -> std::io::Result<()> {
        let (stdout, bytes) = match chunk {
            acyclic_native_runtime::ProcessOutput::Stdout(bytes) => (true, bytes),
            acyclic_native_runtime::ProcessOutput::Stderr(bytes) => (false, bytes),
        };
        // Split only an oversized physical read. No event waits for a later read.
        for bytes in bytes.chunks(acyclic_stream::MAX_RECORD_BYTES / 4) {
            let index = if stdout {
                self.sequence.0
            } else {
                self.sequence.1
            };
            let file = if stdout {
                ToolFile::Stdout(index)
            } else {
                ToolFile::Stderr(index)
            };
            if let Err(error) = self
                .runtime
                .block_on(self.publish(file, bytes.to_vec(), false))
            {
                self.failed = Some(error);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "native output publication is uncertain",
                ));
            }
            if stdout {
                self.sequence.0 += 1;
            } else {
                self.sequence.1 += 1;
            }
        }
        Ok(())
    }
}
