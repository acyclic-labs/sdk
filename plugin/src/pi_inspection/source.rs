use super::*;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiNativeInspectionCursor {
    pub(crate) snapshot: acyclic_fs::LazySnapshotRef,
    pub(crate) continuation: Vec<u8>,
}

#[derive(Serialize)]
pub(crate) struct PiNativeInspectionEntry {
    pub(crate) name: String,
    pub(crate) kind: acyclic_fs::demand::SourceNodeKind,
}

/// Frozen source and its SDK reader.
pub(crate) struct PiNativeSelectedSource {
    pub(crate) key: String,
    pub(crate) workspace: std::sync::Arc<crate::LocalLazyWorkspace>,
    pub(crate) context: std::sync::Arc<TaskContext>,
    pub(crate) owner: std::sync::Arc<TaskJournalOwner<LocalStream>>,
    pub(crate) volume: VolumeRef,
    pub(crate) snapshot: LazySnapshotRef,
    pub(crate) inspection: Inspection,
    pub(crate) _custody: (NativeAllocation, Option<NativeAllocation>),
}
impl PiNativeSelectedSource {
    pub(super) fn path(
        &self,
        path: &str,
        work: &NativeMaterializationWork,
    ) -> Result<(String, NativeAllocation)> {
        if self.context.durable_task_id() != Some(self.owner.task_id()) {
            return Err(selected_unauthorized(
                "selected reader names another caller",
            ));
        }
        if !path.is_empty() {
            let mut io = HistoryWork::new(PreparationIo::from_native_work(work)?);
            validate_content_path(path, &mut Some(&mut io))?;
        }
        Ok(work.display(&format_args!("/{path}"))?)
    }
    pub(super) async fn page(
        &self,
        directory: &str,
        continuation: Option<&[u8]>,
        maximum: u32,
        work: &NativeMaterializationWork,
    ) -> Result<(acyclic_fs::LazyDirectoryPage, NativeAllocation)> {
        let path = self.path(directory, work)?;
        let cancellation = work.cancellation();
        if maximum == 0 || maximum as usize > MAX_PRIVATE_DIRECTORY_PAGE {
            return Err(selected_invalid(
                "selected inspection page bound is invalid",
            ));
        }
        let continuation = match continuation {
            Some(bytes) => {
                if bytes.len() > crate::MAXIMUM_CONTROL_MESSAGE_BYTES {
                    return Err(selected_invalid(
                        "directory cursor exceeds its transport bound",
                    ));
                }
                Some(
                    acyclic_fs::LazyDirectoryCursor::decode_with_native_work(
                        bytes,
                        self.workspace.workspace().profile(),
                        self.workspace.workspace().limits(),
                        work,
                        cancellation,
                    )
                    .map_err(|(source, custody)| Error::NativeSource { source, custody })?,
                )
            }
            None => None,
        };
        self.inspection
            .list_native_directory_with_native_work(
                &self.workspace,
                self.snapshot,
                &path.0,
                continuation.map(|(cursor, _)| cursor),
                maximum,
                work,
            )
            .await
    }
    pub(crate) async fn inspect(
        &self,
        directory: &str,
        cursor: Option<&PiNativeInspectionCursor>,
        maximum_entries: u32,
        work: &NativeMaterializationWork,
    ) -> Result<(
        Vec<PiNativeInspectionEntry>,
        Option<PiNativeInspectionCursor>,
        NativeAllocation,
    )> {
        let mut io = HistoryWork::new(PreparationIo::from_native_work(work)?);
        self.owner.verify(false, &mut Some(&mut io)).await?;
        let cancellation = work.cancellation();
        if cursor.is_some_and(|cursor| cursor.snapshot != self.snapshot) {
            return Err(selected_invalid(
                "inspection cursor names another selection",
            ));
        }
        let page = self
            .page(
                directory,
                cursor.map(|c| c.continuation.as_slice()),
                maximum_entries,
                work,
            )
            .await?;
        let allocation = crate::pi_native::pi_native_vec(page.0.entries.len(), work)?;
        let mut output = (allocation.0, None, allocation.1);
        for entry in &page.0.entries {
            let (name, mut name_custody) = entry
                .name
                .unicode_text_with_native_work(work, cancellation)?;
            output.2.absorb(&mut name_custody)?;
            output.0.push(PiNativeInspectionEntry {
                name,
                kind: entry.kind,
            });
        }
        output.1 = match &page.0.next {
            Some(next) => {
                let (continuation, mut next_custody) = next
                    .encode_with_native_work(work, cancellation)
                    .map_err(|(source, custody)| Error::NativeSource { source, custody })?;
                output.2.absorb(&mut next_custody)?;
                Some(PiNativeInspectionCursor {
                    snapshot: self.snapshot,
                    continuation,
                })
            }
            None => None,
        };
        drop(page);
        self.owner.verify(false, &mut Some(&mut io)).await?;
        Ok(output)
    }
    pub(crate) async fn read(
        &self,
        path: &str,
        maximum_bytes: u64,
        work: &NativeMaterializationWork,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let path = self.path(path, work)?;
        self.inspection
            .read_native_path_with_native_work(
                &self.workspace,
                self.snapshot,
                &path.0,
                maximum_bytes.min(self.context.scope().limits().file_bytes),
                work,
            )
            .await
    }
    pub(crate) async fn lookup(
        &self,
        path: &str,
        work: &NativeMaterializationWork,
    ) -> Result<(acyclic_fs::LazyLookup, NativeAllocation)> {
        let path = self.path(path, work)?;
        self.inspection
            .lookup_native_path_with_native_work(&self.workspace, self.snapshot, &path.0, work)
            .await
    }
    pub(crate) async fn read_link(
        &self,
        path: &str,
        work: &NativeMaterializationWork,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let path = self.path(path, work)?;
        self.inspection
            .read_native_link_with_native_work(&self.workspace, self.snapshot, &path.0, work)
            .await
    }
}
