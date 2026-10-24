// Shared callback boundaries for attached and detached checkout handles.
macro_rules! checkout_file_callbacks {
    () => {
        fn write_range_with_meter(
            &self,
            offset: u64,
            bytes: crate::kernel::OwnedSourceChunk,
            work: &crate::NativeMaterializationWork,
        ) -> Result<(), MountSourceError> {
            self.change_content(
                ContentChange::Write {
                    offset,
                    bytes: bytes.bytes,
                },
                work,
            )
        }

        fn append_range_with_meter(
            &self,
            offset: u64,
            bytes: crate::kernel::OwnedSourceChunk,
            work: &crate::NativeMaterializationWork,
        ) -> Result<(), MountSourceError> {
            self.change_content_at(
                ContentChange::Write {
                    offset,
                    bytes: bytes.bytes,
                },
                true,
                work,
            )
        }

        fn resize_with_meter(
            &self,
            logical_bytes: u64,
            work: &crate::NativeMaterializationWork,
        ) -> Result<(), MountSourceError> {
            self.change_content(ContentChange::Resize { logical_bytes }, work)
        }

        fn allocate_range_with_meter(
            &self,
            offset: u64,
            length: u64,
            operation: MountRangeAllocation,
            work: &crate::NativeMaterializationWork,
        ) -> Result<(), MountSourceError> {
            self.change_content(
                range_allocation(ByteRange { offset, length }, operation),
                work,
            )
        }

        fn lookup(&self) -> Result<MountLookup, MountSourceError> {
            boundary(&self.cancellation, |work| self.lookup_with_meter(work))
        }

        fn read_range(&self, offset: u64, length: u32) -> Result<OwnedBytes, MountSourceError> {
            boundary(&self.cancellation, |work| {
                self.read_range_with_meter(offset, length, work)
            })
            .map(|chunk| chunk.bytes)
        }

        fn read_up_to(
            &self,
            offset: u64,
            maximum_bytes: u32,
        ) -> Result<OwnedBytes, MountSourceError> {
            boundary(&self.cancellation, |work| {
                self.read_up_to_with_meter(offset, maximum_bytes, work)
            })
            .map(|chunk| chunk.bytes)
        }

        fn seek(
            &self,
            offset: u64,
            target: MountSeekTarget,
        ) -> Result<Option<u64>, MountSourceError> {
            boundary(&self.cancellation, |work| {
                self.seek_with_meter(offset, target, work)
            })
        }

        fn write_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
            boundary(&self.cancellation, |work| {
                self.change_content(
                    ContentChange::Write {
                        offset,
                        bytes: bytes.into(),
                    },
                    work,
                )
            })
        }

        fn append_range(&self, offset: u64, bytes: Bytes) -> Result<(), MountSourceError> {
            boundary(&self.cancellation, |work| {
                self.change_content_at(
                    ContentChange::Write {
                        offset,
                        bytes: bytes.into(),
                    },
                    true,
                    work,
                )
            })
        }

        fn resize(&self, logical_bytes: u64) -> Result<(), MountSourceError> {
            boundary(&self.cancellation, |work| {
                self.resize_with_meter(logical_bytes, work)
            })
        }

        fn allocate_range(
            &self,
            offset: u64,
            length: u64,
            operation: MountRangeAllocation,
        ) -> Result<(), MountSourceError> {
            boundary(&self.cancellation, |work| {
                self.allocate_range_with_meter(offset, length, operation, work)
            })
        }

        fn list_attributes(
            &self,
            cursor: Option<&[u8]>,
            maximum_entries: u32,
        ) -> Result<MountAttributePage, MountSourceError> {
            boundary(&self.cancellation, |work| {
                self.list_attributes_with_meter(cursor, maximum_entries, work)
            })
            .map(|(page, _)| page)
        }

        fn write_attribute(
            &self,
            name: &[u8],
            value: crate::kernel::OwnedSourceChunk,
            mode: MountAttributeWriteMode,
            work: &crate::NativeMaterializationWork,
        ) -> Result<(), MountSourceError> {
            let mode = match mode {
                MountAttributeWriteMode::Upsert => NamedAttributeWriteMode::Upsert,
                MountAttributeWriteMode::Create => NamedAttributeWriteMode::Create,
                MountAttributeWriteMode::Replace => NamedAttributeWriteMode::Replace,
            };
            invoke(&self.runtime, work, || {
                self.change_attribute(name, Some((value.bytes.as_ref(), mode)), work)
            })
        }

        fn remove_attribute(
            &self,
            name: &[u8],
            work: &crate::NativeMaterializationWork,
        ) -> Result<(), MountSourceError> {
            invoke(&self.runtime, work, || {
                self.change_attribute(name, None, work)
            })
        }
    };
}
