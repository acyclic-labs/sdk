//! Original invocation admission for bounded credential operations.
use crate::native;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use acyclic_harness_core::Result;
use serde_json::Value;

pub(super) struct CredentialIo {
    custody: Option<NativeAllocation>,
}

impl CredentialIo {
    pub fn new(custody: Option<&mut NativeAllocation>) -> Result<Self> {
        // A zero-byte loan retains the original ledger without moving claims
        // backing other request data. Subsequent IO payload claims are owned.
        let custody = custody.map(|parent| parent.split_off(0)).transpose()?;
        let mut io = Self { custody };
        io.claim(8192, 16, WorkCounters::UNCHARGED)?;
        Ok(io)
    }

    pub fn finish(mut self, parent: Option<&mut NativeAllocation>) -> Result<()> {
        if let (Some(parent), Some(mut custody)) = (parent, self.custody.take()) {
            parent.absorb(&mut custody)?;
        }
        Ok(())
    }

    /// Transfers the original payload custody and serialization owner to the
    /// accepted physical operation. Observer cancellation cannot release either.
    pub async fn blocking<T: Send + 'static, S: Send + 'static>(
        &mut self,
        owner: &mut Option<S>,
        operation: impl FnOnce(&mut Self) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let mut io = Self {
            custody: self.custody.take(),
        };
        let retained = owner.take();
        let meter = io.work().map(|work| (work.runtime_meter(), false));
        let completion = acyclic_native_runtime::run_blocking_io(
            move || {
                let result = operation(&mut io);
                (result, io, retained)
            },
            meter,
        )
        .map_err(acyclic_fs::NativeTreeMaterializationError::Io)?;
        let (result, io, retained) = completion
            .await
            .map_err(acyclic_fs::NativeTreeMaterializationError::Io)?;
        *self = io;
        *owner = retained;
        result
    }

    pub fn work(&self) -> Option<&NativeMaterializationWork> {
        self.custody.as_ref().map(|custody| custody.work())
    }

    pub fn charge(&self, counters: WorkCounters) -> Result<()> {
        if let Some(work) = self.work() {
            work.charge(counters)?;
        }
        Ok(())
    }

    pub fn claim(&mut self, bytes: u64, operations: u64, counters: WorkCounters) -> Result<()> {
        if let Some(custody) = &mut self.custody {
            let mut admitted = custody.work().claim(bytes, operations, counters)?;
            custody.absorb(&mut admitted)?;
        }
        Ok(())
    }

    pub fn graph(&mut self, bytes: u64, nodes: u64) -> Result<()> {
        if let Some(custody) = &mut self.custody {
            let mut admitted = native::reserve(custody.work(), bytes, nodes, 0)?;
            custody.absorb(&mut admitted)?;
        }
        Ok(())
    }

    pub fn json(&mut self, bytes: &[u8]) -> Result<Value> {
        let (bytes_count, nodes) = native::shape(bytes);
        self.graph(bytes_count, nodes)?;
        self.checked(crate::json(bytes))
    }

    pub fn checked<T>(&mut self, result: Result<T>) -> Result<T> {
        match &mut self.custody {
            Some(custody) => native::diagnostic(result, custody),
            None => result,
        }
    }

    pub fn string(&mut self, text: &str) -> Result<String> {
        self.claim(
            text.len() as u64,
            1,
            WorkCounters {
                bytes_copied: text.len() as u64,
                ..WorkCounters::UNCHARGED
            },
        )?;
        Ok(text.to_owned())
    }

    pub fn bytes(&mut self, bytes: Vec<u8>) -> Result<bytes::Bytes> {
        match &mut self.custody {
            Some(custody) => {
                let payload = custody.split_off(bytes.capacity() as u64)?;
                crate::http::owned_bytes::owned(bytes, payload)
            }
            None => Ok(bytes.into()),
        }
    }
}
