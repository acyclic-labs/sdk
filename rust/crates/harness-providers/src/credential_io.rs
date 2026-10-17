//! Original invocation admission for bounded credential operations.
use crate::native;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use acyclic_harness::Result;
use serde_json::Value;

pub(super) struct CredentialIo<'a> {
    custody: Option<&'a mut NativeAllocation>,
}

impl<'a> CredentialIo<'a> {
    pub fn new(custody: Option<&'a mut NativeAllocation>) -> Result<Self> {
        let mut io = Self { custody };
        io.claim(8192, 16, WorkCounters::UNCHARGED)?;
        Ok(io)
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
