use super::*;
use crate::meter::ReadBuffer;
use std::sync::Arc;

const STDERR_TAIL_BYTES: usize = 4096;

/// One bounded output pass, distinguishing quiescence from actual progress.
pub struct ProcessDrain {
    /// The pipe reached its terminal EOF.
    pub closed: bool,
    /// Actual bytes consumed, including those outside the retained tail.
    pub read_bytes: usize,
}

/// Small, stable storage for an originally funded process-tree owner.
///
/// Allocate this before attempting spawn. Storage and retirement use the original
/// invocation's meter and retain partial owners in the same allocation on
/// failure. Moving storage between host states never recharges or reallocates
/// the tree. Its contents retire before its credit. There is no public mutable
/// tree reference: extracting a tree would separate its storage from its credit.
///
/// ```compile_fail
/// use acyclic_native_runtime::ProcessTreeStorage;
/// fn escape(mut storage: ProcessTreeStorage) {
///     let _unfunded = storage.take();
/// }
/// ```
pub struct ProcessTreeStorage {
    slot: ReadBuffer<ProcessSlot>,
}

/// Bounded polling work borrowed from a retained process owner. The caller
/// funds each operation scope; storage and reserved retirement never move to it.
pub struct ProcessTreeIo<'a> {
    tree: &'a mut ProcessTree,
    meter: Arc<dyn crate::NativeReadMeter>,
    stderr: &'a mut ReadBuffer<u8>,
}

impl ProcessTreeIo<'_> {
    /// Queues input without transferring the native pipe or its owner.
    pub fn queue_stdin(&mut self, bytes: Vec<u8>) -> io::Result<()> {
        self.meter.check()?;
        self.tree.queue_stdin(bytes)
    }
    /// Advances bounded nonblocking input delivery.
    pub fn pump_stdin(&mut self) -> io::Result<bool> {
        self.tree.pump_stdin_controlled(Some(&self.meter))
    }
    /// Arms one wait for this process's next readiness; see
    /// [`ProcessTree::readiness`].
    pub fn readiness(&mut self) -> io::Result<ProcessReadiness> {
        self.tree.readiness()
    }
    /// Reads bounded stdout without waiting for data.
    pub fn read_stdout(&mut self, bytes: &mut [u8]) -> io::Result<Option<usize>> {
        self.tree
            .read_output_controlled(bytes, Some(self.meter.as_ref()), false)
    }
    /// Reads bounded stderr through the same cross-platform pipe implementation.
    pub fn read_stderr(&mut self, bytes: &mut [u8]) -> io::Result<Option<usize>> {
        self.tree
            .read_output_controlled(bytes, Some(self.meter.as_ref()), true)
    }
    /// Drains a bounded pass while retaining only the originally funded tail.
    /// Reports EOF and actual progress, even when stdout closed before stderr.
    pub fn drain_stderr(&mut self) -> io::Result<ProcessDrain> {
        let mut bytes = [0; 8192];
        let mut read_bytes = 0;
        for _ in 0..4 {
            let Some(count) = self.read_stderr(&mut bytes)? else {
                break;
            };
            if count == 0 {
                return Ok(ProcessDrain {
                    closed: true,
                    read_bytes,
                });
            }
            read_bytes += count;
            let chunk = bytes.get(..count).ok_or(io::ErrorKind::InvalidData)?;
            let capacity = STDERR_TAIL_BYTES;
            let keep = chunk.len().min(capacity);
            let remove = self
                .stderr
                .data
                .len()
                .saturating_add(keep)
                .saturating_sub(capacity);
            self.stderr.data.drain(..remove);
            self.stderr.data.extend_from_slice(
                chunk
                    .get(chunk.len() - keep..)
                    .ok_or(io::ErrorKind::InvalidData)?,
            );
        }
        Ok(ProcessDrain {
            closed: false,
            read_bytes,
        })
    }
    /// Observes direct-child exit while retaining descendant custody.
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.tree.try_wait(Some(self.meter.as_ref()))
    }
    /// Stops descendants through the retained containment owner.
    pub fn terminate_descendants(&mut self) -> io::Result<()> {
        self.tree
            .capture_terminate_descendants(Some(self.meter.as_ref()))
    }
}

struct ProcessSlot {
    tree: Option<ProcessTree>,
    retirement: Option<Retirement>,
    stderr: ReadBuffer<u8>,
}

/// Concrete kill and wait SDK calls are withheld before physical admission.
/// Cancellation cannot consume the credit needed to stop an abandoned owner.
pub(super) struct Retirement {
    pub(super) writes: crate::NativeOperationReservation<Arc<dyn crate::NativeReadMeter>>,
    pub(super) wait: crate::NativeOperationReservation<Arc<dyn crate::NativeReadMeter>>,
}

impl Retirement {
    fn new(meter: &Arc<dyn crate::NativeReadMeter>) -> io::Result<Self> {
        Ok(Self {
            writes: crate::NativeOperationReservation::new(
                Arc::clone(meter),
                crate::NativeOperationKind::Write,
                platform::RETIREMENT_WRITES,
            )?,
            wait: crate::NativeOperationReservation::new(
                Arc::clone(meter),
                crate::NativeOperationKind::Read,
                1,
            )?,
        })
    }
}

impl ProcessTreeStorage {
    /// Borrows live process I/O against one independently admitted work scope.
    /// This cannot replace the original allocation or its retirement reserves.
    pub fn io(&mut self, meter: &Arc<dyn crate::NativeReadMeter>) -> io::Result<ProcessTreeIo<'_>> {
        meter.check()?;
        let [slot] = self.slot.data.as_mut_slice() else {
            return Err(io::ErrorKind::InvalidData.into());
        };
        Ok(ProcessTreeIo {
            tree: slot.tree.as_mut().ok_or(io::ErrorKind::NotFound)?,
            meter: Arc::clone(meter),
            stderr: &mut slot.stderr,
        })
    }

    /// Bounded raw diagnostic tail. Decoding at the reporting boundary may be lossy.
    pub fn stderr_tail(&self) -> &[u8] {
        match self.slot.data.as_slice() {
            [slot] => &slot.stderr.data,
            _ => &[],
        }
    }

    /// Drains diagnostics even when a refused spawn acquired no native tree.
    pub fn drain_stderr(
        &mut self,
        meter: &Arc<dyn crate::NativeReadMeter>,
    ) -> io::Result<ProcessDrain> {
        if matches!(self.slot.data.as_slice(), [slot] if slot.tree.is_none()) {
            return Ok(ProcessDrain {
                closed: true,
                read_bytes: 0,
            });
        }
        self.io(meter)?.drain_stderr()
    }
    /// Funds the exact slot on the original invocation before allocating it.
    pub fn new(meter: &Arc<dyn crate::NativeReadMeter>) -> io::Result<Self> {
        let mut slot = ReadBuffer::new(1, meter)?;
        slot.data.push(ProcessSlot {
            tree: None,
            retirement: None,
            stderr: ReadBuffer::new(STDERR_TAIL_BYTES, meter)?,
        });
        Ok(Self { slot })
    }

    /// Starts at most one tree. A failed start can still retain physical work;
    /// that slot cannot be replaced by a later spawn, even after termination.
    pub fn spawn(
        &mut self,
        command: &mut Command,
        custody: Option<&ProcessCustody>,
    ) -> io::Result<()> {
        let meter = Arc::clone(self.slot.meter());
        let [state] = self.slot.data.as_mut_slice() else {
            return Err(io::ErrorKind::InvalidData.into());
        };
        if state.tree.is_some() || state.retirement.is_some() {
            return Err(io::ErrorKind::AlreadyExists.into());
        }
        state.retirement = Some(Retirement::new(&meter)?);
        let slot = &mut state.tree;
        match custody {
            Some(custody) => ProcessTree::spawn_in_custody_controlled(
                command,
                custody,
                Some(meter.as_ref()),
                slot,
            ),
            None => ProcessTree::spawn_controlled(command, Some(meter.as_ref()), slot),
        }
    }

    /// Captures through the same retained tree and original meter. Callers fund
    /// the supplied capture buffers before entering this operation.
    pub fn wait_with_output(
        &mut self,
        timeout: Duration,
        max_bytes: usize,
        input: Option<CaptureInput<'_>>,
        check: impl FnMut() -> io::Result<()>,
        observe: impl FnMut(ProcessOutput<'_>) -> io::Result<ProcessInput>,
        buffers: [Vec<u8>; 3],
    ) -> Result<Output, ProcessCaptureFailure> {
        let meter = Arc::clone(self.slot.meter());
        let tree = self
            .slot_mut()
            .map_err(ProcessCaptureFailure::before_capture)?
            .as_mut()
            .ok_or_else(|| ProcessCaptureFailure::before_capture(io::ErrorKind::NotFound.into()))?;
        tree.wait_with_output_controlled(
            timeout,
            max_bytes,
            input,
            check,
            observe,
            (Some(meter.as_ref()), Some(&meter)),
            Some(buffers),
            None,
        )
    }

    /// Cleans up using the original invocation. Failure preserves the tree and
    /// its allocation in this owner; it is not a quiescence acknowledgment.
    pub fn terminate(&mut self) -> io::Result<()> {
        let meter = Arc::clone(self.slot.meter());
        if let Some(tree) = self.slot_mut()?.as_mut() {
            tree.terminate(Some(meter.as_ref()))?;
        }
        Ok(())
    }

    /// Whether this owner has no remaining physical containment or record work.
    /// This observation alone does not authenticate a harness child-turn claim.
    #[must_use]
    pub fn is_reaped(&self) -> bool {
        matches!(self.slot.data.as_slice(), [slot] if slot.tree.as_ref().is_none_or(ProcessTree::is_reaped))
    }

    /// Queues one native prompt on the retained process without giving away its pipe.
    pub fn queue_stdin(&mut self, bytes: Vec<u8>) -> io::Result<()> {
        let meter = Arc::clone(self.slot.meter());
        self.io(&meter)?.queue_stdin(bytes)
    }

    /// Advances the shared stdin pump on the original invocation's meter.
    pub fn pump_stdin(&mut self) -> io::Result<bool> {
        let meter = Arc::clone(self.slot.meter());
        self.io(&meter)?.pump_stdin()
    }

    /// Refuses completion until all queued input has been delivered.
    pub fn require_stdin_complete(&self) -> io::Result<()> {
        let [
            ProcessSlot {
                tree: Some(tree), ..
            },
        ] = self.slot.data.as_slice()
        else {
            return Err(io::ErrorKind::NotFound.into());
        };
        tree.require_stdin_complete()
    }

    /// Reads bounded native output through the retained invocation's meter.
    pub fn read_stdout(&mut self, buffer: &mut [u8]) -> io::Result<Option<usize>> {
        let meter = Arc::clone(self.slot.meter());
        self.io(&meter)?.read_stdout(buffer)
    }

    /// Observes exit while retaining containment and recovery ownership.
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        let meter = Arc::clone(self.slot.meter());
        self.io(&meter)?.try_wait()
    }

    /// Stops descendants through the shared containment implementation.
    pub fn terminate_descendants(&mut self) -> io::Result<()> {
        let meter = Arc::clone(self.slot.meter());
        self.io(&meter)?.terminate_descendants()
    }

    fn slot_mut(&mut self) -> io::Result<&mut Option<ProcessTree>> {
        let [slot] = self.slot.data.as_mut_slice() else {
            return Err(io::ErrorKind::InvalidData.into());
        };
        Ok(&mut slot.tree)
    }
}

impl Drop for ProcessTreeStorage {
    fn drop(&mut self) {
        let [
            ProcessSlot {
                tree: Some(tree),
                retirement: Some(retirement),
                ..
            },
        ] = self.slot.data.as_mut_slice()
        else {
            return;
        };
        if !tree.is_reaped() {
            // The original work funds actual kill/wait attempts even after a
            // cancellation or exhausted allowance. Keep the durable record:
            // only the shared custody fence may declare descendants quiescent.
            tree.retire_owner(retirement);
        }
    }
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;
