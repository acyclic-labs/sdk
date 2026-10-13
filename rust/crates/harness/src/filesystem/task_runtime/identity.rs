//! Identity for the filesystem task runtime.

use super::*;

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn step_key(
    revision: u64,
    input: &mut acyclic_fs::NativeAllocation,
) -> Result<IdempotencyKey> {
    let built = (|| -> Result<_> {
        use std::fmt::Write as _;
        let io = crate::filesystem::PreparationIo::from_native_work(input.work())?;
        let hold = io.reserve_decode(32)?;
        io.allocation_attempt(32)?;
        let mut key = String::new();
        key.try_reserve_exact(32).map_err(Error::Allocation)?;
        if key.capacity() > 32 {
            io.retain_owned((key.capacity() - 32) as u64)?;
        }
        write!(&mut key, "task-step:{revision}").map_err(|_| Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "native task step identity formatting failed",
        })?;
        let retained = key.capacity() as u64;
        let key = IdempotencyKey::new(key)?;
        hold.into_retained();
        Ok((key, io.into_native_custody(retained)?))
    })();
    let (key, mut guard) = match built {
        Ok(value) => value,
        Err(Error::NativeWork { cause, custody }) => {
            let mut held = match input.split_off(input.bytes()) {
                Ok(held) => held,
                Err(_) => return Err(Error::NativeWork { cause, custody }),
            };
            if let Some(mut incoming) = custody {
                if !held.work().shares_ledger(incoming.work())
                    || held.absorb(&mut incoming).is_err()
                {
                    return Err(Error::NativeWork {
                        cause,
                        custody: Some(incoming),
                    });
                }
            }
            return Err(Error::NativeWork {
                cause,
                custody: Some(held),
            });
        }
        Err(error) => return Err(error),
    };
    input
        .absorb(&mut guard)
        .map_err(|cause| Error::NativeWork {
            cause,
            custody: Some(guard),
        })?;
    Ok(key)
}

pub(in crate::filesystem) fn execution_operation(
    task: TaskId,
    turn: OperationId,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<OperationId> {
    if let Some(io) = io {
        let hashed = acyclic_fs::WorkCounters {
            bytes_hashed: b"harness/v2/task-execution\0".len() as u64 + 32,
            ..Default::default()
        };
        io.preflight(hashed)?;
        io.charge(hashed)?;
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"harness/v2/task-execution\0");
    hasher.update(&task.into_bytes());
    hasher.update(&turn.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    Ok(OperationId::from_bytes(bytes))
}
