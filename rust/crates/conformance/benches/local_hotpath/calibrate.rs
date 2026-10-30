use super::report::{Quantiles, Result, Terms, emit, ns};
use acyclic_native_runtime::{Durability, read_at, sync_data, sync_parent};
use divan::black_box;
use std::{
    fs::OpenOptions,
    io::{Seek, SeekFrom, Write},
    path::Path,
    sync::mpsc,
    time::Instant,
};

pub(super) struct Floors {
    flush: f64,
    wake: f64,
    call: f64,
    copy_per_byte: f64,
    hash_per_byte: f64,
    read: f64,
}

impl Floors {
    pub(super) fn compose(&self, terms: Terms) -> f64 {
        self.flush * f64::from(terms.flushes)
            + self.wake * f64::from(terms.wakes)
            + self.call * f64::from(terms.calls)
            + self.read * f64::from(terms.read_syscalls)
            + self.copy_per_byte * (f64::from(terms.copy_bytes) + f64::from(terms.encode_bytes))
            + self.hash_per_byte * f64::from(terms.hash_bytes)
    }
}

fn measured(name: &str, values: Vec<f64>, bytes: Option<u32>) -> Result<f64> {
    let quantiles = Quantiles::new(&mut values.clone())?;
    emit(serde_json::json!({
        "schema": 1, "kind": "calibration", "primitive": name,
        "quantiles": quantiles, "bytes": bytes, "raw_ns": values,
        "bytes_per_ns": bytes.map(|bytes| f64::from(bytes) / quantiles.p50_ns),
    }))?;
    Ok(quantiles.p50_ns)
}

#[inline(never)]
fn call(value: u64) -> u64 {
    black_box(value)
}

pub(super) fn run(root: &Path, samples: usize) -> Result<Floors> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(root.join("floor"))?;
    let source = vec![0x5a; 1024];
    file.write_all(&source)?;
    sync_data(&file, Durability::Full)?;
    sync_parent(&root.join("floor"), Durability::Full)?;
    let mut flush = Vec::with_capacity(samples);
    let mut read = Vec::with_capacity(samples);
    let mut copy = Vec::with_capacity(samples);
    let mut hash = Vec::with_capacity(samples);
    let mut calls = Vec::with_capacity(samples);
    let mut destination = vec![0; source.len()];
    for _ in 0..samples {
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&source)?;
        let start = Instant::now();
        sync_data(&file, Durability::Full)?;
        flush.push(ns(start.elapsed()));
        let start = Instant::now();
        let count = read_at(&file, 0, &mut destination)?;
        read.push(ns(start.elapsed()));
        if count != source.len() || destination != source {
            return Err("warm read calibration changed bytes".into());
        }
        let start = Instant::now();
        for _ in 0..4096 {
            black_box(&mut destination).copy_from_slice(black_box(&source));
        }
        copy.push(ns(start.elapsed()) / 4096.0);
        let start = Instant::now();
        for _ in 0..64 {
            black_box(blake3::hash(black_box(&source)));
        }
        hash.push(ns(start.elapsed()) / 64.0);
        let start = Instant::now();
        for value in 0..4096 {
            black_box(call(black_box(value)));
        }
        calls.push(ns(start.elapsed()) / 4096.0);
    }
    if wake(1, true).is_ok() {
        return Err("dropped wake producer was accepted as delivery".into());
    }
    emit(
        serde_json::json!({"schema": 1, "kind": "negative_control", "control": "dropped wake producer", "result": "typed channel error"}),
    )?;
    let wake = wake(samples, false)?;
    let mut creation = Vec::with_capacity(samples);
    for index in 0..samples {
        let path = root.join(format!("create-{index}"));
        let start = Instant::now();
        let mut created = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;
        created.write_all(&source)?;
        sync_data(&created, Durability::Full)?;
        sync_parent(&path, Durability::Full)?;
        creation.push(ns(start.elapsed()));
    }
    measured("F_create_write_flush_directory", creation, Some(1024))?;
    Ok(Floors {
        flush: measured("F_flush", flush, None)?,
        wake: measured("W_wake", wake, None)?,
        call: measured("C_call", calls, None)?,
        copy_per_byte: measured("B_copy", copy, Some(1024))? / 1024.0,
        hash_per_byte: measured("B_hash", hash, Some(1024))? / 1024.0,
        read: measured("R_sys", read, Some(1024))?,
    })
}

/// A rendezvous channel is an OS-backed cross-thread handoff. The receiver
/// timestamps delivery; the acknowledgement is outside the measured interval.
/// Sender/receiver loss is a channel error, including before the first sample.
fn wake(samples: usize, drop_delivery: bool) -> Result<Vec<f64>> {
    let (send, receive) = mpsc::sync_channel::<Instant>(0);
    let (ack, acknowledged) = mpsc::sync_channel(0);
    let thread = std::thread::spawn(move || {
        while let Ok(start) = receive.recv() {
            if drop_delivery {
                break;
            }
            let elapsed = ns(start.elapsed());
            if ack.send(elapsed).is_err() {
                break;
            }
        }
    });
    let outcome = (|| -> Result<Vec<f64>> {
        let mut values = Vec::with_capacity(samples);
        for _ in 0..samples {
            send.send(Instant::now())?;
            values.push(acknowledged.recv()?);
        }
        Ok(values)
    })();
    drop(send);
    thread
        .join()
        .map_err(|_| "wake calibration thread panicked")?;
    outcome
}
