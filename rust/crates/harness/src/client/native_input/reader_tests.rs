use super::Utf16;

fn decode(reads: &[&[u16]]) -> (String, bool) {
    let mut text = Utf16::default();
    let mut bytes = Vec::new();
    for units in reads {
        let mut out = vec![0; units.len() * 3 + 3];
        let written = text.decode(units, &mut out);
        bytes.extend_from_slice(&out[..written]);
    }
    (String::from_utf8(bytes).unwrap_or_default(), text.ended)
}

#[test]
fn console_text_keeps_a_surrogate_pair_split_across_reads() {
    let units: Vec<u16> = "héllo 😀\r\n".encode_utf16().collect();
    // The emoji's two units straddle the reads.
    let split = units.len() - 3;
    assert_eq!(
        decode(&[&units[..split], &units[split..]]),
        ("héllo 😀\r\n".into(), false)
    );
}

#[test]
fn negative_control_detects_per_read_decoding_of_a_split_pair() {
    let units: Vec<u16> = "😀".encode_utf16().collect();
    let naive = String::from_utf16_lossy(&units[..1]) + &String::from_utf16_lossy(&units[1..]);
    assert_ne!(naive, "😀");
}

#[test]
fn unpaired_surrogates_become_replacement_characters() {
    assert_eq!(decode(&[&[0xd800, 0x61]]), ("\u{fffd}a".into(), false));
    assert_eq!(decode(&[&[0xdc00, 0x61]]), ("\u{fffd}a".into(), false));
    assert_eq!(
        decode(&[&[0xd800, 0xd801, 0xdc00]]),
        ("\u{fffd}𐐀".into(), false)
    );
}

#[test]
fn ctrl_z_ends_console_input_after_the_text_before_it() {
    assert_eq!(decode(&[&[0x61, 0x1a, 0x62]]), ("a".into(), true));
    assert_eq!(decode(&[&[0xd83d], &[0x1a]]), ("\u{fffd}".into(), true));
}

/// Shutdown models over the production channel and pump. The cancel object
/// is modelled as the OS one behaves: level-triggered, so a cancel before
/// the read is admitted still ends it.
#[cfg(acyclic_harness_loom)]
mod model {
    use std::io;

    use loom::sync::{Arc, Condvar, Mutex};

    use super::super::{Channel, Source, pump};

    struct Cancel {
        set: Mutex<bool>,
        changed: Condvar,
        /// False models `CancelSynchronousIo` alone: a cancel issued before
        /// the read starts is lost.
        level: bool,
    }

    impl Cancel {
        fn signal(&self) {
            if self.level {
                *self.set.lock().unwrap() = true;
            }
            self.changed.notify_all();
        }

        fn wait(&self) {
            let mut set = self.set.lock().unwrap();
            while !*set {
                set = self.changed.wait(set).unwrap();
            }
        }
    }

    /// Reads `reads` bytes at once, then waits until cancelled.
    struct Input {
        reads: usize,
        cancel: Arc<Cancel>,
    }

    impl Source for Input {
        fn read_input(&mut self, bytes: &mut [u8]) -> io::Result<Option<usize>> {
            if self.reads == 0 {
                self.cancel.wait();
                return Ok(None);
            }
            self.reads -= 1;
            bytes[0] = 1;
            Ok(Some(1))
        }
    }

    /// Drop (close, then cancel) races the reader through `reads` reads.
    fn shutdown(reads: usize, notify: bool, level: bool) {
        loom::model(move || {
            let channel = Arc::new(Channel::new(1));
            let cancel = Arc::new(Cancel {
                set: Mutex::new(false),
                changed: Condvar::new(),
                level,
            });
            let mut input = Input {
                reads,
                cancel: Arc::clone(&cancel),
            };
            let reader = Arc::clone(&channel);
            let thread = loom::thread::spawn(move || pump(&mut input, &reader));
            if notify {
                channel.close();
            } else {
                channel.close_unnotified();
            }
            cancel.signal();
            thread.join().unwrap();
        });
    }

    #[test]
    fn drop_racing_the_first_read_admission_always_ends_the_reader() {
        shutdown(0, true, true);
    }

    #[test]
    fn drop_racing_a_full_queue_always_ends_the_reader() {
        // Two batches fill the queue; the third waits for space.
        shutdown(3, true, true);
    }

    fn wake_model(broken: bool) {
        loom::model(move || {
            let channel = Arc::new(Channel::new(1));
            let mut input = Input {
                reads: 1,
                cancel: Arc::new(Cancel {
                    set: Mutex::new(true),
                    changed: Condvar::new(),
                    level: true,
                }),
            };
            let reader = Arc::clone(&channel);
            let producer = loom::thread::spawn(move || pump(&mut input, &reader));
            let bytes = loom::future::block_on(Box::pin(async {
                let mut bytes = Vec::new();
                while let Some(batch) = std::future::poll_fn(|cx| {
                    if broken {
                        channel.poll_recv_split(cx)
                    } else {
                        channel.poll_recv(cx)
                    }
                })
                .await
                {
                    bytes.extend(batch.unwrap());
                }
                bytes
            }));
            assert_eq!(bytes, [1]);
            producer.join().unwrap();
        });
    }

    #[test]
    fn publication_and_finish_never_lose_the_consumers_wakeup() {
        wake_model(false);
    }

    #[test]
    fn shutdown_child() {
        match std::env::var("ACYCLIC_READER_SHUTDOWN_CHILD").as_deref() {
            Ok("unnotified") => shutdown(3, false, true),
            Ok("edge") => shutdown(0, true, false),
            Ok("waker") => wake_model(true),
            _ => {}
        }
    }

    /// A Loom deadlock can abort the process, so each control runs in a
    /// child of this test binary.
    fn child_deadlocks(mode: &str) -> io::Result<bool> {
        // libtest names omit the crate prefix, unlike module_path!().
        let module = module_path!()
            .split_once("::")
            .map_or(module_path!(), |(_, module)| module);
        let child = format!("{module}::shutdown_child");
        let output = std::process::Command::new(std::env::current_exe()?)
            .args(["--exact", &child, "--test-threads=1"])
            .env("ACYCLIC_READER_SHUTDOWN_CHILD", mode)
            .output()?;
        // The test harness reports a panic on stdout; an abort, on stderr.
        let report = [output.stdout, output.stderr].concat();
        Ok(!output.status.success() && String::from_utf8_lossy(&report).contains("deadlock"))
    }

    #[test]
    fn negative_control_detects_a_close_that_never_releases_a_full_queue() -> io::Result<()> {
        assert!(child_deadlocks("unnotified")?);
        Ok(())
    }

    #[test]
    fn negative_control_detects_split_waker_registration() -> io::Result<()> {
        assert!(child_deadlocks("waker")?);
        Ok(())
    }

    #[test]
    fn negative_control_detects_a_cancel_lost_before_read_admission() -> io::Result<()> {
        assert!(child_deadlocks("edge")?);
        Ok(())
    }
}

proptest::proptest! {
    #[test]
    fn utf16_chunks_match_lossy_decoding_through_eof(
        units in proptest::collection::vec(proptest::num::u16::ANY, 0..128),
        chunk in 1usize..16,
    ) {
        let units: Vec<_> = units.into_iter().take_while(|unit| *unit != 0x1a).collect();
        let mut decoder = Utf16::default();
        let mut output = Vec::new();
        let mut buffer = [0; 51];
        for part in units.chunks(chunk) {
            let count = decoder.decode(part, &mut buffer);
            output.extend_from_slice(&buffer[..count]);
        }
        let count = decoder.decode(&[], &mut buffer);
        output.extend_from_slice(&buffer[..count]);
        proptest::prop_assert_eq!(output, String::from_utf16_lossy(&units).into_bytes());
        proptest::prop_assert!(decoder.ended);
    }
}

#[test]
fn a_carried_high_surrogate_reserves_a_full_replacement_character() {
    let mut decoder = Utf16::default();
    let mut output = [0; 6];
    assert_eq!(decoder.decode(&[0xd800], &mut output), 0);
    let count = decoder.decode(&[0x800], &mut output);
    assert_eq!(count, 6);
    assert_eq!(&output, "\u{fffd}\u{800}".as_bytes());
    // The old bound allowed four bytes here; the required six expose it.
    assert!(count > 3 + 1);
}
