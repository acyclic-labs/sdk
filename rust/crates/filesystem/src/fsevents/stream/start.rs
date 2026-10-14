use super::*;

impl EventStream {
    pub(crate) fn start<W: SourceDemandReadWork>(
        paths: &[PathBuf],
        handler: Arc<BorrowedEventHandlerFn>,
        published_bytes: Option<&AtomicU64>,
        work: &mut W,
    ) -> Result<Self, DemandError> {
        let api = Api::get(work)?;
        let mut ownership = Some(crate::demand::SourcePayloadOwnership::new(work)?);
        let (paths, path_bytes) = api.path_array(paths, work)?;
        let mut paths_live = true;
        let mut context = std::ptr::null::<StreamContext>();
        let mut context_bytes = 0;
        let mut stream = std::ptr::null_mut();
        let mut queue = std::ptr::null_mut();
        let mut started = false;
        let mut scheduled = false;
        let mut cleanup_reserved = false;
        let mut retained = false;
        let result = (|| {
            // Birth funds both ordinary drain and the same-queue self-drop
            // suffix. Whichever path does not run refunds its exact credit.
            work.check()?;
            work.with_original_leaf(|work| {
                crate::source_watch::reserve_cleanup_credits(
                    work,
                    WorkCounters {
                        backend_write_operations: 5,
                        backend_read_operations: 1,
                        ..WorkCounters::UNCHARGED
                    },
                )
            })?;
            cleanup_reserved = true;
            let requested_context_bytes = crate::source_watch::arc_control_bytes::<StreamContext>();
            work.claim_allocation(
                requested_context_bytes,
                u64::from((requested_context_bytes) != 0),
                crate::WorkCounters::UNCHARGED,
            )?;
            context_bytes = requested_context_bytes;
            context = Arc::into_raw(Arc::new(StreamContext {
                handler,
                terminal: std::cell::UnsafeCell::new(None),
            }));
            let stream_context = FsEventStreamContext {
                version: 0,
                info: context.cast_mut().cast(),
                retain: None,
                release: Some(release_context),
                copy_description: None,
            };
            native_allocate(work, 1)?;
            // SAFETY: a successful stream takes the sole raw context Arc and
            // retains the path array; a failed create takes neither.
            stream = unsafe {
                (api.stream_create)(
                    std::ptr::null(),
                    stream_callback,
                    &raw const stream_context,
                    paths,
                    EVENT_ID_SINCE_NOW,
                    0.0,
                    CREATE_FILE_EVENTS | CREATE_NO_DEFER,
                )
            };
            // SAFETY: path_array returned the owned CF reference released exactly once here.
            unsafe { (api.release)(paths) };
            paths_live = false;
            work.record(WorkCounters {
                backend_write_operations: 1,
                ..WorkCounters::UNCHARGED
            })?;
            if stream.is_null() {
                return Err(native_error("cannot create an FSEvents stream"));
            }
            native_allocate(work, 0)?;
            let custody = ownership.as_ref().expect("startup custody");
            custody.retain(path_bytes + context_bytes, work)?;
            retained = custody.has_native_custody();
            queue =
                // SAFETY: the label is terminated and null attributes select a serial queue.
                unsafe { dispatch_queue_create(c"acyclic.fsevents".as_ptr(), std::ptr::null()) };
            if queue.is_null() {
                return Err(native_error("cannot create the FSEvents dispatch queue"));
            }
            native_write(work, 0)?;
            // SAFETY: both handles are live; the stream retains the original callback queue.
            unsafe { (api.stream_set_dispatch_queue)(stream, queue) };
            scheduled = true;
            native_write(work, 0)?;
            if let Some(published) = published_bytes {
                published.store(path_bytes + context_bytes, Ordering::Release);
            }
            // SAFETY: the held stream has its live context and serial dispatch queue installed.
            if unsafe { (api.stream_start)(stream) } == 0 {
                return Err(native_error("cannot start an FSEvents stream"));
            }
            started = true;
            work.check()?;
            if !retained {
                work.release(path_bytes + context_bytes)?;
            }
            Ok(Self {
                api,
                stream,
                queue,
                context,
                terminal: None,
                started,
                scheduled,
                owned_bytes: path_bytes + context_bytes,
                cleanup: if work.is_native_original() {
                    (1, 5)
                } else {
                    (0, 0)
                },
                birth: work.native_source_work().map(|work| work.allocation_work()),
                ownership: ownership.take().expect("startup custody"),
            })
        })();
        if cleanup_reserved && !work.is_native_original() {
            work.with_original_leaf(|work| {
                crate::source_watch::cancel_cleanup_credits(
                    work,
                    WorkCounters {
                        backend_write_operations: 5,
                        backend_read_operations: 1,
                        ..WorkCounters::UNCHARGED
                    },
                )
            });
        }
        if result.is_err() {
            if paths_live {
                // SAFETY: paths_live retains the reference that no successful release consumed.
                unsafe { (api.release)(paths) };
                let _cleanup = work.record(WorkCounters {
                    backend_write_operations: 1,
                    ..WorkCounters::UNCHARGED
                });
            }
            if stream.is_null() {
                if !context.is_null() {
                    // SAFETY: failed creation left the original raw Arc ours.
                    drop(unsafe { Arc::from_raw(context) });
                }
                if cleanup_reserved && work.is_native_original() {
                    work.with_original_leaf(|work| {
                        crate::source_watch::cancel_cleanup_credits(
                            work,
                            WorkCounters {
                                backend_write_operations: 5,
                                backend_read_operations: 1,
                                ..WorkCounters::UNCHARGED
                            },
                        )
                    });
                }
            } else {
                let mut failed = Self {
                    api,
                    stream,
                    queue,
                    context,
                    terminal: None,
                    started,
                    scheduled,
                    owned_bytes: path_bytes + context_bytes,
                    ownership: if retained {
                        ownership.take().expect("retained failed stream custody")
                    } else {
                        Default::default()
                    },
                    cleanup: if cleanup_reserved && work.is_native_original() {
                        (1, 5)
                    } else {
                        (0, 0)
                    },
                    birth: work.native_source_work().map(|work| work.allocation_work()),
                };
                let _cleanup = failed.cleanup_admitted(work);
            }
            if let Some(published) = published_bytes {
                published.store(0, Ordering::Release);
            }
            if !retained {
                let _cleanup = work.release(path_bytes + context_bytes);
            }
        }
        result
    }
}
