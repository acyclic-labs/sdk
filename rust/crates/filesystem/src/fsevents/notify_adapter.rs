use super::*;

/// Translates one event's flags as the `notify` `FSEvents` backend does.
pub(super) fn translate(flags: u32) -> Vec<Event> {
    let has = |flag| flags & flag != 0;
    let mut events = Vec::new();
    if has(HISTORY_DONE) {
        return events;
    }
    if has(MUST_SCAN_SUBDIRS) {
        let event = Event::new(EventKind::Other).set_flag(Flag::Rescan);
        events.push(if has(USER_DROPPED) {
            event.set_info("rescan: user dropped")
        } else if has(KERNEL_DROPPED) {
            event.set_info("rescan: kernel dropped")
        } else {
            event
        });
    }
    if has(ROOT_CHANGED) {
        events.push(
            Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::From)))
                .set_info("root changed"),
        );
    }
    if has(MOUNT) {
        events.push(Event::new(EventKind::Create(CreateKind::Other)).set_info("mount"));
    }
    if has(UNMOUNT) {
        events.push(Event::new(EventKind::Remove(RemoveKind::Other)).set_info("mount"));
    }
    let link_info = || {
        if has(IS_SYMLINK) {
            Some("is: symlink")
        } else if has(IS_HARDLINK) {
            Some("is: hardlink")
        } else if has(ITEM_CLONED) {
            Some("is: clone")
        } else {
            None
        }
    };
    if has(ITEM_CREATED) {
        events.push(if has(IS_DIR) {
            Event::new(EventKind::Create(CreateKind::Folder))
        } else if has(IS_FILE) {
            Event::new(EventKind::Create(CreateKind::File))
        } else {
            link_info().map_or_else(
                || Event::new(EventKind::Create(CreateKind::Any)),
                |info| Event::new(EventKind::Create(CreateKind::Other)).set_info(info),
            )
        });
    }
    if has(ITEM_REMOVED) {
        events.push(if has(IS_DIR) {
            Event::new(EventKind::Remove(RemoveKind::Folder))
        } else if has(IS_FILE) {
            Event::new(EventKind::Remove(RemoveKind::File))
        } else {
            link_info().map_or_else(
                || Event::new(EventKind::Remove(RemoveKind::Any)),
                |info| Event::new(EventKind::Remove(RemoveKind::Other)).set_info(info),
            )
        });
    }
    if has(ITEM_RENAMED) {
        events.push(Event::new(EventKind::Modify(ModifyKind::Name(
            RenameMode::Any,
        ))));
    }
    if has(INODE_META_MOD) {
        events.push(Event::new(EventKind::Modify(ModifyKind::Metadata(
            MetadataKind::Any,
        ))));
    }
    if has(FINDER_INFO_MOD) {
        events.push(
            Event::new(EventKind::Modify(ModifyKind::Metadata(MetadataKind::Other)))
                .set_info("meta: finder info"),
        );
    }
    if has(ITEM_CHANGE_OWNER) {
        events.push(Event::new(EventKind::Modify(ModifyKind::Metadata(
            MetadataKind::Ownership,
        ))));
    }
    if has(ITEM_XATTR_MOD) {
        events.push(Event::new(EventKind::Modify(ModifyKind::Metadata(
            MetadataKind::Extended,
        ))));
    }
    if has(ITEM_MODIFIED) {
        events.push(Event::new(EventKind::Modify(ModifyKind::Data(
            DataChange::Content,
        ))));
    }
    if has(OWN_EVENT) {
        events = events
            .into_iter()
            .map(|event| event.set_process_id(std::process::id()))
            .collect();
    }
    events
}

/// An `FSEvents` watcher over every watched path, restarted as they change.
pub struct FsEventsWatcher {
    handler: Arc<Mutex<dyn EventHandler>>,
    paths: Vec<PathBuf>,
    recursive: HashMap<PathBuf, bool>,
    stream: Option<EventStream>,
}

impl std::fmt::Debug for FsEventsWatcher {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FsEventsWatcher")
            .field("paths", &self.paths)
            .field("recursive", &self.recursive)
            .field("running", &self.stream.is_some())
            .finish_non_exhaustive()
    }
}

impl FsEventsWatcher {
    fn restart(&mut self) -> notify::Result<()> {
        self.stream = None;
        if self.paths.is_empty() {
            return Ok(());
        }
        let (handler, recursive) = (Arc::clone(&self.handler), self.recursive.clone());
        let cancellation = crate::CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        self.stream = Some(
            EventStream::start(
                &self.paths,
                Arc::new(move |events: EventBatch<'_>| {
                    for (path, flags) in events.iter() {
                        let watched = recursive.iter().any(|(root, recursive)| {
                            path.starts_with(root)
                                && (*recursive
                                    || path == root.as_path()
                                    || path.parent() == Some(root.as_path()))
                        });
                        if !watched {
                            continue;
                        }
                        for event in translate(flags) {
                            let event = event.add_path(path.to_path_buf());
                            if let Ok(mut handler) = handler.lock() {
                                handler.handle_event(Ok(event));
                            }
                        }
                    }
                }),
                None,
                &mut work,
            )
            .map_err(notify_error)?,
        );
        Ok(())
    }
}

impl Watcher for FsEventsWatcher {
    fn new<F: EventHandler>(event_handler: F, _config: Config) -> notify::Result<Self> {
        // Resolved now, so a watcher that cannot run fails where it is made.
        EventStream::initialize_services().map_err(notify_error)?;
        Ok(Self {
            handler: Arc::new(Mutex::new(event_handler)),
            paths: Vec::new(),
            recursive: HashMap::new(),
            stream: None,
        })
    }

    fn watch(&mut self, path: &Path, recursive_mode: RecursiveMode) -> notify::Result<()> {
        if !path.exists() {
            return Err(Error::path_not_found().add_path(path.to_path_buf()));
        }
        let canonical = path.canonicalize()?;
        self.paths.push(path.to_path_buf());
        self.recursive
            .insert(canonical, recursive_mode == RecursiveMode::Recursive);
        self.restart()
    }

    fn unwatch(&mut self, path: &Path) -> notify::Result<()> {
        let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
        let watched = self.recursive.remove(&canonical).is_some();
        self.paths.retain(|candidate| candidate != path);
        self.restart()?;
        if watched {
            Ok(())
        } else {
            Err(Error::watch_not_found())
        }
    }

    fn kind() -> WatcherKind {
        WatcherKind::Fsevent
    }
}
