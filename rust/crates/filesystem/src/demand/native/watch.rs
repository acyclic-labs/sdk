/// The live watch of one native source.
struct NativeWatch(Arc<crate::source_watch::NativeSourceWatch>);

impl SourceWatch for NativeWatch {
    fn fence(&self) -> Result<(), DemandError> {
        self.0.fence().map_err(DemandError::Io)
    }

    fn is_exact(&self) -> bool {
        self.0.is_exact()
    }
}

/// Turns host reports into source changes: each path into its name in
/// the source's namespace and, where the host still names a node there,
/// that node. It refers to the source weakly, as the sink does to its
/// view: nothing a host keeps after the watch stops may hold the source.
struct NativeChanges {
    inner: std::sync::Weak<NativeDemandInner>,
    sink: Arc<dyn SourceChangeSink>,
}

impl NativeDemandInner {
    /// The longest prefix of `relative` the source can name, and whether
    /// that is all of it: a name it cannot represent was never read
    /// through it, but its directory changed with it.
    fn name(&self, relative: &Path) -> (NamespacePath, bool) {
        let limits = self.limits;
        let mut components = Vec::new();
        for component in relative.components() {
            let std::path::Component::Normal(name) = component else {
                break;
            };
            let Ok((encoding, bytes)) = crate::native_name::host_name_bytes(
                name,
                self.profile,
                limits.maximum_component_bytes,
            ) else {
                break;
            };
            let Ok(name) =
                crate::kernel::LogicalName::new(encoding, bytes, limits.maximum_component_bytes)
            else {
                break;
            };
            components.push(name);
        }
        let mut exact = components.len() == relative.components().count();
        loop {
            if let Ok(path) = NamespacePath::new(components.clone(), limits) {
                return (path, exact);
            }
            exact = false;
            components.pop();
        }
    }
}

impl crate::source_watch::HostChangeSink for NativeChanges {
    fn host_changed(&self, changes: &[crate::source_watch::HostChange]) {
        use crate::source_watch::HostChange;
        // A source already dropped has no view left to tell.
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut reported = Vec::with_capacity(changes.len() * 2);
        for change in changes {
            match change {
                HostChange::Everything => reported.push(SourceChange::Everything),
                #[cfg(any(target_os = "macos", windows))]
                HostChange::Unconfirmed => reported.push(SourceChange::Unconfirmed),
                #[cfg(windows)]
                HostChange::File(index) => reported.push(SourceChange::Node(
                    windows_file_identity(inner.root.identity().device, *index),
                )),
                HostChange::Rebound(relative) | HostChange::Altered(relative) => {
                    let (name, exact) = inner.name(relative);
                    // The name identifies its node only while it still
                    // binds it; NTFS reports the node itself as well.
                    let node = inner
                        .root
                        .stat(relative)
                        .ok()
                        .map(|metadata| file_identity(&metadata));
                    // An entry altered in place that can no longer be
                    // found may have been replaced meanwhile; the held
                    // root cannot be.
                    let altered = matches!(change, HostChange::Altered(_))
                        && exact
                        && (node.is_some() || name.is_root());
                    reported.push(if altered {
                        SourceChange::Entry(name)
                    } else if name.is_root() {
                        SourceChange::Everything
                    } else {
                        SourceChange::Name(name)
                    });
                    reported.extend(node.map(SourceChange::Node));
                }
            }
        }
        self.sink.source_changed(&reported);
    }
}
