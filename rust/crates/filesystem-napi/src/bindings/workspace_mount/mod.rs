use super::*;

pub(in crate::bindings) type NativeLocalWorkspaceMount =
    WorkspaceMount<LocalAuthorityBackend, LocalObjectBackend>;

/// Explicit native workspace mount policy.
#[napi(object)]
pub struct NativeWorkspaceMountOptions {
    /// Whether the mount accepts authored changes.
    pub writable: bool,
    /// Exact workspace directory projected as the mount root.
    pub subdirectory: String,
    /// `close-and-sync`, `per-mutation`, or `manual`.
    pub publication: String,
}

/// Retryable owner of one process-local native workspace mount.
#[napi]
pub struct NativeWorkspaceMount {
    pub(in crate::bindings) inner: Arc<std::sync::Mutex<Option<NativeLocalWorkspaceMount>>>,
    pub(in crate::bindings) path: String,
}

/// Worker-thread publication of one native workspace mount.
pub struct NativeWorkspaceMountSyncTask {
    pub(in crate::bindings) inner: Arc<std::sync::Mutex<Option<NativeLocalWorkspaceMount>>>,
}

/// Worker-thread publication and detach of one native workspace mount.
pub struct NativeWorkspaceUnmountTask {
    pub(in crate::bindings) inner: Arc<std::sync::Mutex<Option<NativeLocalWorkspaceMount>>>,
}

#[napi]
impl NativeWorkspaceMount {
    /// Exact native mount path.
    #[must_use]
    #[napi(getter)]
    pub fn path(&self) -> String {
        self.path.clone()
    }

    /// Publishes every pending authored effect without detaching.
    #[must_use]
    #[napi]
    pub fn sync(&self) -> AsyncTask<NativeWorkspaceMountSyncTask> {
        AsyncTask::new(NativeWorkspaceMountSyncTask {
            inner: Arc::clone(&self.inner),
        })
    }

    /// Publishes pending effects and detaches exactly once.
    #[must_use]
    #[napi]
    pub fn unmount(&self) -> AsyncTask<NativeWorkspaceUnmountTask> {
        AsyncTask::new(NativeWorkspaceUnmountTask {
            inner: Arc::clone(&self.inner),
        })
    }
}

impl Task for NativeWorkspaceMountSyncTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        let guard = lock_state(&self.inner, "workspace mount lock")?;
        guard
            .as_ref()
            .ok_or_else(|| Error::new(Status::InvalidArg, "workspace mount is detached"))?
            .sync_blocking()
            .map_err(napi_error)
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(output)
    }
}

impl Task for NativeWorkspaceUnmountTask {
    type Output = bool;
    type JsValue = bool;

    fn compute(&mut self) -> Result<Self::Output> {
        let mut guard = lock_state(&self.inner, "workspace mount lock")?;
        let Some(mount) = guard.as_mut() else {
            return Ok(false);
        };
        mount.unmount_blocking().map_err(napi_error)?;
        guard.take();
        Ok(true)
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(output)
    }
}
