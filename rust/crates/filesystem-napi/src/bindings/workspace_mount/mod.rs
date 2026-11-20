use super::*;

/// Explicit native workspace mount policy.
#[napi(object)]
pub struct NativeWorkspaceMountOptions {
    pub writable: bool,
    pub subdirectory: String,
    pub publication: String,
}

/// Retryable owner of one process-local native workspace mount.
#[napi]
pub struct NativeWorkspaceMount {
    pub(in crate::bindings) inner:
        Arc<std::sync::Mutex<Option<acyclic_fs::Mount<LocalAuthorityBackend, LocalObjectBackend>>>>,
    pub(in crate::bindings) path: String,
}

/// Worker-thread publication or detach of one native workspace mount.
pub struct NativeWorkspaceMountTask {
    pub(in crate::bindings) inner:
        Arc<std::sync::Mutex<Option<acyclic_fs::Mount<LocalAuthorityBackend, LocalObjectBackend>>>>,
    pub(in crate::bindings) detach: bool,
}

#[napi]
impl NativeWorkspaceMount {
    #[must_use]
    #[napi(getter)]
    pub fn path(&self) -> String {
        self.path.clone()
    }

    #[must_use]
    #[napi]
    pub fn sync(&self) -> AsyncTask<NativeWorkspaceMountTask> {
        AsyncTask::new(NativeWorkspaceMountTask {
            inner: Arc::clone(&self.inner),
            detach: false,
        })
    }

    #[must_use]
    #[napi]
    pub fn unmount(&self) -> AsyncTask<NativeWorkspaceMountTask> {
        AsyncTask::new(NativeWorkspaceMountTask {
            inner: Arc::clone(&self.inner),
            detach: true,
        })
    }
}

impl Task for NativeWorkspaceMountTask {
    type Output = bool;
    type JsValue = bool;

    fn compute(&mut self) -> Result<Self::Output> {
        let mut guard = lock_state(&self.inner, "workspace mount lock")?;
        let Some(mount) = guard.as_mut() else {
            if self.detach {
                return Ok(false);
            }
            return Err(Error::new(
                Status::InvalidArg,
                "workspace mount is detached",
            ));
        };
        if self.detach {
            mount.unmount_blocking().map_err(napi_error)?;
            guard.take();
        } else {
            mount.sync_blocking().map_err(napi_error)?;
        }
        Ok(true)
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(output)
    }
}
