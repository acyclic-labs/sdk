use super::{ACYCLIC, BoundedOutput, command, isolated_state, output_with_stdin_timeout};
use super::process::try_output_with_timeout;
use acyclic_native_runtime::ProcessTree;
use serde_json::Value;
use std::{cell::RefCell, fs, path::{Path, PathBuf}, process::{Command, Output}, time::{Duration, Instant}};

pub struct ServiceGuard {
    home: PathBuf,
    identity: Option<String>,
    process_trees: RefCell<Vec<ProcessTree>>,
    active: bool,
}

impl ServiceGuard {
    pub fn new(home: &Path) -> Self {
        Self {
            home: home.to_path_buf(),
            identity: None,
            process_trees: RefCell::new(Vec::new()),
            active: true,
        }
    }

    pub fn attach_process_tree(&self, process_tree: ProcessTree) {
        self.process_trees.borrow_mut().push(process_tree);
    }

    /// Keep client containment alive until authenticated service drain. On
    /// Windows the service inherits the starting client's non-breakaway Job.
    pub fn output_with_stdin(&self, command: &mut Command, input: &[u8]) -> Output {
        let bounded = output_with_stdin_timeout(command, input, Duration::from_secs(120));
        self.attach_process_tree(bounded.process_tree);
        assert!(!bounded.expired, "service client exceeded its deadline");
        bounded.output
    }

    fn terminate_clients(&mut self) -> Result<(), String> {
        for tree in self.process_trees.get_mut().iter_mut() {
            tree.terminate(None).map_err(|error| error.to_string())?;
        }
        self.process_trees.get_mut().clear();
        Ok(())
    }

    pub fn assert_hook_service_live(&mut self) -> String {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut last = String::new();
        while Instant::now() < deadline {
            match self.status() {
                Ok(status) => {
                    let marker = status.get("markerIdentity").and_then(Value::as_str);
                    let reachable = status.get("reachableIdentity").and_then(Value::as_str);
                    if let Some(marker) = marker
                        && Some(marker) == reachable
                        && status.get("lockAcquirable").and_then(Value::as_bool) == Some(false)
                    {
                        let identity = marker.to_owned();
                        self.identity = Some(identity.clone());
                        return identity;
                    }
                    last = status.to_string();
                }
                Err(error) => last = error,
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        panic!("installed hook did not establish a reachable locked service: {last}");
    }

    pub fn drain(mut self) {
        if let Err(error) = self.cleanup() {
            panic!("service drain failed: {error}");
        }
    }

    pub fn assert_timeout_cleanup(mut self, process_tree: ProcessTree) {
        self.attach_process_tree(process_tree);
        let status = self.status().expect("timed-out host service status");
        let identity = status
            .get("markerIdentity")
            .and_then(Value::as_str)
            .expect("timed-out host started hook service");
        self.identity = Some(identity.to_owned());
        if let Err(error) = self.cleanup() {
            panic!("timed-out host service was not cleaned: {error}");
        }
    }

    fn cleanup(&mut self) -> Result<(), String> {
        let authenticated = match self.authenticated_identity() {
            Ok(authenticated) => authenticated,
            Err(authentication_error) => {
                if self.clear_dead_service_marker()? {
                    self.terminate_clients()?;
                    self.active = false;
                    return Ok(());
                }
                return Err(authentication_error);
            }
        };
        let Some(identity) = authenticated else {
            self.terminate_clients()?;
            self.active = false;
            return Ok(());
        };
        // A host can retain open handles below its native mount even after its direct process
        // exits. After authenticating the independently durable service, end the complete client
        // containment before asking it to synchronize and unmount; the reverse order can deadlock
        // Linux FUSE teardown.
        #[cfg(target_os = "linux")]
        self.terminate_clients()?;
        let mut drain = command(ACYCLIC);
        drain.arg("__service-drain").arg(&identity);
        isolated_state(&mut drain, &self.home);
        let BoundedOutput {
            output,
            expired,
            process_tree,
        } = try_output_with_timeout(&mut drain, Duration::from_secs(10))
            .map_err(|error| error.to_string())?;
        drop(process_tree);
        if expired || !output.status.success() {
            return Err(format!(
                "drain command expired={expired}: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        self.verify_drained(&identity)?;
        #[cfg(not(target_os = "linux"))]
        self.terminate_clients()?;
        self.active = false;
        Ok(())
    }

    fn clear_dead_service_marker(&self) -> Result<bool, String> {
        let Some(expected) = self.identity.as_deref() else {
            return Ok(false);
        };
        let status = self.status()?;
        if status.get("markerIdentity").and_then(Value::as_str) != Some(expected)
            || !status.get("reachableIdentity").is_some_and(Value::is_null)
            || status.get("lockAcquirable").and_then(Value::as_bool) != Some(true)
        {
            return Ok(false);
        }
        let mut cleanup = command(ACYCLIC);
        cleanup.arg("__service-drain").arg(expected);
        isolated_state(&mut cleanup, &self.home);
        let BoundedOutput {
            output,
            expired,
            process_tree,
        } = try_output_with_timeout(&mut cleanup, Duration::from_secs(10))
            .map_err(|error| error.to_string())?;
        drop(process_tree);
        if expired || !output.status.success() {
            return Err(format!(
                "dead-service cleanup expired={expired}: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        let status = self.status()?;
        Ok(status.get("markerIdentity").is_some_and(Value::is_null)
            && status.get("reachableIdentity").is_some_and(Value::is_null)
            && status.get("lockAcquirable").and_then(Value::as_bool) == Some(true))
    }

    fn authenticated_identity(&mut self) -> Result<Option<String>, String> {
        let status = self.status()?;
        let marker = status.get("markerIdentity").and_then(Value::as_str);
        let reachable = status.get("reachableIdentity").and_then(Value::as_str);
        let lock_acquirable = status.get("lockAcquirable").and_then(Value::as_bool);
        match (marker, reachable, lock_acquirable) {
            (Some(marker), Some(reachable), Some(false)) if marker == reachable => {
                if self
                    .identity
                    .as_deref()
                    .is_some_and(|expected| expected != marker)
                {
                    return Err(format!(
                        "reachable service identity changed before cleanup: {status}"
                    ));
                }
                let identity = marker.to_owned();
                self.identity = Some(identity.clone());
                Ok(Some(identity))
            }
            (None, None, Some(true)) => Ok(None),
            _ => Err(format!(
                "service identity could not be authenticated for cleanup: {status}"
            )),
        }
    }

    fn verify_drained(&self, identity: &str) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut last = String::new();
        while Instant::now() < deadline {
            match self.status() {
                Ok(status)
                    if status.get("markerIdentity").is_some_and(Value::is_null)
                        && status.get("reachableIdentity").is_some_and(Value::is_null)
                        && status.get("lockAcquirable").and_then(Value::as_bool) == Some(true) =>
                {
                    let drain: Value = serde_json::from_slice(
                        &fs::read(
                            service_data(&self.home)
                                .join("service-drain")
                                .join(format!("{identity}.json")),
                        )
                        .map_err(|error| error.to_string())?,
                    )
                    .map_err(|error| error.to_string())?;
                    if drain.get("identity").and_then(Value::as_str) == Some(identity)
                        && drain.get("ok").and_then(Value::as_bool) == Some(true)
                    {
                        return Ok(());
                    }
                    return Err(format!("durable drain evidence is invalid: {drain}"));
                }
                Ok(status) => last = status.to_string(),
                Err(error) => last = error,
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        Err(format!("service remained reachable or locked: {last}"))
    }

    fn status(&self) -> Result<Value, String> {
        service_status(&self.home)
    }
}

impl Drop for ServiceGuard {
    fn drop(&mut self) {
        if self.active {
            let cleanup = self.cleanup();
            if let Err(error) = cleanup {
                if std::thread::panicking() {
                    eprintln!("service cleanup failed during unwind: {error}");
                } else {
                    panic!("service cleanup failed: {error}");
                }
            }
        }
    }
}

pub fn assert_service_absent(home: &Path) -> Result<(), String> {
    let status = service_status(home)?;
    if status.get("markerIdentity").is_some_and(Value::is_null)
        && status.get("reachableIdentity").is_some_and(Value::is_null)
        && status.get("lockAcquirable").and_then(Value::as_bool) == Some(true)
    {
        Ok(())
    } else {
        Err(format!("unexpected hook service state: {status}"))
    }
}

fn service_status(home: &Path) -> Result<Value, String> {
    let mut status = command(ACYCLIC);
    status.arg("__service-status");
    isolated_state(&mut status, home);
    let BoundedOutput {
        output,
        expired,
        process_tree,
    } = try_output_with_timeout(&mut status, Duration::from_secs(5))
        .map_err(|error| error.to_string())?;
    drop(process_tree);
    if expired || !output.status.success() {
        return Err(format!(
            "service status expired={expired}: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    serde_json::from_slice(&output.stdout).map_err(|error| error.to_string())
}

fn service_data(home: &Path) -> PathBuf {
    if cfg!(windows) {
        home.join("local").join("Acyclic").join("state-v5")
    } else {
        home.join("state").join("acyclic").join("state-v5")
    }
}

