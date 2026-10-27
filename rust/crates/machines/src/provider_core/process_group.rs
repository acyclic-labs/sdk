//! POSIX process ownership for transports without a process-tree signal RPC.
//! Cancellation leaves a tombstone before reading the PID, so it also wins a
//! race with a delayed launch. Each command receives a fresh private directory.
use super::{ExecSession, cleanup_before_exit, collect_success};
use crate::{ExecCommand, ExecStream, ProviderError};
use futures::{StreamExt, TryStreamExt, future::BoxFuture, stream};
use std::sync::Arc;

/// Adapts one raw exec primitive to the common start, cancellation and cleanup
/// sequence. Input belongs only to the user command, never to maintenance.
pub fn process_group_io(
    command: &ExecCommand,
    input: Option<Vec<u8>>,
    transport: impl Fn(
        Vec<String>,
        u32,
        Option<Vec<u8>>,
    ) -> BoxFuture<'static, Result<ExecStream, ProviderError>>
    + Send
    + Sync
    + 'static,
) -> ExecSession {
    let transport = Arc::new(transport);
    let start = Arc::clone(&transport);
    let timeout = command.timeout_ms();
    process_group_session(
        command,
        move |argv| start(argv, timeout, input),
        move |argv| transport(argv, 10_000, None),
    )
}

/// Prepares cancellation before any transport request. A transport start may
/// have been accepted even if its response is lost. The cancellation tombstone
/// prevents that delayed launch from executing the user's command.
pub fn process_group_session(
    command: &ExecCommand,
    start: impl FnOnce(Vec<String>) -> BoxFuture<'static, Result<ExecStream, ProviderError>>
    + Send
    + 'static,
    maintenance: impl Fn(Vec<String>) -> BoxFuture<'static, Result<ExecStream, ProviderError>>
    + Send
    + Sync
    + 'static,
) -> ExecSession {
    let group = ProcessGroup::new();
    let argv = group.argv(command);
    let stop_argv = group.stop_argv();
    let cleanup_argv = group.cleanup_argv();
    let maintenance = Arc::new(maintenance);
    let stop_transport = Arc::clone(&maintenance);
    let exited_stop_argv = stop_argv.clone();
    let stop = Box::pin(async move {
        collect_success(
            stop_transport(stop_argv).await?,
            "guest process-group cancellation",
        )
        .await?;
        Ok(())
    });
    let cleanup = async move {
        collect_success(
            maintenance(exited_stop_argv).await?,
            "exited guest descendant cleanup",
        )
        .await?;
        collect_success(
            maintenance(cleanup_argv).await?,
            "guest exec metadata cleanup",
        )
        .await?;
        Ok(())
    };
    let output = stream::once(async move { start(argv).await })
        .try_flatten()
        .boxed();
    ExecSession {
        output: cleanup_before_exit(output, cleanup),
        stop,
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::{path::Path, time::Duration};

    fn output(argv: Vec<String>) -> std::io::Result<std::process::Output> {
        let executable = argv
            .first()
            .ok_or_else(|| std::io::Error::other("test argv missing"))?;
        let mut command = std::process::Command::new(executable);
        command.args(argv.iter().skip(1));
        acyclic_native_runtime::process_output(&mut command, Duration::from_secs(3), 65_536)
    }

    fn command(source: String) -> Result<ExecCommand, ProviderError> {
        ExecCommand::new(vec!["sh".into(), "-c".into(), source], 3_000)
    }

    #[test]
    fn session_launcher_preserves_binary_streams_and_nonzero_exit()
    -> Result<(), Box<dyn std::error::Error>> {
        let group = ProcessGroup::new();
        let result = output(group.argv(&command(
            "printf '\\000\\377'; printf 'err\\000' >&2; exit 7".into(),
        )?))?;
        assert_eq!(result.stdout, [0, 255]);
        assert_eq!(result.stderr, b"err\0");
        assert_eq!(result.status.code(), Some(7));
        assert!(output(group.cleanup_argv())?.status.success());
        Ok(())
    }

    #[test]
    fn cancellation_before_pid_publication_prevents_user_dispatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let group = ProcessGroup::new();
        let marker = format!("{}/executed", group.directory);
        assert!(output(group.stop_argv())?.status.success());
        let result = output(group.argv(&command(format!(
            "touch {}",
            super::super::shell_quote(&marker)
        ))?))?;
        assert_eq!(result.status.code(), Some(125));
        assert!(!Path::new(&marker).exists());
        assert!(output(group.cleanup_argv())?.status.success());
        Ok(())
    }

    #[test]
    fn root_exit_retires_descendants_that_keep_output_pipes_open()
    -> Result<(), Box<dyn std::error::Error>> {
        let group = ProcessGroup::new();
        let marker = format!("{}/leaked", group.directory);
        let argv = group.argv(&command(format!(
            "(sleep 0.2; touch {}) & printf root; exit 7",
            super::super::shell_quote(&marker)
        ))?);
        // Use plain OS pipe collection: the native runtime's tree cleanup
        // would otherwise mask a missing guest-side supervisor.
        let executable = argv.first().ok_or("missing supervisor executable")?;
        let result = std::process::Command::new(executable)
            .args(argv.iter().skip(1))
            .output()?;
        assert_eq!(result.status.code(), Some(7));
        assert_eq!(result.stdout, b"root");
        std::thread::sleep(Duration::from_millis(250));
        assert!(!Path::new(&marker).exists());
        assert!(output(group.cleanup_argv())?.status.success());
        Ok(())
    }

    #[test]
    fn supervisor_preserves_binary_stdin() -> Result<(), Box<dyn std::error::Error>> {
        use std::{io::Write, process::Stdio};
        let group = ProcessGroup::new();
        let argv = group.argv(&command("cat".into())?);
        let executable = argv.first().ok_or("missing supervisor executable")?;
        let mut child = std::process::Command::new(executable)
            .args(argv.iter().skip(1))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let mut input = child.stdin.take().ok_or("missing supervisor stdin")?;
        input.write_all(&[0, 255, 1, 2])?;
        drop(input);
        let result = child.wait_with_output()?;
        assert!(result.status.success());
        assert_eq!(result.stdout, [0, 255, 1, 2]);
        assert!(output(group.cleanup_argv())?.status.success());
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_stops_descendants() -> Result<(), Box<dyn std::error::Error>> {
        let group = ProcessGroup::new();
        let marker = format!("{}/executed", group.directory);
        let ready = format!("{}/ready", group.directory);
        let source = format!(
            "touch {}; (sleep 0.3; touch {}) & wait",
            super::super::shell_quote(&ready),
            super::super::shell_quote(&marker)
        );
        let argv = group.argv(&command(source)?);
        let child = tokio::task::spawn_blocking(move || output(argv));
        tokio::time::timeout(Duration::from_secs(1), async {
            while !Path::new(&ready).exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        assert!(output(group.stop_argv())?.status.success());
        assert!(!child.await??.status.success());
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert!(!Path::new(&marker).exists());
        assert!(output(group.cleanup_argv())?.status.success());
        Ok(())
    }
}

/// Guest-side process group and cancellation handshake. Requires `setsid` and
/// a POSIX shell in the registered image. Provider exec output stays unmodified.
pub struct ProcessGroup {
    directory: String,
}

impl Default for ProcessGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessGroup {
    /// Allocates an unguessable command identity, without launching anything.
    pub fn new() -> Self {
        Self {
            directory: format!("/tmp/acyclic-exec-{}", uuid::Uuid::new_v4()),
        }
    }

    /// Starts the command as a session leader, with a cancellation barrier.
    pub fn argv(&self, command: &ExecCommand) -> Vec<String> {
        let mut argv = vec!["sh".into(), "-c".into(),
            r#"umask 077
state=$1; shift
# Stay outside the user's process group so its root status survives cleanup.
# Descendants may retain stdout/stderr after their parent exits. Retire them
# here, before waiting for a provider's pipe EOF can hide the root exit.
exec 3<&0
setsid --wait sh -c '
state=$1; shift
mkdir -p "$state" || { printf "%s\n" "exec ownership setup failed; recovery: restore writable /tmp" >&2; exit 125; }
printf "%s\n" "$$" > "$state/pid" || { printf "%s\n" "exec PID publication failed; recovery: restore writable /tmp and destroy the machine" >&2; exit 125; }
test ! -e "$state/cancel" || { printf "%s\n" "exec launch cancelled; recovery: acquire fresh authority before dispatching a new command" >&2; exit 125; }
exec "$@"
' acyclic-exec "$state" "$@" <&3 3<&- &
child=$!
exec 3<&-
wait "$child"
status=$?
if test -r "$state/pid"; then
  read -r pid < "$state/pid"
  case "$pid" in ''|*[!0-9]*) printf '%s\n' 'exec exit PID admission failed; recovery: destroy the machine' >&2; exit 125;; esac
  /bin/kill -KILL -- "-$pid" 2>/dev/null || { if /bin/kill -0 -- "-$pid" 2>/dev/null; then printf '%s\n' 'exec exit group signal failed; recovery: destroy the machine' >&2; exit 125; fi; }
fi
exit "$status""#.into(), "acyclic-supervisor".into(), self.directory.clone()];
        argv.extend_from_slice(command.argv());
        argv
    }

    /// Tombstones a possibly delayed start, then signals its entire group.
    pub fn stop_argv(&self) -> Vec<String> {
        vec!["sh".into(), "-c".into(), r#"umask 077
state=$1
mkdir -p "$state" && : > "$state/cancel" || { printf '%s\n' 'exec cancellation barrier failed; recovery: restore writable /tmp and destroy the machine' >&2; exit 125; }
if test -r "$state/pid"; then
  read -r pid < "$state/pid"
  case "$pid" in ''|*[!0-9]*) printf '%s\n' 'exec PID admission failed; recovery: destroy the machine' >&2; exit 125;; esac
  /bin/kill -KILL -- "-$pid" 2>/dev/null || { if /bin/kill -0 -- "-$pid" 2>/dev/null; then printf '%s\n' 'exec group signal failed; recovery: destroy the machine' >&2; exit 125; fi; }
  # A signal acknowledgement does not prove cleanup. Ignore already dead
  # zombies while waiting for every live member of the owned process group.
  attempt=0
  while :; do
    live=false
    for entry in /proc/[0-9]*/stat; do
      if IFS= read -r stat 2>/dev/null < "$entry"; then
        fields=${stat##*) }
        read -r status parent group rest <<EOF
$fields
EOF
        if test "$group" = "$pid" && test "$status" != Z && test "$status" != X; then live=true; break; fi
      fi
    done
    "$live" || break
    attempt=$((attempt + 1))
    test "$attempt" -lt 200 || { printf '%s\n' 'exec group cleanup unconfirmed; recovery: destroy the machine' >&2; exit 125; }
    sleep 0.01
  done
fi"#.into(), "acyclic-stop".into(), self.directory.clone()]
    }

    /// Removes metadata after a confirmed, fully drained exit. The caller must
    /// never use this after an uncertain start or cancellation acknowledgement.
    pub fn cleanup_argv(&self) -> Vec<String> {
        vec![
            "rm".into(),
            "-rf".into(),
            "--".into(),
            self.directory.clone(),
        ]
    }
}
