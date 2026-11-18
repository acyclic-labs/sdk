use acyclic_native_runtime::{InheritedOutput, coordination_runtime};
use std::{
    fs::File,
    io::{self, Read as _, Seek as _},
    os::fd::FromRawFd as _,
};
use tokio::io::AsyncWriteExt as _;

#[path = "../../src/output/test_flags.rs"]
mod test_flags;
use test_flags::flags;

#[test]
fn synchronous_files_and_terminals_preserve_original_endpoint() -> io::Result<()> {
    coordination_runtime()?.block_on(async {
        // A regular file retains its original shared cursor and original
        // payload. Only the owned task may submit its synchronous write.
        let mut file = tempfile::tempfile()?;
        let original_flags = flags(&file)?;
        let mut output = InheritedOutput::from_file(file.try_clone()?)?;
        assert_eq!(
            output.write(b"borrowed").await.unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(flags(&file)?, original_flags);
        assert_eq!(file.metadata()?.len(), 0);
        let bytes = b"original redirected bytes".to_vec();
        let pointer = bytes.as_ptr();
        let (output, bytes) = output.write_owned(bytes).await?;
        assert_eq!(bytes.as_ptr(), pointer);
        assert_eq!(file.stream_position()?, bytes.len() as u64);
        let (output, _) = output.write_owned(b"\n").await?;
        drop(output);
        file.rewind()?;
        let mut actual = Vec::new();
        file.read_to_end(&mut actual)?;
        assert_eq!(actual, b"original redirected bytes\n");
        assert_eq!(flags(&file)?, original_flags);

        // Blocking terminals keep the original endpoint rather than reopening
        // a PTY master (which would create another endpoint).
        let mut master = -1;
        let mut slave = -1;
        #[expect(unsafe_code, reason = "opens the original real terminal pair")]
        // SAFETY: openpty initializes two independent owned descriptors on
        // success; null optional pointers request the native terminal defaults.
        let opened = unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null(),
                std::ptr::null(),
            )
        };
        if opened < 0 {
            return Err(io::Error::last_os_error());
        }
        #[expect(unsafe_code, reason = "owns both original terminal endpoints")]
        // SAFETY: each successful descriptor is transferred exactly once.
        let (terminal, mut peer) = unsafe { (File::from_raw_fd(master), File::from_raw_fd(slave)) };
        let original_flags = flags(&terminal)?;
        let output = InheritedOutput::from_file(terminal.try_clone()?)?;
        let bytes = b"same original terminal endpoint\n".to_vec();
        let pointer = bytes.as_ptr();
        let (output, bytes) = output.write_owned(bytes).await?;
        assert_eq!(bytes.as_ptr(), pointer);
        let mut actual = vec![0; bytes.len()];
        peer.read_exact(&mut actual)?;
        assert_eq!(actual, bytes);
        assert_eq!(flags(&terminal)?, original_flags);
        drop(output);
        Ok(())
    })
}
