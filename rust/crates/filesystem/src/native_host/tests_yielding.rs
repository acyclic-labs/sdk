use super::*;
    #[test]
    fn a_rejected_oplock_request_drops_without_waiting() -> std::io::Result<()> {
        let file = super::YieldingFile::new(std::fs::File::open("NUL")?, &mut super::ordinary_work()).map_err(super::ordinary_error)?;
        drop(file);
        Ok(())
    }
