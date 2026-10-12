use super::*;

pub(crate) fn preparation_format(
    message: std::fmt::Arguments<'_>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<String> {
    let Some(work) = work.as_deref_mut() else {
        return Ok(message.to_string());
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(native) = work.original_native_work() {
        let (text, custody) = native.display(&message)?;
        work.absorb_native_custody(custody)?;
        return Ok(text);
    }
    struct Diagnostic<'a> {
        text: String,
        work: &'a mut dyn crate::store::history::HistoryIo,
        error: Option<Error>,
    }
    impl std::fmt::Write for Diagnostic<'_> {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            let result = (|| -> Result<()> {
                let length = self
                    .text
                    .len()
                    .checked_add(text.len())
                    .ok_or_else(|| Error::Invalid("schema diagnostic size overflow".into()))?;
                self.work
                    .codec_charge(JsonCodecCharge::Work { bytes: 0, items: 1 })?;
                self.work.admit_copy(text.len() as u64)?;
                self.work.reserve_text(&mut self.text, length)?;
                self.text.push_str(text);
                self.work.copy_bytes(text.len() as u64)
            })();
            if let Err(error) = result {
                self.error = Some(error);
                return Err(std::fmt::Error);
            }
            Ok(())
        }
    }
    let mut diagnostic = Diagnostic {
        text: String::new(),
        work,
        error: None,
    };
    if std::fmt::write(&mut diagnostic, message).is_err() {
        let Diagnostic { text, work, error } = diagnostic;
        let capacity = text.capacity() as u64;
        drop(text);
        let _ = work.release_owned(capacity);
        return Err(
            error.unwrap_or_else(|| Error::Invalid("schema diagnostic formatting failed".into()))
        );
    }
    Ok(diagnostic.text)
}
