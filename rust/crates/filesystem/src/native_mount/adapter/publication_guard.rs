type SourcePublicationFuture = futures::future::Either<
    std::future::Ready<Result<(), MountSourceError>>,
    futures::future::Map<
        super::NativeMountCallbackFuture<'static>,
        fn(Result<(), NativeMountError>) -> Result<(), MountSourceError>,
    >,
>;

fn publication_result(result: Result<(), NativeMountError>) -> Result<(), MountSourceError> {
    result.map_err(|error| match error {
        NativeMountError::CallbackDenied { source, custody } => {
            MountSourceError::NativeCallbackDenied { source, custody }
        }
        NativeMountError::Materialization(error) => MountSourceError::NativeWork(error),
        NativeMountError::Source(error) => error,
        error => engine_error(error),
    })
}

pub(super) fn source_publication_guard(
    guard: Option<Arc<dyn super::NativeMountCallbackGuard>>,
    lifetime: Option<CancellationToken>,
) -> impl for<'m> FnMut(&'m crate::NativeMaterializationWork) -> SourcePublicationFuture
+ Clone
+ Send
+ 'static {
    move |work: &crate::NativeMaterializationWork| {
        use futures::FutureExt;
        let checked = if lifetime
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            Err(MountSourceError::Stale)
        } else {
            work.admit(crate::WorkCounters::UNCHARGED)
                .map_err(MountSourceError::NativeWork)
        };
        if let Err(error) = checked {
            return futures::future::Either::Left(std::future::ready(Err(error)));
        }
        let Some(guard) = guard.clone() else {
            return futures::future::Either::Left(std::future::ready(Ok(())));
        };
        let owned = work.clone();
        let lifetime = lifetime.clone();
        match super::NativeMountCallbackFuture::new_native_guarded(work, move || async move {
            if lifetime
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
            {
                return Err(NativeMountError::Source(MountSourceError::Stale));
            }
            guard.check_write(&owned)?.await?;
            if lifetime
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
            {
                return Err(NativeMountError::Source(MountSourceError::Stale));
            }
            owned.admit(crate::WorkCounters::UNCHARGED)?;
            Ok(())
        }) {
            Ok(future) => {
                futures::future::Either::Right(future.map(publication_result as fn(_) -> _))
            }
            Err(error) => {
                futures::future::Either::Left(std::future::ready(publication_result(Err(error))))
            }
        }
    }
}
