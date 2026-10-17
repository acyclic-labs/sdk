use std::cell::Cell;
loom::thread_local! { static OMIT_STOP: Cell<bool> = Cell::new(false); }

pub(super) fn omit_stop() -> bool {
    OMIT_STOP.with(Cell::get)
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use super::*;
    use crate::task_volume::{Component, FileKey, FileValue, TaskFile, TaskVolume};
    use acyclic_stream::{MemoryStream, StreamPath};
    use futures::FutureExt as _;
    use http_body_util::{BodyExt as _, Full};

    const _: fn(acyclic_stream::query_sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;

    fn response(broken: bool) {
        OMIT_STOP.with(|value| value.set(broken));
        let provider = Arc::new(MemoryStream::default());
        let files = Arc::new(loom::future::block_on(Box::pin(TaskVolume::open(
            provider, StreamPath::new("owner/control").unwrap(),
        ))).unwrap());
        let instance = uuid::Uuid::nil();
        let stop = files.clone();
        let original = Body::new(StreamBody::new(
            BodyStream::new(Full::new(bytes::Bytes::from_static(b"accepted")))
                .chain(futures::stream::pending()),
        ));
        let mut body = close_on_stop(http::Response::new(original), async move {
            super::super::super::server::stopped(&stop, instance).await
        }).into_body();
        let prefix = loom::future::block_on(Box::pin(body.frame())).unwrap().unwrap();
        assert_eq!(prefix.into_data().unwrap(), bytes::Bytes::from_static(b"accepted"));
        let writer = loom::thread::spawn(move || {
            let file = FileValue::new(FileKey::Task {
                task:Component::new("owner").unwrap(),
                file:TaskFile::Request(Component::new(instance.to_string()).unwrap()),
            }, b"{}".as_slice());
            loom::future::block_on(Box::pin(async {
                files.append(files.head().await.unwrap(), &file).await.unwrap();
            }));
        });
        let next = if broken {
            writer.join().unwrap();
            body.frame().now_or_never().expect("owner response remained open after stop")
        } else {
            let next = loom::future::block_on(Box::pin(body.frame()));
            writer.join().unwrap();
            next
        };
        assert!(next.is_none(), "owner response retained a stopped subscription");
    }

    #[test]
    fn accepted_stop_closes_an_idle_response_without_losing_its_prefix() {
        loom::model(|| response(false));
    }

    #[test]
    #[should_panic(expected = "owner response remained open after stop")]
    fn negative_control_detects_an_unbounded_response_lifetime() {
        loom::model(|| response(true));
    }
}
