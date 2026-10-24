use super::*;
use std::sync::{Arc, Mutex};
use tracing::{
    Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{
    Layer,
    layer::{Context, SubscriberExt as _},
};

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<String>>>);
struct Fields<'a>(&'a mut Vec<String>);
impl Visit for Fields<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "kind" {
            self.0.push(value.into());
        }
    }
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}
impl<S: Subscriber> Layer<S> for Capture {
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        _: &tracing::span::Id,
        _: Context<'_, S>,
    ) {
        attrs.record(&mut Fields(&mut self.0.lock().unwrap()));
    }
}
fn emit(kind: &'static str) {
    let _span = tracing::info_span!("spawn boundary", kind);
}

async fn probe(propagate: bool) -> Vec<String> {
    let seen = Capture::default();
    let dispatch =
        acyclic_test_support::capture::dispatch(tracing_subscriber::registry().with(seen.clone()));
    async {
        let job = async {
            emit("async");
        };
        if propagate {
            spawn(job).await.unwrap();
        } else {
            tokio::spawn(job).await.unwrap();
        }
        let mut set = tokio::task::JoinSet::new();
        let job = async {
            emit("join_set");
        };
        if propagate {
            join_set(&mut set, job);
        } else {
            set.spawn(job);
        }
        set.join_next().await.unwrap().unwrap();
        if propagate {
            spawn_blocking(|| emit("blocking")).await.unwrap();
        } else {
            tokio::task::spawn_blocking(|| emit("blocking"))
                .await
                .unwrap();
        }
        let runtime = tokio::runtime::Handle::current();
        if propagate {
            runtime_blocking(&runtime, || emit("runtime_blocking"))
                .await
                .unwrap();
        } else {
            runtime
                .spawn_blocking(|| emit("runtime_blocking"))
                .await
                .unwrap();
        }
        if propagate {
            thread(|| emit("thread")).join().unwrap();
        } else {
            std::thread::spawn(|| emit("thread")).join().unwrap();
        }
        if propagate {
            named_thread(std::thread::Builder::new(), || emit("named_thread"))
                .unwrap()
                .join()
                .unwrap();
        } else {
            std::thread::Builder::new()
                .spawn(|| emit("named_thread"))
                .unwrap()
                .join()
                .unwrap();
        }
        std::thread::scope(|scope| {
            if propagate {
                scoped_thread(scope, || emit("scoped_thread"))
                    .join()
                    .unwrap();
            } else {
                scope.spawn(|| emit("scoped_thread")).join().unwrap();
            }
        });
    }
    .with_subscriber(dispatch)
    .await;
    let mut events = seen.0.lock().unwrap().clone();
    events.sort();
    events
}

#[tokio::test]
async fn every_spawn_boundary_preserves_the_callers_scoped_dispatcher() {
    assert_eq!(
        probe(true).await,
        [
            "async",
            "blocking",
            "join_set",
            "named_thread",
            "runtime_blocking",
            "scoped_thread",
            "thread"
        ]
    );
    assert!(
        probe(false).await.is_empty(),
        "raw-spawn mutation escaped the same event oracle"
    );
}
