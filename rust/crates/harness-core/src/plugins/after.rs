//! Pure after-hook delivery at the existing turn settlement boundary.
use super::{Enabled, MANIFEST, load};
use crate::{
    Error, Result,
    config::{self, Surface, Tree},
    layers::Layer,
};
use serde_json::Value;
use std::sync::Arc;

/// One admitted settlement delta and its original module/config identity.
#[derive(Clone, Debug, serde::Serialize)]
#[serde(transparent)]
pub struct AfterHook {
    #[serde(skip)]
    notices: Arc<[String]>,
    identity: [u8; 32],
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Effect {
    notice: Notice,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Notice {
    text: String,
    /// A settlement is delivered once per journaled turn, never periodically.
    wake: bool,
}

pub(super) fn evaluate(module: &config::Prepared, settings: &Value) -> Result<config::Evaluation> {
    module.evaluate(
        &serde_json::json!({
            "config": settings,
            "delivery": { "kind": "settlement", "path": "live/agents/self/status.json" }
        }),
        Surface::Compose,
    )
}

impl AfterHook {
    pub(super) fn new(evaluation: config::Evaluation) -> Result<Self> {
        // This source has one event vocabulary and no changing payload. Resolve
        // the pure callback once at admission, before any model request. Delivery
        // then borrows the admitted delta without an engine, clock or allocation.
        let identity = evaluation.identity();
        let evaluated = evaluation.into_result()?;
        let effects: Vec<Effect> = serde_json::from_value(evaluated.value)
            .map_err(|error| Error::Invalid(format!("after-hook result: {error}")))?;
        let notices = effects
            .into_iter()
            .map(|effect| {
                if !effect.notice.wake {
                    return Err(Error::Invalid(
                        "settlement after-hooks require waking notices".into(),
                    ));
                }
                Ok(effect.notice.text)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            notices: notices.into(),
            identity,
        })
    }

    /// Borrow the admitted delta. The executor's journaled stop continuation
    /// suppresses delivery while answering these notices.
    pub fn settlement(&self) -> &[String] {
        &self.notices
    }
}

/// Seed plugin admission shared by interactive, headless and served stock
/// compositions, without a role or user-surface filter.
pub async fn seed() -> Result<Arc<Enabled>> {
    static SEED: tokio::sync::OnceCell<Arc<Enabled>> = tokio::sync::OnceCell::const_new();
    Ok(Arc::clone(
        SEED.get_or_try_init(|| async {
            struct Seed;
            impl Tree for Seed {
                fn read<'a>(
                    &'a self,
                    path: &'a str,
                ) -> futures::future::BoxFuture<'a, Result<Option<acyclic_stream::OwnedBytes>>>
                {
                    Box::pin(async move {
                        Ok(crate::seed::file(path).map(acyclic_stream::OwnedBytes::from_static))
                    })
                }
            }
            Ok::<_, Error>(Arc::new(
                load(
                    &Seed,
                    &[Layer {
                        name: "seed",
                        generation: "",
                        bytes: crate::seed::file(MANIFEST),
                    }],
                    "seed",
                )
                .await?,
            ))
        })
        .await?,
    ))
}
