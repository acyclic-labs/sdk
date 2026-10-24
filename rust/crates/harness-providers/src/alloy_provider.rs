//! A model policy routes concrete, journal-bound requests; it never selects in dispatch.
use super::*;
use acyclic_harness_core::{
    NativeWork,
    context::ModelTokenCount,
    model::{Model, ModelDispatch, ModelEvent, ModelProvider, ModelTurn, PreparedModelRequest},
};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream};
use futures::StreamExt as _;
use serde::{Serialize, Serializer, ser::SerializeMap as _};

impl ResolvedAlloy {
    fn member(&self, model: &Model, work: NativeWork<'_>) -> Result<&Arc<ResolvedModel>> {
        self.charge_selection(work)?;
        self.models
            .iter()
            .find(|member| member.model() == model)
            .ok_or_else(|| {
                selection_error(work, "recorded model is not a member of the bound alloy")
            })
    }

    fn charge_selection(&self, work: NativeWork<'_>) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = work {
            work.charge(acyclic_fs::WorkCounters {
                items_examined: self.models.len() as u64,
                bytes_hashed: if matches!(self.spec.select, crate::AlloySelection::Weighted { .. })
                {
                    16
                } else {
                    0
                },
                ..acyclic_fs::WorkCounters::UNCHARGED
            })?;
        }
        #[cfg(target_arch = "wasm32")]
        let _ = work;
        Ok(())
    }

    fn check_policy(&self, model: &Model, work: NativeWork<'_>) -> Result<()> {
        self.charge_selection(work)?;
        if model != &self.model {
            return Err(selection_error(work, "bound alloy policy changed"));
        }
        Ok(())
    }
}

impl ModelProvider for ResolvedAlloy {
    fn model_for_turn<'a>(
        &'a self,
        fallback: &'a Model,
        turn: ModelTurn<'_>,
        work: NativeWork<'_>,
    ) -> Result<&'a Model> {
        self.check_policy(fallback, work)?;
        Ok(self
            .select(AlloyTurn {
                operation_id: turn.operation_id,
                ordinal: turn.ordinal,
                role: turn.role,
            })
            .model())
    }

    fn recorded_model<'a>(
        &'a self,
        fallback: &'a Model,
        recorded: &Model,
        work: NativeWork<'_>,
    ) -> Result<&'a Model> {
        self.check_policy(fallback, work)?;
        Ok(self.member(recorded, work)?.model())
    }

    fn bind_content(
        &self,
        reader: Arc<dyn acyclic_harness_core::conversation::ContentResidencyVerifier>,
        maximum_bytes: u64,
    ) -> Option<Arc<dyn ModelProvider>> {
        let models = self
            .models
            .iter()
            .map(|member| {
                let mut member = member.as_ref().clone();
                member.files.reader = Some(Arc::clone(&reader));
                member.files.maximum_bytes = maximum_bytes.min(member.files.maximum_bytes);
                Arc::new(member)
            })
            .collect();
        Some(Arc::new(Self {
            spec: self.spec.clone(),
            models,
            capacity: self.capacity,
            model: self.model.clone(),
        }))
    }

    fn context_capacity(&self, model: &Model) -> Result<ModelContextCapacity> {
        if model != &self.model {
            self.member(model, None)?;
        }
        Ok(self.capacity)
    }

    fn count_tokens<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
    ) -> BoxProviderFuture<'a, Result<ModelTokenCount>> {
        Box::pin(async move {
            self.member(&request.request().model, None)?
                .count_tokens(request)
                .await
        })
    }

    fn count_policy_tokens<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        limits: acyclic_harness_core::conversation::Limits,
    ) -> BoxProviderFuture<'a, Result<ModelTokenCount>> {
        Box::pin(async move {
            self.check_policy(&request.request().model, None)?;
            let mut maximum: Option<ModelTokenCount> = None;
            for member in &self.models {
                let mut input = request.request().clone();
                input.model = member.model().clone();
                let input = PreparedModelRequest::prepare(input, limits, None)?;
                let count = member.count_tokens(&input).await?;
                count.validate(&input, &mut None)?;
                match maximum.as_mut() {
                    None => maximum = Some(count),
                    Some(maximum) => {
                        maximum.input_tokens = maximum.input_tokens.max(count.input_tokens);
                        maximum.fixed_tokens = maximum.fixed_tokens.max(count.fixed_tokens);
                        for (maximum, count) in
                            maximum.message_tokens.iter_mut().zip(count.message_tokens)
                        {
                            *maximum = (*maximum).max(count);
                        }
                    }
                }
            }
            let mut maximum = maximum.ok_or_else(|| invalid("alloy has no members"))?;
            maximum.request_digest = request.request_digest();
            Ok(maximum)
        })
    }

    fn generate<'a>(
        &'a self,
        request: PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> BoxProviderStream<'a, Result<ModelEvent>> {
        match self.member(&request.request().model, None) {
            Ok(member) => Box::pin(member.generate(request, dispatch).map(move |result| {
                result.and_then(|event| match event {
                    ModelEvent::Completed { metadata } => serde_json::to_value(Completion {
                        metadata: Metadata {
                            member: member.model(),
                            usage: &metadata,
                        },
                    })
                    .and_then(serde_json::from_value)
                    .map_err(|_| invalid("cannot retain alloy usage")),
                    event => Ok(event),
                })
            })),
            Err(error) => Box::pin(futures::stream::once(async { Err(error) })),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_model(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        self.charge_selection(Some(work))?;
        for member in &self.models {
            if !member.supports_native_model(work)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn context_capacity_with_native_work(
        &self,
        model: &Model,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<ModelContextCapacity> {
        self.charge_selection(Some(work))?;
        if model != &self.model {
            self.member(model, Some(work))?;
        }
        Ok(self.capacity)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn count_tokens_with_native_work<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        acyclic_harness_core::NativeFutureBundle<
            'a,
            (ModelTokenCount, acyclic_fs::NativeAllocation),
        >,
    > {
        self.member(&request.request().model, Some(work))?
            .count_tokens_with_native_work(request, work)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn generate_with_native_work<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        dispatch: ModelDispatch,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        acyclic_harness_core::NativeFutureBundle<
            'a,
            acyclic_harness_core::model::NativeModelStream<'a>,
        >,
    > {
        let member = self.member(&request.request().model, Some(work))?;
        let future = member.generate_with_native_work(request, dispatch, work)?;
        acyclic_harness_core::NativeFutureBundle::new(work, || async move {
            let stream = future.await?;
            acyclic_harness_core::model::NativeModelStream::new(work, || {
                stream.map(move |result| {
                    let (event, custody) = result?;
                    if let ModelEvent::Completed { metadata } = &event {
                        let io = acyclic_harness_core::filesystem::PreparationIo::from_native_work(
                            work,
                        )?;
                        let bytes = io.encode(
                            &Completion {
                                metadata: Metadata {
                                    member: member.model(),
                                    usage: metadata,
                                },
                            },
                            io.remaining()?.bytes_encoded,
                            true,
                        )?;
                        ModelEvent::decode_with_native_work(&bytes, work)
                    } else {
                        Ok((event, custody))
                    }
                })
            })
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn reconcile_with_native_work<'a>(
        &'a self,
        _: ModelDispatch,
        _: &'a [ModelEvent],
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        acyclic_harness_core::NativeFutureBundle<
            'a,
            (Option<Vec<ModelEvent>>, acyclic_fs::NativeAllocation),
        >,
    > {
        acyclic_harness_core::NativeFutureBundle::new(work, || async move {
            Ok((None, work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?))
        })
    }
}

fn selection_error(work: NativeWork<'_>, reason: &'static str) -> acyclic_harness_core::Error {
    #[cfg(not(target_arch = "wasm32"))]
    if work.is_some() {
        return acyclic_harness_core::Error::NativeBoundary {
            code: acyclic_harness_core::wire::ErrorCode::Invalid,
            reason,
        };
    }
    let _ = work;
    invalid(reason)
}

struct Member<'a>(&'a Model);
impl Serialize for Member<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(&format_args!("{}/{}", self.0.provider, self.0.name))
    }
}

struct Members<'a> {
    member: &'a Model,
    usage: &'a serde_json::Value,
}
impl Serialize for Members<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(&Member(self.member), self.usage)?;
        map.end()
    }
}

struct Metadata<'a> {
    member: &'a Model,
    usage: &'a serde_json::Value,
}
impl Serialize for Metadata<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        if let Some(usage) = self.usage.as_object() {
            for (key, value) in usage {
                if key != "member" && key != "members" {
                    map.serialize_entry(key, value)?;
                }
            }
        }
        map.serialize_entry("member", &Member(self.member))?;
        map.serialize_entry(
            "members",
            &Members {
                member: self.member,
                usage: self.usage,
            },
        )?;
        map.end()
    }
}

struct Completion<'a> {
    metadata: Metadata<'a>,
}
impl Serialize for Completion<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("kind", "completed")?;
        map.serialize_entry("metadata", &self.metadata)?;
        map.end()
    }
}
