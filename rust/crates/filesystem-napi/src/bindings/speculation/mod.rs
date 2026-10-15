use super::*;

/// Native speculation admission status emitted by the Rust engine.
#[napi(string_enum = "kebab-case")]
pub enum NativeResidencyStatus {
    Admitted,
    Rejected,
}

/// Native promotion admission status emitted by the Rust engine.
#[napi(string_enum = "kebab-case")]
pub enum NativePromotionStatus {
    Satisfied,
    Planned,
    Rejected,
}

/// Hard policy for native local-residency prediction.
#[napi(object)]
pub struct NativeResidencySpeculationOptions {
    pub maximum_active_operations: NapiU32,
    pub maximum_active_bytes: BigInt,
    pub outcome_window: NapiU32,
    pub traffic_window: NapiU32,
    pub speculative_cost_basis_points: NapiU32,
    pub minimum_usefulness_samples: NapiU32,
    pub minimum_usefulness_basis_points: NapiU32,
}

/// Hard policy for native cross-location promotion planning.
#[napi(object)]
pub struct NativePromotionSpeculationOptions {
    pub maximum_active_operations: NapiU32,
    pub maximum_active_bytes: BigInt,
    pub maximum_active_cost_units: BigInt,
    pub maximum_residency_facts: NapiU32,
    pub maximum_destinations: NapiU32,
    pub maximum_accepted_tiers: NapiU32,
    pub outcome_window: NapiU32,
    pub minimum_usefulness_samples: NapiU32,
    pub minimum_usefulness_basis_points: NapiU32,
}

/// Complete policy for both native speculation engines.
#[napi(object)]
pub struct NativeSpeculationOptions {
    pub residency: NativeResidencySpeculationOptions,
    pub promotion: NativePromotionSpeculationOptions,
}

/// One authenticated foreground successor supplied to native speculation.
#[napi(object)]
pub struct NativeResidencyObservation {
    pub operation_id: Buffer,
    pub volume_id: Buffer,
    pub generation_id: Buffer,
    pub foreground_bytes: BigInt,
    pub object_id: Buffer,
    pub maximum_bytes: BigInt,
    pub reason: String,
}

/// Native residency admission result.
#[napi(object)]
pub struct NativeResidencyAdmission {
    pub status: NativeResidencyStatus,
    pub rejection: Option<String>,
}

/// Completed native residency execution.
#[napi(object)]
pub struct NativeResidencyExecution {
    pub object_bytes: BigInt,
    pub work_json: String,
}

/// Caller-observed exact object location used by native promotion planning.
#[napi(object)]
pub struct NativeObjectResidency {
    pub object_id: Buffer,
    pub location_id: Buffer,
    pub tier: String,
    pub source_priority: NapiU32,
}

/// Writable native promotion destination capability.
#[napi(object)]
pub struct NativePromotionDestination {
    pub location_id: Buffer,
    pub tier: String,
    pub writable: bool,
    pub maximum_object_bytes: BigInt,
    pub priority: NapiU32,
    pub cost_units_per_byte: BigInt,
}

/// Bounded native promotion planning result.
#[napi(object)]
pub struct NativePromotionAdmission {
    pub status: NativePromotionStatus,
    pub rejection: Option<String>,
    pub operation_id: Option<Buffer>,
    pub object_id: Option<Buffer>,
    pub source_location_id: Option<Buffer>,
    pub destination_location_id: Option<Buffer>,
    pub estimated_cost_units: Option<BigInt>,
}

/// Identities terminalized by native preemption or generation replacement.
#[napi(object)]
pub struct NativeSpeculationPreemption {
    pub residency_operation_ids: Vec<Buffer>,
    pub promotion_operation_ids: Vec<Buffer>,
}

/// Native owner of one volume generation's two canonical speculation engines.
#[napi]
pub struct NativeSpeculation {
    pub(in crate::bindings) fs: Arc<LocalFs>,
    pub(in crate::bindings) controller: tokio::sync::Mutex<SpeculationController>,
    pub(in crate::bindings) cancellation: CancellationToken,
}

#[napi]
impl NativeSpeculation {
    #[napi]
    pub async fn observe(
        &self,
        observation: NativeResidencyObservation,
    ) -> Result<NativeResidencyAdmission> {
        let operation_id = OperationId::from_bytes(fixed_16(&observation.operation_id)?);
        let volume_id = VolumeId::from_bytes(fixed_16(&observation.volume_id)?);
        let generation_id = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
            &observation.generation_id,
            "generation identity",
        )?));
        let hint = ResidencyHint {
            request: ObjectReadRequest {
                object_id: decode_object_id(&observation.object_id)?,
                maximum_bytes: bigint_u64(&observation.maximum_bytes, "maximumBytes")?,
            },
            reason: ResidencyReason::from_public_str(&observation.reason).ok_or_else(|| {
                Error::new(Status::InvalidArg, "unknown residency speculation reason")
            })?,
        };
        let admission = self
            .controller
            .lock()
            .await
            .observe_hint(
                operation_id,
                volume_id,
                generation_id,
                bigint_u64(&observation.foreground_bytes, "foregroundBytes")?,
                hint,
            )
            .map_err(napi_error)?;
        Ok(match admission {
            ResidencyAdmission::Admitted(_) => NativeResidencyAdmission {
                status: NativeResidencyStatus::Admitted,
                rejection: None,
            },
            ResidencyAdmission::Rejected(rejection) => NativeResidencyAdmission {
                status: NativeResidencyStatus::Rejected,
                rejection: Some(rejection.as_str().to_owned()),
            },
        })
    }

    #[napi]
    pub async fn execute_residency(
        &self,
        operation_id: Buffer,
    ) -> Result<NativeResidencyExecution> {
        let operation_id = OperationId::from_bytes(fixed_16(&operation_id)?);
        let controller = self.controller.lock().await;
        let permit = controller
            .active_residency_permit(operation_id)
            .ok_or_else(|| Error::new(Status::InvalidArg, "residency operation is not active"))?;
        let receipt = self
            .fs
            .execute_residency(
                controller.residency(),
                permit,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(NativeResidencyExecution {
            object_bytes: bigint(receipt.value),
            work_json: work_json(&receipt.work)?,
        })
    }

    #[napi]
    pub async fn finish_residency(&self, operation_id: Buffer, useful: bool) -> Result<()> {
        self.controller
            .lock()
            .await
            .finish_residency(OperationId::from_bytes(fixed_16(&operation_id)?), useful)
            .map_err(napi_error)
    }

    #[napi]
    pub async fn plan_promotion(
        &self,
        operation_id: Buffer,
        accepted_tiers: Vec<String>,
        residency: Vec<NativeObjectResidency>,
        destinations: Vec<NativePromotionDestination>,
    ) -> Result<NativePromotionAdmission> {
        let operation_id = OperationId::from_bytes(fixed_16(&operation_id)?);
        let mut controller = self.controller.lock().await;
        let permit = controller
            .active_residency_permit(operation_id)
            .ok_or_else(|| Error::new(Status::InvalidArg, "residency operation is not active"))?;
        let tiers = accepted_tiers
            .iter()
            .map(|tier| native_storage_tier(tier))
            .collect::<Result<Vec<_>>>()?;
        let residency = residency
            .iter()
            .map(native_object_residency)
            .collect::<Result<Vec<_>>>()?;
        let destinations = destinations
            .iter()
            .map(native_promotion_destination)
            .collect::<Result<Vec<_>>>()?;
        let admission = controller
            .plan_promotion(permit, tiers, &residency, &destinations)
            .map_err(napi_error)?;
        Ok(native_promotion_admission(admission))
    }

    #[napi]
    pub async fn finish_promotion(&self, operation_id: Buffer, useful: bool) -> Result<()> {
        self.controller
            .lock()
            .await
            .finish_promotion(OperationId::from_bytes(fixed_16(&operation_id)?), useful)
            .map_err(napi_error)
    }

    #[napi]
    pub async fn preempt_for_foreground(
        &self,
        foreground_bytes: BigInt,
    ) -> Result<NativeSpeculationPreemption> {
        let preemption = self
            .controller
            .lock()
            .await
            .preempt_for_foreground(bigint_u64(&foreground_bytes, "foregroundBytes")?)
            .map_err(napi_error)?;
        Ok(native_speculation_preemption(preemption))
    }

    #[napi]
    pub async fn replace_generation(
        &self,
        generation_id: Buffer,
    ) -> Result<NativeSpeculationPreemption> {
        let generation_id = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
            &generation_id,
            "generation identity",
        )?));
        let preemption = self
            .controller
            .lock()
            .await
            .replace_generation(generation_id)
            .map_err(napi_error)?;
        Ok(native_speculation_preemption(preemption))
    }

    #[napi]
    pub async fn metrics_json(&self) -> Result<String> {
        let metrics = self.controller.lock().await.metrics();
        serde_json::to_string(&serde_json::json!({
            "residency": native_residency_metrics_json(metrics.residency),
            "promotion": native_promotion_metrics_json(metrics.promotion),
        }))
        .map_err(napi_error)
    }

    #[napi]
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }
}

pub(in crate::bindings) fn native_speculation_options(
    options: &NativeSpeculationOptions,
) -> Result<SpeculationOptions> {
    Ok(SpeculationOptions {
        residency: ResidencySpeculatorOptions {
            maximum_active_operations: options.residency.maximum_active_operations.into_inner(),
            maximum_active_bytes: bigint_u64(
                &options.residency.maximum_active_bytes,
                "maximumActiveBytes",
            )?,
            outcome_window: options.residency.outcome_window.into_inner(),
            traffic_window: options.residency.traffic_window.into_inner(),
            speculative_cost_basis_points: u16_from_u32(
                options.residency.speculative_cost_basis_points.into_inner(),
                "speculativeCostBasisPoints",
            )?,
            minimum_usefulness_samples: options.residency.minimum_usefulness_samples.into_inner(),
            minimum_usefulness_basis_points: u16_from_u32(
                options
                    .residency
                    .minimum_usefulness_basis_points
                    .into_inner(),
                "residency minimumUsefulnessBasisPoints",
            )?,
        },
        promotion: PromotionSpeculatorOptions {
            maximum_active_operations: options.promotion.maximum_active_operations.into_inner(),
            maximum_active_bytes: bigint_u64(
                &options.promotion.maximum_active_bytes,
                "maximumActiveBytes",
            )?,
            maximum_active_cost_units: bigint_u64(
                &options.promotion.maximum_active_cost_units,
                "maximumActiveCostUnits",
            )?,
            maximum_residency_facts: options.promotion.maximum_residency_facts.into_inner(),
            maximum_destinations: options.promotion.maximum_destinations.into_inner(),
            maximum_accepted_tiers: options.promotion.maximum_accepted_tiers.into_inner(),
            outcome_window: options.promotion.outcome_window.into_inner(),
            minimum_usefulness_samples: options.promotion.minimum_usefulness_samples.into_inner(),
            minimum_usefulness_basis_points: u16_from_u32(
                options
                    .promotion
                    .minimum_usefulness_basis_points
                    .into_inner(),
                "promotion minimumUsefulnessBasisPoints",
            )?,
        },
    })
}

pub(in crate::bindings) fn native_object_residency(
    value: &NativeObjectResidency,
) -> Result<ObjectResidency> {
    Ok(ObjectResidency {
        object_id: decode_object_id(&value.object_id)?,
        location_id: StorageLocationId::from_bytes(fixed_16(&value.location_id)?),
        tier: native_storage_tier(&value.tier)?,
        source_priority: u16_from_u32(value.source_priority.into_inner(), "sourcePriority")?,
    })
}

pub(in crate::bindings) fn native_promotion_destination(
    value: &NativePromotionDestination,
) -> Result<PromotionDestination> {
    Ok(PromotionDestination {
        location_id: StorageLocationId::from_bytes(fixed_16(&value.location_id)?),
        tier: native_storage_tier(&value.tier)?,
        writable: value.writable,
        maximum_object_bytes: bigint_u64(&value.maximum_object_bytes, "maximumObjectBytes")?,
        priority: u16_from_u32(value.priority.into_inner(), "promotion priority")?,
        cost_units_per_byte: bigint_u64(&value.cost_units_per_byte, "costUnitsPerByte")?,
    })
}

pub(in crate::bindings) fn native_promotion_admission(
    value: PromotionAdmission,
) -> NativePromotionAdmission {
    match value {
        PromotionAdmission::Satisfied(residency) => NativePromotionAdmission {
            status: NativePromotionStatus::Satisfied,
            rejection: None,
            operation_id: None,
            object_id: Some(encode_object_id(residency.object_id)),
            source_location_id: Some(Buffer::from(residency.location_id.into_bytes().to_vec())),
            destination_location_id: None,
            estimated_cost_units: None,
        },
        PromotionAdmission::Planned(plan) => NativePromotionAdmission {
            status: NativePromotionStatus::Planned,
            rejection: None,
            operation_id: Some(Buffer::from(
                plan.candidate.operation_id.into_bytes().to_vec(),
            )),
            object_id: Some(encode_object_id(plan.candidate.request.object_id)),
            source_location_id: Some(Buffer::from(plan.source.location_id.into_bytes().to_vec())),
            destination_location_id: Some(Buffer::from(
                plan.destination.location_id.into_bytes().to_vec(),
            )),
            estimated_cost_units: Some(bigint(plan.estimated_cost_units)),
        },
        PromotionAdmission::Rejected(rejection) => NativePromotionAdmission {
            status: NativePromotionStatus::Rejected,
            rejection: Some(rejection.as_str().to_owned()),
            operation_id: None,
            object_id: None,
            source_location_id: None,
            destination_location_id: None,
            estimated_cost_units: None,
        },
    }
}

pub(in crate::bindings) fn native_speculation_preemption(
    value: acyclic_fs::SpeculationPreemption,
) -> NativeSpeculationPreemption {
    NativeSpeculationPreemption {
        residency_operation_ids: value
            .residency
            .into_iter()
            .map(|value| Buffer::from(value.into_bytes().to_vec()))
            .collect(),
        promotion_operation_ids: value
            .promotion
            .into_iter()
            .map(|value| Buffer::from(value.into_bytes().to_vec()))
            .collect(),
    }
}

pub(in crate::bindings) fn native_residency_metrics_json(
    value: acyclic_fs::ResidencyMetrics,
) -> serde_json::Value {
    serde_json::json!({
        "candidates": value.candidates.to_string(),
        "admitted": value.admitted.to_string(),
        "active": value.active.to_string(),
        "activeBytes": value.active_bytes.to_string(),
        "useful": value.useful.to_string(),
        "wasted": value.wasted.to_string(),
        "rejectedFence": value.rejected_fence.to_string(),
        "rejectedDuplicate": value.rejected_duplicate.to_string(),
        "rejectedCapacity": value.rejected_capacity.to_string(),
        "rejectedCost": value.rejected_cost.to_string(),
        "rejectedUsefulness": value.rejected_usefulness.to_string(),
    })
}

pub(in crate::bindings) fn native_promotion_metrics_json(
    value: acyclic_fs::PromotionMetrics,
) -> serde_json::Value {
    serde_json::json!({
        "candidates": value.candidates.to_string(),
        "satisfied": value.satisfied.to_string(),
        "planned": value.planned.to_string(),
        "active": value.active.to_string(),
        "activeBytes": value.active_bytes.to_string(),
        "activeCostUnits": value.active_cost_units.to_string(),
        "useful": value.useful.to_string(),
        "wasted": value.wasted.to_string(),
        "rejected": value.rejected.to_string(),
    })
}

pub(in crate::bindings) fn native_object_cache_options(
    options: NativeObjectCacheOptions,
) -> Result<ObjectCacheOptions> {
    let converted = ObjectCacheOptions {
        maximum_entries: options.maximum_entries.into_inner(),
        maximum_bytes: bigint_u64(&options.maximum_bytes, "maximumBytes")?,
        maximum_in_flight: options.maximum_in_flight.into_inner(),
        maximum_waiters_per_object: options.maximum_waiters_per_object.into_inner(),
    };
    drop(options);
    Ok(converted)
}
