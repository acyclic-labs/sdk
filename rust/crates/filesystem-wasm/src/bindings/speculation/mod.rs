use super::*;

/// Browser owner of one volume generation's residency and promotion engines.
#[wasm_bindgen]
pub struct BrowserSpeculation {
    pub(in crate::bindings) engine: Fs<BrowserAuthority, CachedObjectStore<BrowserObjects>>,
    pub(in crate::bindings) controller: std::cell::RefCell<SpeculationController>,
    pub(in crate::bindings) cancellation: CancellationToken,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::bindings) struct BrowserSpeculationOptions {
    pub(in crate::bindings) residency: BrowserResidencySpeculationOptions,
    pub(in crate::bindings) promotion: BrowserPromotionSpeculationOptions,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::bindings) struct BrowserResidencySpeculationOptions {
    pub(in crate::bindings) maximum_active_operations: u32,
    pub(in crate::bindings) maximum_active_bytes: u64,
    pub(in crate::bindings) outcome_window: u32,
    pub(in crate::bindings) traffic_window: u32,
    pub(in crate::bindings) speculative_cost_basis_points: u16,
    pub(in crate::bindings) minimum_usefulness_samples: u32,
    pub(in crate::bindings) minimum_usefulness_basis_points: u16,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::bindings) struct BrowserPromotionSpeculationOptions {
    pub(in crate::bindings) maximum_active_operations: u32,
    pub(in crate::bindings) maximum_active_bytes: u64,
    pub(in crate::bindings) maximum_active_cost_units: u64,
    pub(in crate::bindings) maximum_residency_facts: u32,
    pub(in crate::bindings) maximum_destinations: u32,
    pub(in crate::bindings) maximum_accepted_tiers: u32,
    pub(in crate::bindings) outcome_window: u32,
    pub(in crate::bindings) minimum_usefulness_samples: u32,
    pub(in crate::bindings) minimum_usefulness_basis_points: u16,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::bindings) struct BrowserResidencyObservation {
    pub(in crate::bindings) operation_id: Vec<u8>,
    pub(in crate::bindings) volume_id: Vec<u8>,
    pub(in crate::bindings) generation_id: Vec<u8>,
    pub(in crate::bindings) foreground_bytes: u64,
    pub(in crate::bindings) object_id: Vec<u8>,
    pub(in crate::bindings) maximum_bytes: u64,
    pub(in crate::bindings) reason: String,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::bindings) struct BrowserPromotionRequest {
    pub(in crate::bindings) operation_id: Vec<u8>,
    pub(in crate::bindings) accepted_tiers: Vec<String>,
    pub(in crate::bindings) residency: Vec<BrowserObjectResidency>,
    pub(in crate::bindings) destinations: Vec<BrowserPromotionDestination>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::bindings) struct BrowserObjectResidency {
    pub(in crate::bindings) object_id: Vec<u8>,
    pub(in crate::bindings) location_id: Vec<u8>,
    pub(in crate::bindings) tier: String,
    pub(in crate::bindings) source_priority: u16,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(in crate::bindings) struct BrowserPromotionDestination {
    pub(in crate::bindings) location_id: Vec<u8>,
    pub(in crate::bindings) tier: String,
    pub(in crate::bindings) writable: bool,
    pub(in crate::bindings) maximum_object_bytes: u64,
    pub(in crate::bindings) priority: u16,
    pub(in crate::bindings) cost_units_per_byte: u64,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserResidencyExecution {
    pub(in crate::bindings) object_bytes: u64,
    pub(in crate::bindings) work: BrowserWorkCounters,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserPromotionAdmission {
    #[tsify(type = "\"satisfied\" | \"planned\" | \"rejected\"")]
    pub(in crate::bindings) status: &'static str,
    #[tsify(type = "string | undefined")]
    pub(in crate::bindings) rejection: Option<&'static str>,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) operation_id: Option<serde_bytes::ByteBuf>,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) object_id: Option<serde_bytes::ByteBuf>,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) source_location_id: Option<serde_bytes::ByteBuf>,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) destination_location_id: Option<serde_bytes::ByteBuf>,
    pub(in crate::bindings) estimated_cost_units: Option<u64>,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct BrowserSpeculationPreemption {
    #[tsify(type = "Uint8Array[]")]
    pub(in crate::bindings) residency_operation_ids: Vec<serde_bytes::ByteBuf>,
    #[tsify(type = "Uint8Array[]")]
    pub(in crate::bindings) promotion_operation_ids: Vec<serde_bytes::ByteBuf>,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserResidencyMetrics {
    pub(in crate::bindings) candidates: u64,
    pub(in crate::bindings) admitted: u64,
    pub(in crate::bindings) active: u64,
    pub(in crate::bindings) active_bytes: u64,
    pub(in crate::bindings) useful: u64,
    pub(in crate::bindings) wasted: u64,
    pub(in crate::bindings) rejected_fence: u64,
    pub(in crate::bindings) rejected_duplicate: u64,
    pub(in crate::bindings) rejected_capacity: u64,
    pub(in crate::bindings) rejected_cost: u64,
    pub(in crate::bindings) rejected_usefulness: u64,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserPromotionMetrics {
    pub(in crate::bindings) candidates: u64,
    pub(in crate::bindings) satisfied: u64,
    pub(in crate::bindings) planned: u64,
    pub(in crate::bindings) active: u64,
    pub(in crate::bindings) active_bytes: u64,
    pub(in crate::bindings) active_cost_units: u64,
    pub(in crate::bindings) useful: u64,
    pub(in crate::bindings) wasted: u64,
    pub(in crate::bindings) rejected: u64,
}

#[derive(Serialize, Tsify)]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserSpeculationMetrics {
    pub(in crate::bindings) residency: BrowserResidencyMetrics,
    pub(in crate::bindings) promotion: BrowserPromotionMetrics,
}

#[wasm_bindgen]
impl BrowserSpeculation {
    pub fn observe(&self, observation: JsValue) -> Result<Ts<BrowserAdmissionResult>, JsValue> {
        let observation: BrowserResidencyObservation =
            serde_wasm_bindgen::from_value(observation).map_err(js_error)?;
        let admission = self
            .controller
            .try_borrow_mut()
            .map_err(|_| speculation_busy())?
            .observe_hint(
                OperationId::from_bytes(fixed_16(&observation.operation_id)?),
                VolumeId::from_bytes(fixed_16(&observation.volume_id)?),
                acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
                    &observation.generation_id,
                    "generation identity",
                )?)),
                observation.foreground_bytes,
                ResidencyHint {
                    request: ObjectReadRequest {
                        object_id: decode_object_id(&observation.object_id)?,
                        maximum_bytes: observation.maximum_bytes,
                    },
                    reason: ResidencyReason::from_public_str(&observation.reason)
                        .ok_or_else(|| js_error("unknown residency speculation reason"))?,
                },
            )
            .map_err(js_error)?;
        let result = match admission {
            ResidencyAdmission::Admitted(_) => BrowserAdmissionResult {
                status: "admitted",
                rejection: None,
            },
            ResidencyAdmission::Rejected(rejection) => BrowserAdmissionResult {
                status: "rejected",
                rejection: Some(rejection.as_str()),
            },
        };
        ts(result)
    }

    #[wasm_bindgen(js_name = executeResidency)]
    pub async fn execute_residency(
        &self,
        operation_id: Vec<u8>,
    ) -> Result<Ts<BrowserResidencyExecution>, JsValue> {
        let operation_id = OperationId::from_bytes(fixed_16(&operation_id)?);
        let controller = self
            .controller
            .try_borrow()
            .map_err(|_| speculation_busy())?
            .clone();
        let permit = controller
            .active_residency_permit(operation_id)
            .ok_or_else(|| js_error("residency operation is not active"))?;
        let receipt = self
            .engine
            .execute_residency(
                controller.residency(),
                permit,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(js_error)?;
        ts(BrowserResidencyExecution {
            object_bytes: receipt.value,
            work: browser_work(receipt.work),
        })
    }

    #[wasm_bindgen(js_name = finishResidency)]
    pub fn finish_residency(&self, operation_id: &[u8], useful: bool) -> Result<(), JsValue> {
        self.controller
            .try_borrow_mut()
            .map_err(|_| speculation_busy())?
            .finish_residency(OperationId::from_bytes(fixed_16(operation_id)?), useful)
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = planPromotion)]
    pub fn plan_promotion(
        &self,
        request: JsValue,
    ) -> Result<Ts<BrowserPromotionAdmission>, JsValue> {
        let request: BrowserPromotionRequest =
            serde_wasm_bindgen::from_value(request).map_err(js_error)?;
        let operation_id = OperationId::from_bytes(fixed_16(&request.operation_id)?);
        let mut controller = self
            .controller
            .try_borrow_mut()
            .map_err(|_| speculation_busy())?;
        let permit = controller
            .active_residency_permit(operation_id)
            .ok_or_else(|| js_error("residency operation is not active"))?;
        let accepted_tiers = request
            .accepted_tiers
            .iter()
            .map(|tier| storage_tier(tier))
            .collect::<Result<Vec<_>, _>>()?;
        let residency = request
            .residency
            .iter()
            .map(browser_object_residency)
            .collect::<Result<Vec<_>, _>>()?;
        let destinations = request
            .destinations
            .iter()
            .map(browser_promotion_destination)
            .collect::<Result<Vec<_>, _>>()?;
        let admission = controller
            .plan_promotion(permit, accepted_tiers, &residency, &destinations)
            .map_err(js_error)?;
        ts(browser_promotion_admission(admission))
    }

    #[wasm_bindgen(js_name = finishPromotion)]
    pub fn finish_promotion(&self, operation_id: &[u8], useful: bool) -> Result<(), JsValue> {
        self.controller
            .try_borrow_mut()
            .map_err(|_| speculation_busy())?
            .finish_promotion(OperationId::from_bytes(fixed_16(operation_id)?), useful)
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = preemptForForeground)]
    pub fn preempt_for_foreground(
        &self,
        bytes: u64,
    ) -> Result<Ts<BrowserSpeculationPreemption>, JsValue> {
        let value = self
            .controller
            .try_borrow_mut()
            .map_err(|_| speculation_busy())?
            .preempt_for_foreground(bytes)
            .map_err(js_error)?;
        ts(browser_speculation_preemption(value))
    }

    #[wasm_bindgen(js_name = replaceGeneration)]
    pub fn replace_generation(
        &self,
        generation_id: &[u8],
    ) -> Result<Ts<BrowserSpeculationPreemption>, JsValue> {
        let value = self
            .controller
            .try_borrow_mut()
            .map_err(|_| speculation_busy())?
            .replace_generation(acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
                generation_id,
                "generation identity",
            )?)))
            .map_err(js_error)?;
        ts(browser_speculation_preemption(value))
    }

    pub fn metrics(&self) -> Result<Ts<BrowserSpeculationMetrics>, JsValue> {
        let metrics = self
            .controller
            .try_borrow()
            .map_err(|_| speculation_busy())?
            .metrics();
        ts(BrowserSpeculationMetrics {
            residency: browser_residency_metrics(metrics.residency),
            promotion: browser_promotion_metrics(metrics.promotion),
        })
    }

    pub fn cancel(&self) {
        self.cancellation.cancel();
    }
}

pub(in crate::bindings) fn speculation_busy() -> JsValue {
    browser_error(
        "AcyclicFilesystemError",
        "speculation controller is already executing an operation",
    )
}

pub(in crate::bindings) const fn browser_speculation_options(
    options: &BrowserSpeculationOptions,
) -> SpeculationOptions {
    SpeculationOptions {
        residency: ResidencySpeculatorOptions {
            maximum_active_operations: options.residency.maximum_active_operations,
            maximum_active_bytes: options.residency.maximum_active_bytes,
            outcome_window: options.residency.outcome_window,
            traffic_window: options.residency.traffic_window,
            speculative_cost_basis_points: options.residency.speculative_cost_basis_points,
            minimum_usefulness_samples: options.residency.minimum_usefulness_samples,
            minimum_usefulness_basis_points: options.residency.minimum_usefulness_basis_points,
        },
        promotion: PromotionSpeculatorOptions {
            maximum_active_operations: options.promotion.maximum_active_operations,
            maximum_active_bytes: options.promotion.maximum_active_bytes,
            maximum_active_cost_units: options.promotion.maximum_active_cost_units,
            maximum_residency_facts: options.promotion.maximum_residency_facts,
            maximum_destinations: options.promotion.maximum_destinations,
            maximum_accepted_tiers: options.promotion.maximum_accepted_tiers,
            outcome_window: options.promotion.outcome_window,
            minimum_usefulness_samples: options.promotion.minimum_usefulness_samples,
            minimum_usefulness_basis_points: options.promotion.minimum_usefulness_basis_points,
        },
    }
}

pub(in crate::bindings) fn browser_object_residency(
    value: &BrowserObjectResidency,
) -> Result<ObjectResidency, JsValue> {
    Ok(ObjectResidency {
        object_id: decode_object_id(&value.object_id)?,
        location_id: StorageLocationId::from_bytes(fixed_16(&value.location_id)?),
        tier: storage_tier(&value.tier)?,
        source_priority: value.source_priority,
    })
}

pub(in crate::bindings) fn browser_promotion_destination(
    value: &BrowserPromotionDestination,
) -> Result<PromotionDestination, JsValue> {
    Ok(PromotionDestination {
        location_id: StorageLocationId::from_bytes(fixed_16(&value.location_id)?),
        tier: storage_tier(&value.tier)?,
        writable: value.writable,
        maximum_object_bytes: value.maximum_object_bytes,
        priority: value.priority,
        cost_units_per_byte: value.cost_units_per_byte,
    })
}

pub(in crate::bindings) fn browser_promotion_admission(
    value: PromotionAdmission,
) -> BrowserPromotionAdmission {
    match value {
        PromotionAdmission::Satisfied(residency) => BrowserPromotionAdmission {
            status: "satisfied",
            rejection: None,
            operation_id: None,
            object_id: Some(encode_object_id(residency.object_id).into()),
            source_location_id: Some(residency.location_id.into_bytes().to_vec().into()),
            destination_location_id: None,
            estimated_cost_units: None,
        },
        PromotionAdmission::Planned(plan) => BrowserPromotionAdmission {
            status: "planned",
            rejection: None,
            operation_id: Some(plan.candidate.operation_id.into_bytes().to_vec().into()),
            object_id: Some(encode_object_id(plan.candidate.request.object_id).into()),
            source_location_id: Some(plan.source.location_id.into_bytes().to_vec().into()),
            destination_location_id: Some(
                plan.destination.location_id.into_bytes().to_vec().into(),
            ),
            estimated_cost_units: Some(plan.estimated_cost_units),
        },
        PromotionAdmission::Rejected(rejection) => BrowserPromotionAdmission {
            status: "rejected",
            rejection: Some(rejection.as_str()),
            operation_id: None,
            object_id: None,
            source_location_id: None,
            destination_location_id: None,
            estimated_cost_units: None,
        },
    }
}

pub(in crate::bindings) fn browser_speculation_preemption(
    value: acyclic_fs::SpeculationPreemption,
) -> BrowserSpeculationPreemption {
    BrowserSpeculationPreemption {
        residency_operation_ids: value
            .residency
            .into_iter()
            .map(|operation| operation.into_bytes().to_vec())
            .map(Into::into)
            .collect(),
        promotion_operation_ids: value
            .promotion
            .into_iter()
            .map(|operation| operation.into_bytes().to_vec())
            .map(Into::into)
            .collect(),
    }
}

pub(in crate::bindings) fn browser_residency_metrics(
    value: acyclic_fs::ResidencyMetrics,
) -> BrowserResidencyMetrics {
    BrowserResidencyMetrics {
        candidates: value.candidates,
        admitted: value.admitted,
        active: value.active,
        active_bytes: value.active_bytes,
        useful: value.useful,
        wasted: value.wasted,
        rejected_fence: value.rejected_fence,
        rejected_duplicate: value.rejected_duplicate,
        rejected_capacity: value.rejected_capacity,
        rejected_cost: value.rejected_cost,
        rejected_usefulness: value.rejected_usefulness,
    }
}

pub(in crate::bindings) fn browser_promotion_metrics(
    value: acyclic_fs::PromotionMetrics,
) -> BrowserPromotionMetrics {
    BrowserPromotionMetrics {
        candidates: value.candidates,
        satisfied: value.satisfied,
        planned: value.planned,
        active: value.active,
        active_bytes: value.active_bytes,
        active_cost_units: value.active_cost_units,
        useful: value.useful,
        wasted: value.wasted,
        rejected: value.rejected,
    }
}

pub(in crate::bindings) const fn browser_object_cache_options(
    options: &BrowserObjectCacheOptions,
) -> ObjectCacheOptions {
    ObjectCacheOptions {
        maximum_entries: options.entries,
        maximum_bytes: options.bytes,
        maximum_in_flight: options.in_flight,
        maximum_waiters_per_object: options.waiters_per_object,
    }
}
