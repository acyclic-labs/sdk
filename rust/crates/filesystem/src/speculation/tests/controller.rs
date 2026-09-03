use super::*;
use crate::foundation::Digest;
use crate::storage::{ObjectId, ObjectKind, ObjectReadRequest, object_digest};
use crate::{ResidencyReason, ResidencyRejection, StorageLocationId};

fn generation(byte: u8) -> GenerationId {
    GenerationId::new(Digest::from_bytes([byte; 32]))
}

fn object(byte: u8) -> ObjectId {
    let bytes = [byte; 16];
    ObjectId {
        kind: ObjectKind::BlobChunk,
        digest: object_digest(ObjectKind::BlobChunk, &bytes),
    }
}

fn hint(byte: u8) -> ResidencyHint {
    ResidencyHint {
        request: ObjectReadRequest {
            object_id: object(byte),
            maximum_bytes: 16,
        },
        reason: ResidencyReason::SequentialRange,
    }
}

fn options() -> SpeculationOptions {
    SpeculationOptions {
        residency: ResidencySpeculatorOptions {
            maximum_active_operations: 8,
            maximum_active_bytes: 128,
            speculative_cost_basis_points: 10_000,
            minimum_usefulness_samples: 8,
            ..ResidencySpeculatorOptions::default()
        },
        promotion: PromotionSpeculatorOptions {
            maximum_active_operations: 8,
            maximum_active_bytes: 128,
            maximum_active_cost_units: 128,
            minimum_usefulness_samples: 8,
            ..PromotionSpeculatorOptions::default()
        },
    }
}

fn promotion_inputs(object_id: ObjectId) -> (ObjectResidency, PromotionDestination) {
    (
        ObjectResidency {
            object_id,
            location_id: StorageLocationId::from_bytes([1; 16]),
            tier: StorageTier::DurableOrigin,
            source_priority: 0,
        },
        PromotionDestination {
            location_id: StorageLocationId::from_bytes([2; 16]),
            tier: StorageTier::NodeLocal,
            writable: true,
            maximum_object_bytes: 16,
            priority: 0,
            cost_units_per_byte: 1,
        },
    )
}

#[test]
fn composed_preemption_and_generation_replacement_are_atomic_and_ordered()
-> Result<(), Box<dyn std::error::Error>> {
    let volume_id = VolumeId::from_bytes([3; 16]);
    let generation_id = generation(4);
    let mut controller = SpeculationController::new(options(), volume_id, generation_id)?;
    let operation_id = OperationId::from_bytes([5; 16]);
    let ResidencyAdmission::Admitted(permit) =
        controller.observe_hint(operation_id, volume_id, generation_id, 16, hint(6))?
    else {
        return Err("residency hint was not admitted".into());
    };
    let (source, destination) = promotion_inputs(permit.candidate().request.object_id);
    assert!(matches!(
        controller.plan_promotion(
            permit,
            vec![StorageTier::NodeLocal],
            &[source],
            &[destination],
        )?,
        PromotionAdmission::Planned(_)
    ));
    assert_eq!(
        controller.preempt_for_foreground(32)?,
        SpeculationPreemption {
            residency: vec![operation_id],
            promotion: vec![operation_id],
        }
    );
    let metrics = controller.metrics();
    assert_eq!(metrics.residency.wasted, 1);
    assert_eq!(metrics.promotion.wasted, 1);
    assert_eq!(metrics.residency.active, 0);
    assert_eq!(metrics.promotion.active, 0);

    assert_eq!(
        controller.replace_generation(generation(7))?,
        SpeculationPreemption::default()
    );
    assert_eq!(
        controller.observe_hint(
            OperationId::from_bytes([8; 16]),
            volume_id,
            generation_id,
            16,
            hint(9),
        )?,
        ResidencyAdmission::Rejected(ResidencyRejection::StaleGeneration)
    );
    Ok(())
}

#[test]
fn controller_exposes_only_exact_active_engine_state() -> Result<(), Box<dyn std::error::Error>> {
    let volume_id = VolumeId::from_bytes([13; 16]);
    let generation_id = generation(14);
    let mut controller = SpeculationController::new(options(), volume_id, generation_id)?;
    let operation_id = OperationId::from_bytes([15; 16]);
    assert!(controller.active_residency_permit(operation_id).is_none());
    assert_eq!(
        controller.residency().metrics(),
        controller.metrics().residency
    );
    assert_eq!(
        controller.promotion().metrics(),
        controller.metrics().promotion
    );

    let ResidencyAdmission::Admitted(permit) =
        controller.observe_hint(operation_id, volume_id, generation_id, 16, hint(16))?
    else {
        return Err("controller rejected an admitted residency hint".into());
    };
    assert_eq!(
        controller.active_residency_permit(operation_id),
        Some(permit)
    );
    let (source, destination) = promotion_inputs(permit.candidate().request.object_id);
    assert!(matches!(
        controller.plan_promotion(
            permit,
            vec![StorageTier::NodeLocal],
            &[source],
            &[destination],
        )?,
        PromotionAdmission::Planned(_)
    ));
    assert_eq!(controller.residency().metrics().active, 1);
    assert_eq!(controller.promotion().metrics().active, 1);
    Ok(())
}

#[derive(Clone, Copy, Default)]
struct Model {
    active_residency: u64,
    active_promotion: u64,
    admitted: u64,
    planned: u64,
    residency_useful: u64,
    residency_wasted: u64,
    promotion_wasted: u64,
}

#[derive(Clone)]
struct HistoryState {
    controller: SpeculationController,
    model: Model,
    generation_byte: u8,
    next_operation: u8,
    active: Vec<(OperationId, ResidencyPermit, bool)>,
}

fn verify_model(state: &HistoryState) {
    let metrics = state.controller.metrics();
    assert_eq!(metrics.residency.active, state.model.active_residency);
    assert_eq!(metrics.promotion.active, state.model.active_promotion);
    assert_eq!(metrics.residency.admitted, state.model.admitted);
    assert_eq!(metrics.promotion.planned, state.model.planned);
    assert_eq!(metrics.residency.useful, state.model.residency_useful);
    assert_eq!(metrics.residency.wasted, state.model.residency_wasted);
    assert_eq!(metrics.promotion.wasted, state.model.promotion_wasted);
}

fn explore(state: &HistoryState, depth: u8) -> Result<u64, Box<dyn std::error::Error>> {
    if depth == 0 {
        verify_model(state);
        return Ok(1);
    }
    let histories = explore_observe(state, depth)?
        + explore_active(state, depth)?
        + explore_fence(state, depth, false)?
        + explore_fence(state, depth, true)?;
    Ok(histories)
}

fn explore_observe(state: &HistoryState, depth: u8) -> Result<u64, Box<dyn std::error::Error>> {
    let volume_id = VolumeId::from_bytes([3; 16]);
    let generation_id = generation(state.generation_byte);
    let mut observed = state.clone();
    let operation_id = OperationId::from_bytes([state.next_operation; 16]);
    let ResidencyAdmission::Admitted(permit) = observed.controller.observe_hint(
        operation_id,
        volume_id,
        generation_id,
        16,
        hint(state.next_operation),
    )?
    else {
        return Err("generated residency admission was rejected".into());
    };
    observed.active.push((operation_id, permit, false));
    observed.next_operation += 1;
    observed.model.active_residency += 1;
    observed.model.admitted += 1;
    explore(&observed, depth - 1)
}

fn explore_active(state: &HistoryState, depth: u8) -> Result<u64, Box<dyn std::error::Error>> {
    let Some((operation_id, permit, promoted)) = state.active.last().copied() else {
        return Ok(0);
    };
    let mut histories = 0;
    if !promoted {
        let mut planned = state.clone();
        let (source, destination) = promotion_inputs(permit.candidate().request.object_id);
        assert!(matches!(
            planned.controller.plan_promotion(
                permit,
                vec![StorageTier::NodeLocal],
                &[source],
                &[destination],
            )?,
            PromotionAdmission::Planned(_)
        ));
        *planned.active.last_mut().ok_or("missing active permit")? = (operation_id, permit, true);
        planned.model.active_promotion += 1;
        planned.model.planned += 1;
        histories += explore(&planned, depth - 1)?;
    }
    let mut finished = state.clone();
    finished.controller.finish_residency(operation_id, true)?;
    if promoted {
        finished.controller.finish_promotion(operation_id, false)?;
    }
    finished.active.pop();
    finished.model.active_residency -= 1;
    finished.model.active_promotion -= u64::from(promoted);
    finished.model.residency_useful += 1;
    finished.model.promotion_wasted += u64::from(promoted);
    histories += explore(&finished, depth - 1)?;

    let mut cancelled = state.clone();
    cancelled.controller.finish_residency(operation_id, false)?;
    if promoted {
        cancelled.controller.finish_promotion(operation_id, false)?;
    }
    cancelled.active.pop();
    cancelled.model.active_residency -= 1;
    cancelled.model.active_promotion -= u64::from(promoted);
    cancelled.model.residency_wasted += 1;
    cancelled.model.promotion_wasted += u64::from(promoted);
    histories += explore(&cancelled, depth - 1)?;
    Ok(histories)
}

fn explore_fence(
    state: &HistoryState,
    depth: u8,
    replace: bool,
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut fenced = state.clone();
    if replace {
        fenced.generation_byte += 1;
        fenced
            .controller
            .replace_generation(generation(fenced.generation_byte))?;
    } else {
        fenced.controller.preempt_for_foreground(16)?;
    }
    fenced.model.residency_wasted += fenced.model.active_residency;
    fenced.model.promotion_wasted += fenced.model.active_promotion;
    fenced.model.active_residency = 0;
    fenced.model.active_promotion = 0;
    fenced.active.clear();
    explore(&fenced, depth - 1)
}

#[test]
fn generated_composed_histories_match_independent_terminal_count_model()
-> Result<(), Box<dyn std::error::Error>> {
    let state = HistoryState {
        controller: SpeculationController::new(
            options(),
            VolumeId::from_bytes([3; 16]),
            generation(4),
        )?,
        model: Model::default(),
        generation_byte: 4,
        next_operation: 16,
        active: Vec::new(),
    };
    let histories = explore(&state, 6)?;
    assert!(histories >= 2_000, "generated {histories} histories");
    Ok(())
}
