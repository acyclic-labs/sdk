//! Intentional negative control for the production numeric admission theorem.
use acyclic_fs::exact_u32_from_f64;

#[kani::proof]
fn negative_control_wrongly_accepts_fraction() {
    let result = exact_u32_from_f64(0.5);
    kani::assert(result.is_ok(), "intentional mutation control: 0.5 must not be accepted");
}

fn main() {}

