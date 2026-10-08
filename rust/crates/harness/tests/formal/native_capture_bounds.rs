//! Solver harness for the exact production capture-admission arithmetic.
//! OS behavior, durable commits, schemas and path authority are outside this proof.

#[path = "../../src/filesystem/native_capture_bounds.rs"]
mod production;

#[kani::proof]
#[kani::unwind(1)]
fn accepted_capture_has_finite_control_and_complete_result_allowance() {
    let timeout: u32 = kani::any();
    let control: u32 = kani::any();
    let poll: u32 = kani::any();
    let output: u32 = kani::any();
    let result: u32 = kani::any();
    let accepted = production::capture_allowances_valid(timeout, control, poll, output, result);
    if accepted {
        assert!(timeout > 0 && control > 0 && control <= timeout);
        assert!(poll > 0 && poll <= timeout);
        assert!(output > 0 && result > 0);
        // Express the same safety obligation independently by subtracting the
        // metadata allowance, with subtraction checked before it is evaluated.
        assert!(result >= 2048);
        assert!(u64::from(output) <= u64::from(result - 2048) / 4);
    } else {
        assert!(timeout == 0
            || control == 0
            || control > timeout
            || poll == 0
            || poll > timeout
            || output == 0
            || result < 2048
            || u64::from(output) > u64::from(result - 2048) / 4);
    }
}

fn main() {}
