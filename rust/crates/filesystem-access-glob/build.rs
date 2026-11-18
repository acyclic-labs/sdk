#[path = "build/case_fold.rs"]
mod case_fold;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    case_fold::generate()
}
