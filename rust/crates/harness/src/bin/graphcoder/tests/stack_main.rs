//! Stack budgets run on a process main thread, outside libtest worker threads.
#[path = "../args.rs"]
mod args;
#[path = "../entry.rs"]
mod entry;
#[path = "../stack_budget.rs"]
mod stack_budget;
#[path = "../view.rs"]
mod view;
fn main() -> std::process::ExitCode {
    match stack_budget::qualify() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("stack budget qualification failed: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
