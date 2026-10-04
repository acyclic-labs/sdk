use acyclic_sdk_examples::guide_projections;
use serde_json::json;

fn main() {
    let projections = guide_projections::all()
        .into_iter()
        .map(|projection| {
            json!({
                "scenario_id": projection.scenario_id,
                "family": projection.family,
                "operation": projection.operation,
                "language": projection.language.as_str(),
                "mode": format!("{:?}", projection.mode).to_ascii_lowercase(),
                "source": projection.source,
                // Keep the standalone projection stream bound to the same
                // Rust source closure as the sdk-examples bundle. The
                // qualification harness carries this through every package
                // install and execution receipt.
                "source_sha256": env!("SDK_EXAMPLES_SOURCE_SHA256"),
                "capability": projection.capability.as_str(),
                "package_manager": projection.package.package_manager,
                "package_name": projection.package.package_name,
                "artifact_path": projection.package.artifact_path,
                "code": projection.code,
            })
        })
        .collect::<Vec<_>>();
    println!(
        "{}",
        serde_json::to_string_pretty(&projections).expect("projection JSON")
    );
}
