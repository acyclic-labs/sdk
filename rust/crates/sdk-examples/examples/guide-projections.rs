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
                "capability": projection.capability.as_str(),
                "package_manager": projection.package.package_manager,
                "package_name": projection.package.package_name,
                "artifact_path": projection.package.artifact_path,
                "code": projection.code,
            })
        })
        .collect::<Vec<_>>();
    println!("{}", serde_json::to_string_pretty(&projections).expect("projection JSON"));
}
