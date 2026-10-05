use std::env;
use std::fs;
use std::path::PathBuf;

use sdk_docs::{
    build_bundle, generate_rustdoc, to_pretty_json, to_website_json, write_rustdoc_profile,
    BuildOptions, GenerateOptions,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if arguments
        .first()
        .is_some_and(|argument| argument == "generate-rustdoc")
    {
        return generate_command(&arguments[1..]);
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "write-rustdoc-profile")
    {
        return write_profile_command(&arguments[1..]);
    }
    let mut repository_root = PathBuf::from(".");
    let mut output = None;
    let mut rustdoc_json = None;
    let mut source_revision = None;
    let mut profile_manifest = None;
    let mut examples_bundle = None;
    let mut source_authority = None;
    let mut source_authority_sha256 = None;
    let mut release_manifest = None;
    let mut registry_manifest = None;
    let mut require_rustdoc_json = false;
    let mut website_output = None;
    let mut source_state = "working-tree".to_owned();
    let mut channel = "branch-preview".to_owned();
    let mut args = env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--repo-root" => {
                repository_root = PathBuf::from(args.next().ok_or("missing --repo-root value")?);
            }
            "--output" => {
                output = Some(PathBuf::from(args.next().ok_or("missing --output value")?))
            }
            "--rustdoc-json" => {
                rustdoc_json = Some(PathBuf::from(
                    args.next().ok_or("missing --rustdoc-json value")?,
                ));
            }
            "--source-revision" => {
                source_revision = Some(args.next().ok_or("missing --source-revision value")?)
            }
            "--profile-manifest" => {
                profile_manifest = Some(PathBuf::from(
                    args.next().ok_or("missing --profile-manifest value")?,
                ));
            }
            "--examples-bundle" => {
                examples_bundle = Some(PathBuf::from(
                    args.next().ok_or("missing --examples-bundle value")?,
                ));
            }
            "--source-authority" => {
                source_authority = Some(PathBuf::from(
                    args.next().ok_or("missing --source-authority value")?,
                ));
            }
            "--source-authority-sha256" => {
                source_authority_sha256 = Some(
                    args.next()
                        .ok_or("missing --source-authority-sha256 value")?,
                );
            }
            "--release-manifest" => {
                release_manifest = Some(PathBuf::from(
                    args.next().ok_or("missing --release-manifest value")?,
                ));
            }
            "--registry-manifest" => {
                registry_manifest = Some(PathBuf::from(
                    args.next().ok_or("missing --registry-manifest value")?,
                ));
            }
            "--strict-rustdoc-json" => require_rustdoc_json = true,
            "--website-output" => {
                website_output = Some(PathBuf::from(
                    args.next().ok_or("missing --website-output value")?,
                ))
            }
            "--source-state" => source_state = args.next().ok_or("missing --source-state value")?,
            "--channel" => channel = args.next().ok_or("missing --channel value")?,
            "--help" | "-h" => {
                println!("sdk-docs --repo-root ROOT --output BUNDLE.json [--website-output PROJECTION.json] [--rustdoc-json FILE_OR_DIR] [--profile-manifest FILE] [--examples-bundle DIR] [--source-authority FILE --source-authority-sha256 SHA256] [--release-manifest FILE] [--registry-manifest FILE] [--source-revision REV] [--strict-rustdoc-json] [--source-state STATE] [--channel CHANNEL]");
                return Ok(());
            }
            unknown => return Err(format!("unknown argument: {unknown}").into()),
        }
    }
    let output = output.ok_or("--output is required")?;
    let mut options = BuildOptions::new(repository_root);
    options.rustdoc_json = rustdoc_json;
    options.source_revision = source_revision;
    options.profile_manifest = profile_manifest;
    options.examples_bundle = examples_bundle;
    options.source_authority = source_authority;
    options.source_authority_sha256 = source_authority_sha256;
    options.release_manifest = release_manifest;
    options.registry_manifest = registry_manifest;
    options.require_rustdoc_json = require_rustdoc_json;
    options.source_state = source_state.clone();
    let bundle = build_bundle(&options)?;
    fs::write(output, to_pretty_json(&bundle)?)?;
    if let Some(website_output) = website_output {
        fs::write(
            website_output,
            to_website_json(
                &bundle,
                "https://github.com/acyclic-labs/sdk",
                &source_state,
                &channel,
            )?,
        )?;
    }
    Ok(())
}

fn generate_command(arguments: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let mut repository_root = PathBuf::from(".");
    let mut generator_root = None;
    let mut source_revision = None;
    let mut profile_manifest = None;
    let mut output_dir = None;
    let mut toolchain = std::env::var("RUSTDOC_TOOLCHAIN").unwrap_or_else(|_| "1.98.1".to_owned());
    let mut compiler_cache_dir = None;
    let mut index = 0;
    while index < arguments.len() {
        match arguments[index].as_str() {
            "--repo-root" => {
                index += 1;
                repository_root =
                    PathBuf::from(arguments.get(index).ok_or("missing --repo-root value")?);
            }
            "--generator-root" => {
                index += 1;
                generator_root = Some(PathBuf::from(
                    arguments
                        .get(index)
                        .ok_or("missing --generator-root value")?,
                ));
            }
            "--source-revision" => {
                index += 1;
                source_revision = Some(
                    arguments
                        .get(index)
                        .ok_or("missing --source-revision value")?
                        .clone(),
                );
            }
            "--profile-manifest" => {
                index += 1;
                profile_manifest = Some(PathBuf::from(
                    arguments
                        .get(index)
                        .ok_or("missing --profile-manifest value")?,
                ));
            }
            "--output-dir" => {
                index += 1;
                output_dir = Some(PathBuf::from(
                    arguments.get(index).ok_or("missing --output-dir value")?,
                ));
            }
            "--toolchain" => {
                index += 1;
                toolchain = arguments
                    .get(index)
                    .ok_or("missing --toolchain value")?
                    .clone();
            }
            "--compiler-cache-dir" => {
                index += 1;
                compiler_cache_dir = Some(PathBuf::from(
                    arguments
                        .get(index)
                        .ok_or("missing --compiler-cache-dir value")?,
                ));
            }
            "--help" | "-h" => {
                println!("sdk-docs generate-rustdoc --repo-root ROOT --profile-manifest FILE --output-dir DIR [--source-revision REV] [--generator-root DIR] [--toolchain TOOLCHAIN] [--compiler-cache-dir DIR]");
                return Ok(());
            }
            unknown => return Err(format!("unknown generation argument: {unknown}").into()),
        }
        index += 1;
    }
    let profile_manifest =
        profile_manifest.unwrap_or_else(|| repository_root.join("docs/rustdoc-profiles.json"));
    let output_dir = output_dir.ok_or("--output-dir is required")?;
    let receipt = generate_rustdoc(&GenerateOptions {
        repository_root,
        source_revision,
        generator_root,
        profile_manifest,
        output_dir,
        toolchain,
        compiler_cache_dir,
    })?;
    println!("generated {} rustdoc artifacts", receipt.artifacts.len());
    Ok(())
}

fn write_profile_command(arguments: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let mut repository_root = PathBuf::from(".");
    let mut profile_manifest = None;
    let mut output = None;
    let mut toolchain = std::env::var("RUSTDOC_TOOLCHAIN").unwrap_or_else(|_| "1.98.1".to_owned());
    let mut index = 0;
    while index < arguments.len() {
        match arguments[index].as_str() {
            "--repo-root" => {
                index += 1;
                repository_root =
                    PathBuf::from(arguments.get(index).ok_or("missing --repo-root value")?);
            }
            "--profile-manifest" => {
                index += 1;
                profile_manifest = Some(PathBuf::from(
                    arguments
                        .get(index)
                        .ok_or("missing --profile-manifest value")?,
                ));
            }
            "--output" => {
                index += 1;
                output = Some(PathBuf::from(
                    arguments.get(index).ok_or("missing --output value")?,
                ));
            }
            "--toolchain" => {
                index += 1;
                toolchain = arguments
                    .get(index)
                    .ok_or("missing --toolchain value")?
                    .clone();
            }
            "--help" | "-h" => {
                println!("sdk-docs write-rustdoc-profile --repo-root ROOT [--profile-manifest FILE] --output FILE [--toolchain TOOLCHAIN]");
                return Ok(());
            }
            unknown => return Err(format!("unknown argument: {unknown}").into()),
        }
        index += 1;
    }
    let profile_manifest =
        profile_manifest.unwrap_or_else(|| repository_root.join("docs/rustdoc-profiles.json"));
    let output = output.ok_or("--output is required")?;
    let receipt = write_rustdoc_profile(&repository_root, &profile_manifest, &output, &toolchain)?;
    println!(
        "wrote {} Rustdoc profile packages across {} profiles",
        receipt
            .profiles
            .iter()
            .map(|profile| profile.packages.len())
            .sum::<usize>(),
        receipt.profiles.len()
    );
    Ok(())
}
