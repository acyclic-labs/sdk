//! Generate the public auth contract from the crate-owned protocol.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("CARGO_FEATURE_NATIVE_JAVASCRIPT").is_some() {
        println!("cargo:rerun-if-env-changed=NAPI_TYPE_DEF_TMP_FOLDER");
        println!("cargo:rerun-if-env-changed=NAPI_FORCE_BUILD_ACYCLIC_AUTH");
    }
    let grpc = false;
    acyclic_proto_build::compile(
        acyclic_proto_build::config()?,
        acyclic_proto_build::services(grpc),
        &["proto/auth/v1/auth.proto"],
        "acyclic-auth-v1.bin",
    )
}
