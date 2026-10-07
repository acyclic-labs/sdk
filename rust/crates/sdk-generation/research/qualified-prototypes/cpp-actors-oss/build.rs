fn main() {
    cxx_build::bridge("src/lib.rs")
        .std("c++11")
        .compile("cpp_actors_oss_qualification");
    println!("cargo:rerun-if-changed=src/lib.rs");
}
