//! Links the dependent's rlib with the probe's: a crate hash that differs between the probe's
//! metadata and rlib actions fails this link (E0460), and an `OUT_DIR` outside the execution root
//! fails the probe's compile.

#[test]
fn the_build_script_file_reads_back_through_out_dir() {
    assert_eq!(execroot_dependent::probe(), "written by the build script");
}
