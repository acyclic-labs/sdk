use std::collections::BTreeSet;
use std::io::Write;

pub fn generate() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build/case_fold.rs");
    let mapper = icu_casemap::CaseMapper::new();
    let mut expansions = BTreeSet::new();
    for scalar in (0..=0x10ffff).filter_map(char::from_u32) {
        let mut encoded = [0; 4];
        let folded = mapper.fold_string(scalar.encode_utf8(&mut encoded));
        if folded.chars().count() > 1 {
            expansions.insert(folded.into_owned());
        }
    }
    let output = std::path::PathBuf::from(std::env::var_os("OUT_DIR").ok_or("OUT_DIR missing")?);
    let mut file = std::io::BufWriter::new(std::fs::File::create(output.join("case_fold.rs"))?);
    writeln!(file, "pub(crate) const CASE_FOLD_EXPANSIONS: &[&str] = &[")?;
    for expansion in expansions {
        writeln!(file, "{expansion:?},")?;
    }
    writeln!(file, "]; ")?;
    Ok(())
}
