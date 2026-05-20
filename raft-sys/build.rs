use std::env;
use std::path::PathBuf;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let crate_path = PathBuf::from(&crate_dir);
    let include_dir = crate_path.join("include");
    std::fs::create_dir_all(&include_dir).expect("create include/");
    let header_path = include_dir.join("raft_sys.h");

    let cfg = cbindgen::Config::from_file(crate_path.join("cbindgen.toml"))
        .expect("read cbindgen.toml");

    cbindgen::Builder::new()
        .with_crate(&crate_dir)
        .with_config(cfg)
        .generate()
        .expect("cbindgen failed")
        .write_to_file(&header_path);

    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=cbindgen.toml");
    println!("cargo:rerun-if-changed=build.rs");
}
