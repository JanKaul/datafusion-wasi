fn main() -> Result<(), String> {
    // for use in docker build where file changes can be wonky
    println!("cargo:rerun-if-env-changed=FORCE_REBUILD");

    println!("cargo:rerun-if-changed=proto/datafusionwasi.proto");
    tonic_build::configure()
        .compile(&["proto/datafusionwasi.proto"], &["proto"])
        .map_err(|e| format!("protobuf compilation failed: {}", e))
}
