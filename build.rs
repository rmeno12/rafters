fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/rafters.proto");

    tonic_build::configure()
        // Specify the path for the generated descriptor set
        .file_descriptor_set_path("proto/rafters_descriptor.pb")
        // Compile the proto files and generate Rust code
        .compile_protos(&["proto/rafters.proto"], &["proto"])
        .unwrap_or_else(|e| panic!("Failed to compile protos: {:?}", e));

    Ok(())
}
