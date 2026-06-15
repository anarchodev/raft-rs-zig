//! raft-rs-zig — Zig bindings over tikv's `raft-rs 0.7` (via
//! cbindgen → C → @cImport). Two-piece build:
//!
//!   * `cargo build` produces `raft-sys/target/{profile}/libraft_sys.a`
//!     + `raft-sys/include/raft_sys.h`. The Rust crate uses
//!     `cbindgen` as a build-script dep, so the header is generated
//!     each time cargo runs.
//!
//!   * The Zig side exposes a module + a static library wrapping
//!     the C ABI. Consumers do:
//!
//!     ```
//!     const raft_dep = b.dependency("raft_rs_zig", ...);
//!     my_mod.addImport("raft_rs_zig", raft_dep.module("raft_rs_zig"));
//!     my_exe.linkLibrary(raft_dep.artifact("raft_rs_zig"));
//!     ```
//!
//! Importing the module gives the Zig API; linking the artifact
//! triggers the cargo step and pulls libraft_sys.a + Rust runtime
//! system libs (pthread/dl/m/gcc_s on Linux).

const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const release = optimize != .Debug;
    const profile_dir: []const u8 = if (release) "release" else "debug";

    // Build the Rust raft-sys static library. Redirect cargo's output into a
    // build-tracked directory so the produced `.a` is a LazyPath: adding it via
    // `addObjectFile` then creates an automatic build dependency (and triggers
    // cargo) wherever the module is used — no wrapper artifact required.
    const cargo = b.addSystemCommand(&.{ "cargo", "build" });
    if (release) cargo.addArg("--release");
    cargo.addArg("--manifest-path");
    cargo.addFileArg(b.path("raft-sys/Cargo.toml"));
    const cargo_out = cargo.addPrefixedOutputDirectoryArg("--target-dir=", "cargo-target");
    // Declare the Rust sources / build inputs as file inputs (not args)
    // so the cargo step's cache key tracks them — otherwise editing
    // `lib.rs` leaves the step cache-valid and zig links a STALE `.a`
    // (the cbindgen header + FFI ABI silently lag the source).
    cargo.addFileInput(b.path("raft-sys/src/lib.rs"));
    cargo.addFileInput(b.path("raft-sys/build.rs"));
    cargo.addFileInput(b.path("raft-sys/cbindgen.toml"));
    cargo.addFileInput(b.path("raft-sys/Cargo.lock"));
    const libsys = cargo_out.path(b, b.fmt("{s}/libraft_sys.a", .{profile_dir}));

    // The public Zig module. The prebuilt raft-sys static lib is linked as a
    // single object input (canonical for an external `.a` at a known path);
    // these link settings propagate to anything that imports the module.
    const mod = b.addModule("raft_rs_zig", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    mod.addIncludePath(b.path("raft-sys/include"));
    mod.addObjectFile(libsys);
    mod.link_libc = true;
    if (target.result.os.tag == .linux) {
        // Required by the Rust runtime: pthread for std::thread,
        // dl for dynamic linker symbols, m for f64 intrinsics,
        // gcc_s for _Unwind_* (rust_eh_personality / backtrace).
        mod.linkSystemLibrary("pthread", .{});
        mod.linkSystemLibrary("dl", .{});
        mod.linkSystemLibrary("m", .{});
        mod.linkSystemLibrary("gcc_s", .{});
    }

    // Spike exe — the demo. Same module, uses the library API.
    const exe_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    exe_mod.addImport("raft_rs_zig", mod);
    const exe = b.addExecutable(.{
        .name = "raft-spike",
        .root_module = exe_mod,
    });
    exe.step.dependOn(&cargo.step);
    b.installArtifact(exe);

    const run = b.addRunArtifact(exe);
    run.step.dependOn(b.getInstallStep());
    if (b.args) |args| run.addArgs(args);
    const run_step = b.step("run", "Run the spike");
    run_step.dependOn(&run.step);

    // Test step.
    const test_step = b.step("test", "Run raft-rs-zig tests");
    const tests = b.addTest(.{ .root_module = mod });
    tests.step.dependOn(&cargo.step);
    const run_tests = b.addRunArtifact(tests);
    test_step.dependOn(&run_tests.step);
}
