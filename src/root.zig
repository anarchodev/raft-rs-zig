//! raft-rs-zig — Zig bindings over tikv's `raft-rs 0.7`.
//!
//! The Rust crate (`raft-sys/`) is built via cbindgen into a C
//! ABI; this module wraps that ABI in idiomatic Zig types
//! (`Manager`, `MemStorage`, `ApplyCb`, `Error`). The library
//! is intentionally thin: it erases the C-ABI noise (int return
//! codes, raw pointer+length pairs) but doesn't pretend the
//! lifecycle quirks of raft aren't there — consumers still call
//! `tickAll → pollReady → processReady` on their own schedule.
//!
//! Status: single-process, MemStorage-backed, no network
//! transport. Sufficient for rewind2's testbed; multi-node +
//! durable storage are the next growth axes.
//!
//! Build wiring: `b.dependency("raft_rs_zig", ...)`. Two pieces
//! to consume:
//!
//! ```zig
//! const raft_dep = b.dependency("raft_rs_zig", .{ ... });
//! my_module.addImport("raft_rs_zig", raft_dep.module("raft_rs_zig"));
//! my_exe.linkLibrary(raft_dep.artifact("raft_rs_zig"));  // triggers cargo
//! ```
//!
//! Importing the module gets you the Zig API; linking the
//! artifact triggers the underlying `cargo build` and pulls
//! libraft_sys.a + the Rust runtime's required system libs.

pub const manager = @import("manager.zig");
pub const storage = @import("storage.zig");

// ── flattened public API ──────────────────────────────────────────────

pub const Manager = manager.Manager;
pub const MemStorage = manager.MemStorage;
pub const ApplyCb = manager.ApplyCb;
pub const Error = manager.Error;

test {
    @import("std").testing.refAllDecls(@This());
}
