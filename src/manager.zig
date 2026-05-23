//! Idiomatic Zig wrapper around the C FFI exposed by raft-sys
//! (which itself wraps tikv's `raft-rs 0.7` via cbindgen). The
//! wrapper is intentionally thin — it doesn't hide the lifecycle
//! quirks of raft-rs (need to call `tick` periodically to drive
//! timers, then `poll_ready` to discover groups with pending work,
//! then `process_ready` to apply that work via a callback). It
//! does erase the C-ABI noise: bool returns, slices instead of
//! pointer+length pairs, Zig errors instead of int return codes.
//!
//! Apply callbacks remain raw `callconv(.c)` for now — consumers
//! that want a "give me a Zig closure" wrapper can build one on
//! top, but the trampoline pattern adds a hop that we don't pay
//! for unless we need it. The cost matters: `process_ready` fires
//! the callback once per applied entry, on the hot path.

const std = @import("std");
const c = @cImport({
    @cInclude("raft_sys.h");
});
const storage_mod = @import("storage.zig");
const file_storage_mod = @import("file_storage.zig");

pub const MemStorage = storage_mod.MemStorage;
pub const FileStorage = file_storage_mod.FileStorage;
/// Re-export of the C-ABI storage vtable type so consumers can
/// pass it around (as `*const StorageVTable`) without their own
/// `@cImport` of `raft_sys.h`.
pub const StorageVTable = c.RaftStorageVTable;
pub const storage_vtable: *const c.RaftStorageVTable = @ptrCast(&storage_mod.vtable);
pub const file_storage_vtable: *const c.RaftStorageVTable = @ptrCast(&file_storage_mod.vtable);

/// Raw C-ABI apply callback signature. Re-exported so consumers
/// don't have to repeat the calling-convention boilerplate.
pub const ApplyCb = *const fn (
    userdata: ?*anyopaque,
    group_id: u64,
    index: u64,
    term: u64,
    data: [*c]const u8,
    len: usize,
) callconv(.c) void;

/// Raw C-ABI outbound-message callback signature. Fires once per
/// message queued during `processReady`, when the caller drives
/// `takeMessages`. `to` is the destination raft node id;
/// `msg_bytes` is the rust-protobuf serialization of the
/// `eraftpb::Message` and is valid only for the duration of the
/// callback. The caller hands `msg_bytes` to the peer's `step`.
pub const MessageCb = *const fn (
    userdata: ?*anyopaque,
    to: u64,
    msg_bytes: [*c]const u8,
    msg_len: usize,
) callconv(.c) void;

pub const Error = error{
    ManagerInitFailed,
    CreateGroupFailed,
    DestroyGroupFailed,
    CampaignFailed,
    ProposeFailed,
    ProcessReadyFailed,
    TakeMessagesFailed,
    StepFailed,
    StepDecodeFailed,
};

/// One `raft-rs` MultiRaft manager. Owns all the groups beneath
/// it; `deinit` walks them and frees their storage via each
/// group's destroy-vtable callback.
pub const Manager = struct {
    ptr: *c.RaftManager,

    pub fn init() Error!Manager {
        const p = c.raft_manager_new() orelse return Error.ManagerInitFailed;
        return .{ .ptr = p };
    }

    pub fn deinit(self: *Manager) void {
        c.raft_manager_free(self.ptr);
        self.* = undefined;
    }

    /// Create a raft group identified by `group_id`. `node_id` is
    /// this node's identity within the group's voter set. The
    /// `storage` pointer is held by raft-rs via the vtable's
    /// userdata slot; the storage is freed when the group is
    /// destroyed (via the `destroy` vtable callback).
    ///
    /// `vtable` selects the storage implementation. Pass
    /// `storage_vtable` for the in-memory MemStorage (most callers)
    /// or `file_storage.vtable` for the WAL-backed FileStorage. Any
    /// vtable whose userdata layout matches the pointer passed in
    /// `storage_userdata` works — the C ABI here is opaque.
    pub fn createGroup(
        self: *Manager,
        group_id: u64,
        node_id: u64,
        vtable: *const c.RaftStorageVTable,
        storage_userdata: *anyopaque,
    ) Error!void {
        // `node_id` is this node's identity within the group's
        // voter set — must match one of the `voters` the storage
        // returns from `initial_state`. Single-node tests/demos
        // always pass 1; multi-node uses 1..=cluster_size.
        const rc = c.raft_manager_create_group(self.ptr, group_id, node_id, vtable, storage_userdata);
        if (rc != 0) return Error.CreateGroupFailed;
    }

    /// Tear down the group. Storage is freed via the `destroy`
    /// vtable callback; any outstanding pointers into it are
    /// dangling after this call.
    pub fn destroyGroup(self: *Manager, group_id: u64) Error!void {
        if (c.raft_manager_destroy_group(self.ptr, group_id) != 0) return Error.DestroyGroupFailed;
    }

    /// Force this node to become leader of `group_id`. Useful for
    /// single-node testing; in multi-node deployments the
    /// election fires automatically when a campaign timeout
    /// elapses (driven via `tickAll`).
    pub fn campaign(self: *Manager, group_id: u64) Error!void {
        if (c.raft_manager_campaign(self.ptr, group_id) != 0) return Error.CampaignFailed;
    }

    /// Propose an entry to `group_id`. Returns when the entry is
    /// in raft-rs's pending list, NOT when it's applied —
    /// application happens via `processReady` after the entry
    /// commits.
    pub fn propose(self: *Manager, group_id: u64, data: []const u8) Error!void {
        if (c.raft_manager_propose(self.ptr, group_id, data.ptr, data.len) != 0)
            return Error.ProposeFailed;
    }

    /// Drive the per-group election/heartbeat/snapshot timers
    /// once. In a real deployment, call this periodically (every
    /// ~10ms is typical for raft-rs) so leader-election + log
    /// replication timeouts fire on schedule.
    pub fn tickAll(self: *Manager) void {
        _ = c.raft_manager_tick_all(self.ptr);
    }

    /// Identify groups with pending applies. Returns a slice into
    /// the caller's buffer; the buffer must be sized to fit at
    /// least as many group IDs as the manager has groups (use
    /// `groupCount`).
    pub fn pollReady(self: *Manager, buf: []u64) []u64 {
        const n = c.raft_manager_poll_ready(self.ptr, buf.ptr, buf.len);
        return buf[0..n];
    }

    /// Drive `group_id`'s pending applies through `callback`.
    /// Called once per applied entry. `userdata` is passed
    /// through verbatim.
    pub fn processReady(self: *Manager, group_id: u64, callback: ApplyCb, userdata: ?*anyopaque) Error!void {
        if (c.raft_manager_process_ready(self.ptr, group_id, callback, userdata) != 0)
            return Error.ProcessReadyFailed;
    }

    pub fn isLeader(self: *const Manager, group_id: u64) bool {
        return c.raft_manager_is_leader(self.ptr, group_id);
    }

    pub fn groupCount(self: *const Manager) usize {
        return c.raft_manager_group_count(self.ptr);
    }

    /// Drain the group's outbox of outbound raft messages. `cb`
    /// fires once per message in FIFO order; the bytes handed to
    /// the callback live only until the callback returns (Rust
    /// frees them after). The typical pattern: in the callback,
    /// route `(to, bytes)` into a network sim or transport layer,
    /// which will eventually deliver them to the recipient's
    /// `step`.
    pub fn takeMessages(
        self: *Manager,
        group_id: u64,
        cb: MessageCb,
        userdata: ?*anyopaque,
    ) Error!void {
        if (c.raft_manager_take_messages(self.ptr, group_id, cb, userdata) != 0)
            return Error.TakeMessagesFailed;
    }

    /// Deliver an inbound raft message produced by a peer's
    /// `takeMessages`. `msg_bytes` is the protobuf-serialized
    /// `eraftpb::Message` — opaque to Zig; raft-rs deserializes
    /// on the Rust side.
    pub fn step(self: *Manager, group_id: u64, msg_bytes: []const u8) Error!void {
        const rc = c.raft_manager_step(self.ptr, group_id, msg_bytes.ptr, msg_bytes.len);
        return switch (rc) {
            0 => {},
            -2 => Error.StepDecodeFailed,
            else => Error.StepFailed,
        };
    }
};

// ── Tests ──────────────────────────────────────────────────────────────────

const testing = std.testing;

const MsgCollector = struct {
    var bytes_buf: [4096]u8 = undefined;
    var bytes_len: usize = 0;
    var last_to: u64 = 0;
    var count: usize = 0;

    fn reset() void {
        bytes_len = 0;
        last_to = 0;
        count = 0;
    }

    fn cb(
        ud: ?*anyopaque,
        to: u64,
        msg_bytes: [*c]const u8,
        msg_len: usize,
    ) callconv(.c) void {
        _ = ud;
        last_to = to;
        count += 1;
        const n = @min(msg_len, bytes_buf.len);
        @memcpy(bytes_buf[0..n], msg_bytes[0..n]);
        bytes_len = n;
    }

    fn snapshot() []const u8 {
        return bytes_buf[0..bytes_len];
    }
};

test "Manager: take_messages drains the outbox after a campaign (multi-node sanity)" {
    // Two nodes (groups 1 and 2 internally, but they're really two
    // logical raft nodes with peer ids 1, 2). Node 1 thinks the
    // cluster is {1, 2}; node 2 thinks the same. Node 1 campaigns;
    // raft-rs queues a RequestVote message destined for node 2.
    // We drain it and assert.

    var mgr = try Manager.init();
    defer mgr.deinit();

    const storage = try MemStorage.init(testing.allocator, &.{ 1, 2 });
    try mgr.createGroup(1, 1, storage_vtable, storage);
    try mgr.campaign(1);

    // The campaign synchronously stages a RequestVote message;
    // processReady extracts it into the outbox. One tick + one
    // ready pump is enough for the message to materialize.
    var buf: [16]u64 = undefined;
    mgr.tickAll();
    const ready = mgr.pollReady(&buf);
    try testing.expect(ready.len >= 1);
    for (ready) |g| try mgr.processReady(g, struct {
        fn cb(
            ud: ?*anyopaque,
            gid: u64,
            idx: u64,
            term: u64,
            data: [*c]const u8,
            len: usize,
        ) callconv(.c) void {
            _ = ud;
            _ = gid;
            _ = idx;
            _ = term;
            _ = data;
            _ = len;
        }
    }.cb, null);

    MsgCollector.reset();
    try mgr.takeMessages(1, MsgCollector.cb, null);
    // RequestVote goes to node 2.
    try testing.expectEqual(@as(u64, 2), MsgCollector.last_to);
    try testing.expect(MsgCollector.count >= 1);
    try testing.expect(MsgCollector.snapshot().len > 0);
}

test "Manager: step rejects garbage bytes with StepDecodeFailed" {
    var mgr = try Manager.init();
    defer mgr.deinit();
    const storage = try MemStorage.init(testing.allocator, &.{1});
    try mgr.createGroup(1, 1, storage_vtable, storage);

    const garbage = [_]u8{ 0xff, 0xff, 0xff, 0xff };
    try testing.expectError(Error.StepDecodeFailed, mgr.step(1, &garbage));
}
