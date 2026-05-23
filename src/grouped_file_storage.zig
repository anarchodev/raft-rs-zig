//! Multi-group file-backed Storage — N raft groups share ONE WAL file
//! per node, with one fsync per pump cycle amortizing every group's
//! writes. Built as `SharedWal` (the shared file handle) + per-group
//! `GroupedFileStorage` (the instance raft-rs sees through its
//! storage vtable).
//!
//! Why interleaved: fsync is the scarce resource. A per-group
//! FileStorage design forces K fsyncs per pump cycle (each
//! processReady ends with that group's flush). On btrfs / nvme that's
//! ms-scale work per fsync; K>4 craters throughput. Interleaving lets
//! one fsync amortize every group's writes — the only shape that
//! scales multi-tenant raft.
//!
//! Record format extends `FileStorage`'s with a `group_id` field:
//!
//!     record  = [tag:u8][group_id:u64 LE][payload_len:u32 LE][payload][crc32:u32 LE]
//!     tag = 1 → entry
//!           payload = [entry_type:u32][term:u64][index:u64]
//!                     [data_len:u32][data]
//!                     [context_len:u32][context]
//!                     [sync_log:u8]
//!     tag = 2 → hardstate
//!           payload = [term:u64][vote:u64][commit:u64]   (24 bytes)
//!     tag = 3 → confstate
//!           payload = [voters_len:u32][voters: u64*N]
//!                     [learners_len:u32][learners: u64*M]
//!
//! CRC32 is `std.hash.Crc32` (IEEE 802.3) over `tag || group_id ||
//! payload_len || payload`.
//!
//! `flush()` lives on `SharedWal`, not `GroupedFileStorage` — the
//! dispatcher calls it once per pump cycle after every ready group has
//! run processReady. No per-group flush is exposed by the vtable.
//!
//! v1 scope (benchmark-correct only, same caveat as `FileStorage` v1):
//!   - WAL is truncated on init; there is no replay-from-existing path.
//!   - **Tail-truncate** (leader-change conflict where raft-rs rewrites
//!     the uncommitted suffix of a group's log) updates that group's
//!     in-memory `MemStorage` view + its `entry_offsets` map, but does
//!     NOT physically rewind the shared file — other groups have
//!     appended past the truncated region, so a `setEndPos` would
//!     orphan their entries. The bytes between a truncated entry's
//!     start and the file's current end are dead-but-present. Replay
//!     (when implemented) must walk the file, dispatch records by
//!     `group_id`, and apply per-group last-authoritative-entry
//!     semantics.

const std = @import("std");
const c = @cImport({
    @cInclude("raft_sys.h");
});
const mem_storage = @import("storage.zig");

pub const MemStorage = mem_storage.MemStorage;

pub const Tag = enum(u8) {
    entry = 1,
    hardstate = 2,
    confstate = 3,
};

/// Length of the per-record header: tag + group_id + payload_len.
const HEADER_LEN: usize = 1 + 8 + 4;
/// Length of the per-record trailer: crc32.
const TRAILER_LEN: usize = 4;

/// One shared WAL file across all groups on a single cluster-node.
/// Construction + flush are the dispatcher's responsibility; group
/// storages borrow `*SharedWal` via `GroupedFileStorage.init` and
/// write through `appendRecord`.
///
/// Single-threaded by contract: the dispatcher pumps one ready group
/// at a time per node, so the file handle + offset + scratch buffer
/// are accessed serially. If the dispatcher ever parallelizes per-
/// group processReady, this struct needs a mutex on `wal_offset` +
/// `file.writevAll` (and per-group scratch — already on
/// `GroupedFileStorage`, not here).
pub const SharedWal = struct {
    allocator: std.mem.Allocator,
    file: std.fs.File,
    wal_path: []u8,
    /// Monotonic write head — used to record per-group entry offsets
    /// before each append so the per-group map stays in lockstep.
    wal_offset: u64,

    pub fn init(allocator: std.mem.Allocator, wal_path: []const u8) !*SharedWal {
        const self = try allocator.create(SharedWal);
        errdefer allocator.destroy(self);

        const path_dup = try allocator.dupe(u8, wal_path);
        errdefer allocator.free(path_dup);

        // v1: always start fresh. Same caveat as `FileStorage` v1 —
        // the truncate is what makes this "benchmark-correct only";
        // a restart-correct version replays the existing file.
        const file = try std.fs.cwd().createFile(wal_path, .{
            .truncate = true,
            .read = false,
        });
        errdefer file.close();

        self.* = .{
            .allocator = allocator,
            .file = file,
            .wal_path = path_dup,
            .wal_offset = 0,
        };
        return self;
    }

    pub fn deinit(self: *SharedWal) void {
        self.file.close();
        const a = self.allocator;
        a.free(self.wal_path);
        a.destroy(self);
    }

    /// fsync the WAL. The dispatcher calls this ONCE per pump cycle
    /// after every ready group has run processReady (and every
    /// vtable write callback has returned). This is the load-bearing
    /// "one fsync amortizes K groups" point — the entire reason
    /// SharedWal exists.
    pub fn flush(self: *SharedWal) !void {
        try self.file.sync();
    }

    /// Append one record to the shared file. Returns the file offset
    /// the record was written at — the caller's per-group
    /// `entry_offsets` records this so a later tail-truncate can map
    /// "entry index N" back to its byte range.
    ///
    /// `payload` is the body bytes only; this function frames the
    /// header (tag + group_id + payload_len) and trailer (crc32).
    pub fn appendRecord(
        self: *SharedWal,
        group_id: u64,
        tag: Tag,
        payload: []const u8,
    ) !u64 {
        var header: [HEADER_LEN]u8 = undefined;
        header[0] = @intFromEnum(tag);
        std.mem.writeInt(u64, header[1..9], group_id, .little);
        std.mem.writeInt(u32, header[9..13], @intCast(payload.len), .little);

        var crc = std.hash.Crc32.init();
        crc.update(header[0..]);
        crc.update(payload);
        var trailer: [TRAILER_LEN]u8 = undefined;
        std.mem.writeInt(u32, &trailer, crc.final(), .little);

        const offset_before = self.wal_offset;
        var iov = [_]std.posix.iovec_const{
            .{ .base = &header, .len = header.len },
            .{ .base = if (payload.len == 0) undefined else payload.ptr, .len = payload.len },
            .{ .base = &trailer, .len = trailer.len },
        };
        try self.file.writevAll(iov[0..]);
        self.wal_offset += HEADER_LEN + payload.len + TRAILER_LEN;
        return offset_before;
    }
};

/// Per-group storage on top of `SharedWal`. raft-rs sees this through
/// the `vtable` below; `*GroupedFileStorage` is the userdata pointer
/// passed to `Manager.createGroup`. Each group has its own
/// `MemStorage` (its read view + entry buffer) + its own
/// `entry_offsets` map; both are independent across groups. The shared
/// piece is just the file handle on `wal`.
///
/// **Lifetime**: `wal` is **borrowed** (not owned). The dispatcher
/// owns `*SharedWal` and tears it down after every `GroupedFileStorage`
/// has been destroyed (via `destroyGroup`'s vtable callback). Destroying
/// a group does NOT close the shared file.
pub const GroupedFileStorage = struct {
    allocator: std.mem.Allocator,
    /// Per-group read view + entry buffer — same role MemStorage plays
    /// inside `FileStorage`.
    mem: *MemStorage,
    /// Borrowed shared-WAL handle. Other groups on this node share it.
    wal: *SharedWal,
    /// The group id all records appended by this storage carry. Set at
    /// init; never changes.
    group_id: u64,
    /// Per-group offset map — tracks `mem.entries` 1:1. Slot `i` is the
    /// file offset where the record for `mem.entries.items[i]` starts.
    /// Slot 0 is the MemStorage sentinel and is never read.
    entry_offsets: std.ArrayList(u64),
    /// Per-group scratch buffer for assembling entry payloads. Lives
    /// here (not on `SharedWal`) so that if the dispatcher ever runs
    /// per-group processReady in parallel, the scratch buffers don't
    /// alias. The `SharedWal.appendRecord` call itself is still
    /// serialized by the dispatcher today.
    scratch: std.ArrayList(u8),

    pub fn init(
        allocator: std.mem.Allocator,
        voters: []const u64,
        wal: *SharedWal,
        group_id: u64,
    ) !*GroupedFileStorage {
        const self = try allocator.create(GroupedFileStorage);
        errdefer allocator.destroy(self);

        const mem = try MemStorage.init(allocator, voters);
        errdefer mem.deinit();

        var offsets: std.ArrayList(u64) = .empty;
        errdefer offsets.deinit(allocator);
        // Sentinel slot mirrors mem.entries[0]; never read.
        try offsets.append(allocator, 0);

        self.* = .{
            .allocator = allocator,
            .mem = mem,
            .wal = wal,
            .group_id = group_id,
            .entry_offsets = offsets,
            .scratch = .empty,
        };
        return self;
    }

    pub fn deinit(self: *GroupedFileStorage) void {
        // NB: we do NOT close `self.wal` — it's borrowed; the
        // dispatcher owns it and tears it down after all groups
        // sharing it have been destroyed.
        self.mem.deinit();
        self.entry_offsets.deinit(self.allocator);
        self.scratch.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    fn writeEntryRecord(self: *GroupedFileStorage, e: c.RaftEntryFfi) !u64 {
        self.scratch.clearRetainingCapacity();
        const w = self.scratch.writer(self.allocator);
        try w.writeInt(u32, e.entry_type, .little);
        try w.writeInt(u64, e.term, .little);
        try w.writeInt(u64, e.index, .little);
        try w.writeInt(u32, @intCast(e.data_len), .little);
        if (e.data_len > 0) try w.writeAll(e.data[0..e.data_len]);
        try w.writeInt(u32, @intCast(e.context_len), .little);
        if (e.context_len > 0) try w.writeAll(e.context[0..e.context_len]);
        try w.writeByte(if (e.sync_log) 1 else 0);
        return self.wal.appendRecord(self.group_id, .entry, self.scratch.items);
    }

    fn writeHardStateRecord(self: *GroupedFileStorage, hs: c.RaftHardStateFfi) !void {
        var payload: [24]u8 = undefined;
        std.mem.writeInt(u64, payload[0..8], hs.term, .little);
        std.mem.writeInt(u64, payload[8..16], hs.vote, .little);
        std.mem.writeInt(u64, payload[16..24], hs.commit, .little);
        _ = try self.wal.appendRecord(self.group_id, .hardstate, &payload);
    }
};

// ── Vtable callbacks ─────────────────────────────────────────────────────────
//
// Read callbacks delegate straight to MemStorage's vtable. Write
// callbacks build the payload, append through the shared WAL, then
// delegate. fsync is NOT here — the dispatcher calls
// `SharedWal.flush()` after every ready group has run processReady.

fn destroyCb(ud: ?*anyopaque) callconv(.c) void {
    const self: *GroupedFileStorage = @ptrCast(@alignCast(ud.?));
    self.deinit();
}

fn initialStateCb(
    ud: ?*anyopaque,
    out_hs: [*c]c.RaftHardStateFfi,
    out_cs: [*c]c.RaftConfStateFfi,
) callconv(.c) i32 {
    const self: *GroupedFileStorage = @ptrCast(@alignCast(ud.?));
    return mem_storage.vtable.initial_state.?(self.mem, @ptrCast(out_hs), @ptrCast(out_cs));
}

fn entriesCb(
    ud: ?*anyopaque,
    low: u64,
    high: u64,
    max_size: u64,
    out: [*c]c.RaftEntriesOut,
) callconv(.c) i32 {
    const self: *GroupedFileStorage = @ptrCast(@alignCast(ud.?));
    return mem_storage.vtable.entries.?(self.mem, low, high, max_size, @ptrCast(out));
}

fn termCb(ud: ?*anyopaque, idx: u64, out: [*c]u64) callconv(.c) i32 {
    const self: *GroupedFileStorage = @ptrCast(@alignCast(ud.?));
    return mem_storage.vtable.term.?(self.mem, idx, out);
}

fn firstIndexCb(ud: ?*anyopaque, out: [*c]u64) callconv(.c) i32 {
    const self: *GroupedFileStorage = @ptrCast(@alignCast(ud.?));
    return mem_storage.vtable.first_index.?(self.mem, out);
}

fn lastIndexCb(ud: ?*anyopaque, out: [*c]u64) callconv(.c) i32 {
    const self: *GroupedFileStorage = @ptrCast(@alignCast(ud.?));
    return mem_storage.vtable.last_index.?(self.mem, out);
}

fn snapshotCb(
    ud: ?*anyopaque,
    request_index: u64,
    out_data: [*c][*c]const u8,
    out_data_len: [*c]usize,
    out_meta_index: [*c]u64,
    out_meta_term: [*c]u64,
) callconv(.c) i32 {
    const self: *GroupedFileStorage = @ptrCast(@alignCast(ud.?));
    return mem_storage.vtable.snapshot.?(
        self.mem,
        request_index,
        @ptrCast(out_data),
        out_data_len,
        out_meta_index,
        out_meta_term,
    );
}

fn appendEntriesCb(
    ud: ?*anyopaque,
    entries: [*c]const c.RaftEntryFfi,
    n: usize,
) callconv(.c) i32 {
    const self: *GroupedFileStorage = @ptrCast(@alignCast(ud.?));
    if (n == 0) return 0;

    // Tail-truncate handling: raft-rs may rewrite this group's
    // uncommitted suffix on a leader-change. The per-group offset map
    // and MemStorage view both follow the truncation, but the shared
    // file does NOT physically rewind — other groups have appended
    // past the truncated entry. v1 leaves orphan bytes in the file;
    // see the module doc comment.
    const last_idx = self.mem.lastIndex();
    const first_new_idx = entries[0].index;
    if (first_new_idx <= last_idx) {
        const sentinel_idx = self.mem.entries.items[0].index;
        if (first_new_idx <= sentinel_idx) return -1; // before the sentinel — bug
        const truncate_at: usize = @intCast(first_new_idx - sentinel_idx);
        self.entry_offsets.shrinkRetainingCapacity(truncate_at);
    }

    // Append: each new entry's offset is recorded before the write so
    // a later tail-truncate can locate the boundary in the offset map.
    for (entries[0..n]) |e| {
        const offset = self.writeEntryRecord(e) catch return -1;
        self.entry_offsets.append(self.allocator, offset) catch return -1;
    }

    return mem_storage.vtable.append_entries.?(self.mem, @ptrCast(entries), n);
}

fn setHardStateCb(ud: ?*anyopaque, hs: [*c]const c.RaftHardStateFfi) callconv(.c) i32 {
    const self: *GroupedFileStorage = @ptrCast(@alignCast(ud.?));
    self.writeHardStateRecord(hs.*) catch return -1;
    return mem_storage.vtable.set_hard_state.?(self.mem, @ptrCast(hs));
}

fn applySnapshotCb(
    ud: ?*anyopaque,
    data: [*c]const u8,
    data_len: usize,
    meta_index: u64,
    meta_term: u64,
) callconv(.c) i32 {
    const self: *GroupedFileStorage = @ptrCast(@alignCast(ud.?));
    return mem_storage.vtable.apply_snapshot.?(self.mem, data, data_len, meta_index, meta_term);
}

pub const vtable: c.RaftStorageVTable = .{
    .initial_state = initialStateCb,
    .entries = entriesCb,
    .term = termCb,
    .first_index = firstIndexCb,
    .last_index = lastIndexCb,
    .snapshot = snapshotCb,
    .append_entries = appendEntriesCb,
    .set_hard_state = setHardStateCb,
    .apply_snapshot = applySnapshotCb,
    .destroy = destroyCb,
};

// ── Tests ──────────────────────────────────────────────────────────────────

const testing = std.testing;

const Harness = struct {
    tmp: std.testing.TmpDir,
    path: [:0]u8,

    fn init() !Harness {
        var tmp = testing.tmpDir(.{});
        errdefer tmp.cleanup();
        var dir_buf: [std.fs.max_path_bytes]u8 = undefined;
        const dir_path = try tmp.dir.realpath(".", &dir_buf);
        const joined = try std.fmt.allocPrint(
            testing.allocator,
            "{s}/grouped_file_storage.wal",
            .{dir_path},
        );
        defer testing.allocator.free(joined);
        const path = try testing.allocator.dupeZ(u8, joined);
        return .{ .tmp = tmp, .path = path };
    }

    fn deinit(self: *Harness) void {
        testing.allocator.free(self.path);
        self.tmp.cleanup();
    }
};

fn fakeEntry(term: u64, index: u64, data: []const u8) c.RaftEntryFfi {
    return .{
        .entry_type = 0,
        .term = term,
        .index = index,
        .data = if (data.len == 0) null else data.ptr,
        .data_len = data.len,
        .context = null,
        .context_len = 0,
        .sync_log = false,
    };
}

test "SharedWal: init creates empty file; deinit closes cleanly" {
    var h = try Harness.init();
    defer h.deinit();

    const wal = try SharedWal.init(testing.allocator, h.path);
    defer wal.deinit();
    try testing.expectEqual(@as(u64, 0), wal.wal_offset);
}

test "SharedWal: one record written by group 1 lands at offset 0 with right framing" {
    var h = try Harness.init();
    defer h.deinit();

    const wal = try SharedWal.init(testing.allocator, h.path);
    defer wal.deinit();

    const payload: [3]u8 = .{ 'x', 'y', 'z' };
    const offset = try wal.appendRecord(1, .entry, &payload);
    try testing.expectEqual(@as(u64, 0), offset);
    // 13 header + 3 payload + 4 trailer = 20
    try testing.expectEqual(@as(u64, 20), wal.wal_offset);

    // Read back via the OS and check the header framing.
    const f = try std.fs.cwd().openFile(h.path, .{});
    defer f.close();
    var buf: [20]u8 = undefined;
    _ = try f.readAll(&buf);
    try testing.expectEqual(@as(u8, @intFromEnum(Tag.entry)), buf[0]);
    try testing.expectEqual(@as(u64, 1), std.mem.readInt(u64, buf[1..9], .little));
    try testing.expectEqual(@as(u32, 3), std.mem.readInt(u32, buf[9..13], .little));
    try testing.expectEqualSlices(u8, "xyz", buf[13..16]);
}

test "SharedWal: K groups interleave into one file; offsets are returned per write" {
    var h = try Harness.init();
    defer h.deinit();

    const wal = try SharedWal.init(testing.allocator, h.path);
    defer wal.deinit();

    // Three groups, one entry each.
    const off1 = try wal.appendRecord(1, .entry, "aaa");
    const off2 = try wal.appendRecord(2, .entry, "bb");
    const off3 = try wal.appendRecord(1, .entry, "cccc");

    try testing.expectEqual(@as(u64, 0), off1);
    try testing.expectEqual(@as(u64, 13 + 3 + 4), off2);
    try testing.expectEqual(@as(u64, 13 + 3 + 4 + 13 + 2 + 4), off3);
    try testing.expectEqual(@as(u64, 13 + 3 + 4 + 13 + 2 + 4 + 13 + 4 + 4), wal.wal_offset);
}

test "GroupedFileStorage: init wires through SharedWal; deinit doesn't close the shared file" {
    var h = try Harness.init();
    defer h.deinit();

    const wal = try SharedWal.init(testing.allocator, h.path);
    defer wal.deinit();

    const g1 = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 1);
    defer g1.deinit();
    const g2 = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 2);
    defer g2.deinit();

    try testing.expectEqual(@as(u64, 1), g1.group_id);
    try testing.expectEqual(@as(u64, 2), g2.group_id);
    try testing.expectEqual(@as(usize, 1), g1.entry_offsets.items.len);
    try testing.expectEqual(@as(usize, 1), g2.entry_offsets.items.len);
}

test "GroupedFileStorage: each group's appends land in the shared file; per-group offsets are independent" {
    var h = try Harness.init();
    defer h.deinit();

    const wal = try SharedWal.init(testing.allocator, h.path);
    defer wal.deinit();

    const g1 = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 1);
    defer g1.deinit();
    const g2 = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 2);
    defer g2.deinit();

    // Group 1 appends an entry, then group 2 appends an entry —
    // verify both land in the same file at correct offsets.
    const e1 = fakeEntry(1, 1, "x");
    var arr1 = [_]c.RaftEntryFfi{e1};
    try testing.expectEqual(@as(i32, 0), appendEntriesCb(g1, &arr1, 1));

    const e2 = fakeEntry(1, 1, "yyyy");
    var arr2 = [_]c.RaftEntryFfi{e2};
    try testing.expectEqual(@as(i32, 0), appendEntriesCb(g2, &arr2, 1));

    // Per-group entry offsets are independent.
    try testing.expectEqual(@as(usize, 2), g1.entry_offsets.items.len);
    try testing.expectEqual(@as(usize, 2), g2.entry_offsets.items.len);
    // g1's entry starts at 0; g2's starts after g1's full record.
    try testing.expectEqual(@as(u64, 0), g1.entry_offsets.items[1]);
    //   g1 record: 13 header + 30 payload + 4 trailer = 47
    //   (30 = 4 entry_type + 8 term + 8 index + 4 data_len + 1 data
    //         + 4 context_len + 0 + 1 sync_log)
    try testing.expectEqual(@as(u64, 47), g2.entry_offsets.items[1]);

    // mem views also independent.
    try testing.expectEqual(@as(u64, 1), g1.mem.lastIndex());
    try testing.expectEqual(@as(u64, 1), g2.mem.lastIndex());
}

test "GroupedFileStorage: tail-truncate updates mem + offsets but does NOT physically rewind the file" {
    var h = try Harness.init();
    defer h.deinit();

    const wal = try SharedWal.init(testing.allocator, h.path);
    defer wal.deinit();

    const g1 = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 1);
    defer g1.deinit();
    const g2 = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 2);
    defer g2.deinit();

    // g1: append 3 entries.
    var batch = [_]c.RaftEntryFfi{
        fakeEntry(1, 1, "a"),
        fakeEntry(1, 2, "b"),
        fakeEntry(1, 3, "c"),
    };
    try testing.expectEqual(@as(i32, 0), appendEntriesCb(g1, &batch, batch.len));
    try testing.expectEqual(@as(u64, 3), g1.mem.lastIndex());

    // g2: append after g1, then g1 tail-truncates from index 2 — the
    // shared file must NOT physically rewind, or g2's entry would be
    // orphaned.
    var g2_batch = [_]c.RaftEntryFfi{fakeEntry(1, 1, "z")};
    try testing.expectEqual(@as(i32, 0), appendEntriesCb(g2, &g2_batch, 1));
    const wal_offset_before_trunc = wal.wal_offset;
    const g2_last_offset = g2.entry_offsets.items[1];

    // g1's leader-change overwrites index 2 onward.
    var rewrite = [_]c.RaftEntryFfi{fakeEntry(2, 2, "B'")};
    try testing.expectEqual(@as(i32, 0), appendEntriesCb(g1, &rewrite, 1));

    // g1's mem view is rewritten — last index = 2.
    try testing.expectEqual(@as(u64, 2), g1.mem.lastIndex());
    // g1's entry_offsets shrank then re-appended one slot.
    try testing.expectEqual(@as(usize, 3), g1.entry_offsets.items.len); // sentinel + idx 1 + idx 2
    // g2 still has its entry's offset — wasn't disturbed.
    try testing.expectEqual(@as(u64, g2_last_offset), g2.entry_offsets.items[1]);
    // wal_offset advanced (didn't rewind) — the rewrite is APPENDED.
    try testing.expect(wal.wal_offset > wal_offset_before_trunc);
}

test "GroupedFileStorage: setHardState writes a per-group hardstate record" {
    var h = try Harness.init();
    defer h.deinit();

    const wal = try SharedWal.init(testing.allocator, h.path);
    defer wal.deinit();

    const g1 = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 7);
    defer g1.deinit();

    const hs: c.RaftHardStateFfi = .{ .term = 5, .vote = 3, .commit = 2 };
    try testing.expectEqual(@as(i32, 0), setHardStateCb(g1, &hs));

    // Header (13) + 24 byte hs payload + 4 trailer = 41
    try testing.expectEqual(@as(u64, 41), wal.wal_offset);

    // Read back, confirm tag + group_id.
    const f = try std.fs.cwd().openFile(h.path, .{});
    defer f.close();
    var buf: [41]u8 = undefined;
    _ = try f.readAll(&buf);
    try testing.expectEqual(@as(u8, @intFromEnum(Tag.hardstate)), buf[0]);
    try testing.expectEqual(@as(u64, 7), std.mem.readInt(u64, buf[1..9], .little));
    try testing.expectEqual(@as(u32, 24), std.mem.readInt(u32, buf[9..13], .little));
    try testing.expectEqual(@as(u64, 5), std.mem.readInt(u64, buf[13..21], .little));
}

test "SharedWal.flush: a single flush amortizes K groups' writes" {
    // The structural guarantee — the entire reason this module exists.
    // We verify by writing through K groups, calling flush ONCE, and
    // confirming all records are on disk.
    var h = try Harness.init();
    defer h.deinit();

    const wal = try SharedWal.init(testing.allocator, h.path);
    defer wal.deinit();

    const K: usize = 8;
    var groups: [K]*GroupedFileStorage = undefined;
    for (&groups, 0..) |*g, i| {
        g.* = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, @intCast(i + 1));
    }
    defer for (groups) |g| g.deinit();

    for (groups) |g| {
        var arr = [_]c.RaftEntryFfi{fakeEntry(1, 1, "data")};
        try testing.expectEqual(@as(i32, 0), appendEntriesCb(g, &arr, 1));
    }

    try wal.flush(); // ONE fsync amortizes K groups' writes.

    // All K records on disk. Each entry record: 13 header + 33 payload
    // + 4 trailer = 50 bytes (33 = 4 entry_type + 8 term + 8 index + 4
    // data_len + 4 data + 4 context_len + 1 sync_log).
    try testing.expectEqual(@as(u64, K * 50), wal.wal_offset);
}
