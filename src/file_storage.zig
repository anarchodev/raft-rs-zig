//! File-backed Storage — a sibling to `MemStorage` that pairs the
//! in-memory log + state with an append-only WAL sidecar that gets
//! `fsync`'d once per ready cycle. Lets benchmarks measure the
//! real disk-fsync interaction without rewriting MemStorage's
//! well-tested read paths.
//!
//! Strategy: FileStorage owns a `*MemStorage` for raft-rs's read
//! view + its append/HardState bookkeeping. Each write callback
//! serializes the FFI struct, `pwrite`-appends it to the WAL,
//! then delegates to the MemStorage vtable callback to update
//! the in-memory view. `fsync` is deferred to an explicit
//! `flush()` so the consumer can amortize one disk sync over an
//! entire `processReady` cycle (raft-rs's contract requires the
//! ready's writes to be durable before sending append-entries
//! responses; the consumer must call `flush()` before treating
//! `processReady` as complete).
//!
//! Record format (etcd-style mixed stream):
//!
//!     record  = [tag:u8][payload_len:u32 LE][payload][crc32:u32 LE]
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
//! CRC32 is `std.hash.Crc32` (IEEE 802.3) over `tag || payload_len || payload`.
//!
//! v1 scope: **benchmark-correct only.** `init` always truncates
//! the WAL — there is no replay-from-existing-WAL path on this
//! commit. raft-rs sees the same fresh-group state it would
//! with MemStorage. The disk writes + fsync happen in the right
//! places for honest throughput measurement; full restart
//! correctness is a follow-up that doesn't change the perf
//! numbers.

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

pub const FileStorage = struct {
    allocator: std.mem.Allocator,
    /// Truth-of-record for raft-rs's read view. FileStorage owns it;
    /// `destroyCb` tears both down.
    mem: *MemStorage,
    wal: std.fs.File,
    wal_path: []u8,
    /// Current write head in the WAL. Tracks `mem.entries` 1:1 —
    /// `entry_offsets.items[i]` is the WAL offset where the record
    /// for `mem.entries.items[i]` starts. Slot 0 corresponds to the
    /// MemStorage sentinel and is never read (offset is 0 there but
    /// no record exists at offset 0 until a real append).
    wal_offset: u64,
    entry_offsets: std.ArrayList(u64),

    /// One scratch buffer reused across writeRecord calls so the
    /// hot path doesn't allocate. Single-threaded, like MemStorage.
    scratch: std.ArrayList(u8),

    pub fn init(
        allocator: std.mem.Allocator,
        voters: []const u64,
        wal_path: []const u8,
    ) !*FileStorage {
        const self = try allocator.create(FileStorage);
        errdefer allocator.destroy(self);

        const path_dup = try allocator.dupe(u8, wal_path);
        errdefer allocator.free(path_dup);

        const mem = try MemStorage.init(allocator, voters);
        errdefer mem.deinit();

        // v1: always start fresh. The truncate is the load-bearing
        // bit that justifies the "benchmark-correct only" caveat —
        // a real restart-correct version would open + replay instead.
        const wal = try std.fs.cwd().createFile(wal_path, .{
            .truncate = true,
            .read = false,
        });
        errdefer wal.close();

        var offsets: std.ArrayList(u64) = .empty;
        errdefer offsets.deinit(allocator);
        // Sentinel slot mirrors mem.entries[0].
        try offsets.append(allocator, 0);

        self.* = .{
            .allocator = allocator,
            .mem = mem,
            .wal = wal,
            .wal_path = path_dup,
            .wal_offset = 0,
            .entry_offsets = offsets,
            .scratch = .empty,
        };
        return self;
    }

    pub fn deinit(self: *FileStorage) void {
        self.wal.close();
        self.mem.deinit();
        self.entry_offsets.deinit(self.allocator);
        self.scratch.deinit(self.allocator);
        const a = self.allocator;
        a.free(self.wal_path);
        a.destroy(self);
    }

    /// fsync the WAL. Consumers MUST call this once per
    /// `processReady` cycle (after the storage callbacks have
    /// returned, before treating apply as final) to honor raft-rs's
    /// durability contract. Returning from a write callback without
    /// a subsequent flush leaves the writes in the page cache only.
    pub fn flush(self: *FileStorage) !void {
        try self.wal.sync();
    }

    /// Truncate the WAL back to `target_offset`. Called when raft
    /// overwrites the uncommitted tail (leader-change conflict);
    /// `mem` has already been told to drop the corresponding
    /// entries, so the file follows.
    fn truncateTo(self: *FileStorage, target_offset: u64) !void {
        try self.wal.setEndPos(target_offset);
        try self.wal.seekTo(target_offset);
        self.wal_offset = target_offset;
    }

    // ── WAL writers ──────────────────────────────────────────────────

    fn writeEntryRecord(self: *FileStorage, e: c.RaftEntryFfi) !void {
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
        try self.writeRecord(.entry, self.scratch.items);
    }

    fn writeHardStateRecord(self: *FileStorage, hs: c.RaftHardStateFfi) !void {
        var payload: [24]u8 = undefined;
        std.mem.writeInt(u64, payload[0..8], hs.term, .little);
        std.mem.writeInt(u64, payload[8..16], hs.vote, .little);
        std.mem.writeInt(u64, payload[16..24], hs.commit, .little);
        try self.writeRecord(.hardstate, &payload);
    }

    fn writeRecord(self: *FileStorage, tag: Tag, payload: []const u8) !void {
        var header: [5]u8 = undefined;
        header[0] = @intFromEnum(tag);
        std.mem.writeInt(u32, header[1..5], @intCast(payload.len), .little);

        var crc = std.hash.Crc32.init();
        crc.update(header[0..]);
        crc.update(payload);
        var trailer: [4]u8 = undefined;
        std.mem.writeInt(u32, &trailer, crc.final(), .little);

        // One pwritev-equivalent: writevAll lets us avoid copying
        // payload into a contiguous buffer just for the syscall.
        var iov = [_]std.posix.iovec_const{
            .{ .base = &header, .len = header.len },
            .{ .base = if (payload.len == 0) undefined else payload.ptr, .len = payload.len },
            .{ .base = &trailer, .len = trailer.len },
        };
        try self.wal.writevAll(iov[0..]);
        self.wal_offset += header.len + payload.len + trailer.len;
    }
};

// ── Vtable callbacks ─────────────────────────────────────────────────────────
//
// Each callback either delegates straight to MemStorage (reads) or
// writes to the WAL first and then delegates (writes). The read
// callbacks pass `self.mem` as the userdata to MemStorage's vtable;
// the write callbacks do the same after the WAL append.

fn destroyCb(ud: ?*anyopaque) callconv(.c) void {
    const self: *FileStorage = @ptrCast(@alignCast(ud.?));
    self.deinit();
}

fn initialStateCb(
    ud: ?*anyopaque,
    out_hs: [*c]c.RaftHardStateFfi,
    out_cs: [*c]c.RaftConfStateFfi,
) callconv(.c) i32 {
    const self: *FileStorage = @ptrCast(@alignCast(ud.?));
    // `@ptrCast` bridges per-file @cImport scopes — see the same
    // workaround on manager.zig's createGroup call.
    return mem_storage.vtable.initial_state.?(self.mem, @ptrCast(out_hs), @ptrCast(out_cs));
}

fn entriesCb(
    ud: ?*anyopaque,
    low: u64,
    high: u64,
    max_size: u64,
    out: [*c]c.RaftEntriesOut,
) callconv(.c) i32 {
    const self: *FileStorage = @ptrCast(@alignCast(ud.?));
    return mem_storage.vtable.entries.?(self.mem, low, high, max_size, @ptrCast(out));
}

fn termCb(ud: ?*anyopaque, idx: u64, out: [*c]u64) callconv(.c) i32 {
    const self: *FileStorage = @ptrCast(@alignCast(ud.?));
    return mem_storage.vtable.term.?(self.mem, idx, out);
}

fn firstIndexCb(ud: ?*anyopaque, out: [*c]u64) callconv(.c) i32 {
    const self: *FileStorage = @ptrCast(@alignCast(ud.?));
    return mem_storage.vtable.first_index.?(self.mem, out);
}

fn lastIndexCb(ud: ?*anyopaque, out: [*c]u64) callconv(.c) i32 {
    const self: *FileStorage = @ptrCast(@alignCast(ud.?));
    return mem_storage.vtable.last_index.?(self.mem, out);
}

fn snapshotCb(
    ud: ?*anyopaque,
    request_index: u64,
    out_data: [*c][*c]const u8,
    out_data_len: [*c]usize,
    out_meta_index: [*c]u64,
    out_meta_term: [*c]u64,
    out_conf_state: [*c]c.RaftConfStateFfi,
) callconv(.c) i32 {
    const self: *FileStorage = @ptrCast(@alignCast(ud.?));
    return mem_storage.vtable.snapshot.?(
        self.mem,
        request_index,
        @ptrCast(out_data),
        out_data_len,
        out_meta_index,
        out_meta_term,
        out_conf_state,
    );
}

fn appendEntriesCb(
    ud: ?*anyopaque,
    entries: [*c]const c.RaftEntryFfi,
    n: usize,
) callconv(.c) i32 {
    const self: *FileStorage = @ptrCast(@alignCast(ud.?));
    if (n == 0) return 0;

    // Truncate-tail: if the first entry overwrites an existing
    // index, rewind the WAL to that offset before appending. The
    // delegated MemStorage call drops the in-memory tail; we keep
    // the offset map in lockstep.
    const last_idx = self.mem.lastIndex();
    const first_new_idx = entries[0].index;
    if (first_new_idx <= last_idx) {
        const sentinel_idx = self.mem.entries.items[0].index;
        if (first_new_idx <= sentinel_idx) return -1; // before the sentinel — bug
        const truncate_at: usize = @intCast(first_new_idx - sentinel_idx);
        const target_offset = self.entry_offsets.items[truncate_at];
        self.truncateTo(target_offset) catch return -1;
        self.entry_offsets.shrinkRetainingCapacity(truncate_at);
    }

    // Append: record the offset before each write so a later
    // truncate-tail lands on the right boundary.
    for (entries[0..n]) |e| {
        const offset_before = self.wal_offset;
        self.writeEntryRecord(e) catch return -1;
        self.entry_offsets.append(self.allocator, offset_before) catch return -1;
    }

    return mem_storage.vtable.append_entries.?(self.mem, @ptrCast(entries), n);
}

fn setHardStateCb(ud: ?*anyopaque, hs: [*c]const c.RaftHardStateFfi) callconv(.c) i32 {
    const self: *FileStorage = @ptrCast(@alignCast(ud.?));
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
    const self: *FileStorage = @ptrCast(@alignCast(ud.?));
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
//
// Direct unit tests of the WAL + offset-map invariants. The
// end-to-end behavior (FileStorage plugged into Manager) is
// covered by rewind2's dispatcher tests once the dispatcher
// gains a Storage parameter.

const testing = std.testing;

const Harness = struct {
    tmp: std.testing.TmpDir,
    path: [:0]u8,

    fn init() !Harness {
        var tmp = testing.tmpDir(.{});
        errdefer tmp.cleanup();
        var dir_buf: [std.fs.max_path_bytes]u8 = undefined;
        const dir_path = try tmp.dir.realpath(".", &dir_buf);
        const joined = try std.fmt.allocPrint(testing.allocator, "{s}/file_storage.wal", .{dir_path});
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

test "FileStorage: init creates an empty WAL; deinit closes cleanly" {
    var h = try Harness.init();
    defer h.deinit();

    const fs = try FileStorage.init(testing.allocator, &.{1}, h.path);
    defer fs.deinit();

    try testing.expectEqual(@as(u64, 0), fs.wal_offset);
    try testing.expectEqual(@as(u64, 1), fs.mem.firstIndex());
    try testing.expectEqual(@as(u64, 0), fs.mem.lastIndex());
}

test "FileStorage: append writes a record, advances offset, mem sees the entry" {
    var h = try Harness.init();
    defer h.deinit();

    const fs = try FileStorage.init(testing.allocator, &.{1}, h.path);
    defer fs.deinit();

    const e = fakeEntry(1, 1, "hello");
    var arr = [_]c.RaftEntryFfi{e};
    try testing.expectEqual(@as(i32, 0), appendEntriesCb(fs, &arr, 1));

    // Mem view advanced.
    try testing.expectEqual(@as(u64, 1), fs.mem.lastIndex());
    // Offset map gained a slot.
    try testing.expectEqual(@as(usize, 2), fs.entry_offsets.items.len);
    try testing.expectEqual(@as(u64, 0), fs.entry_offsets.items[1]);

    // WAL has one record: 5 (header) + payload + 4 (crc).
    //   payload = 4 (entry_type) + 8 (term) + 8 (index)
    //           + 4 (data_len) + 5 (data) + 4 (context_len)
    //           + 0 (context) + 1 (sync_log) = 34
    try testing.expectEqual(@as(u64, 5 + 34 + 4), fs.wal_offset);

    // Bytes on disk match — fsync, reopen for read, eyeball the header.
    try fs.flush();
    var rf = try std.fs.cwd().openFile(h.path, .{ .mode = .read_only });
    defer rf.close();
    var buf: [5]u8 = undefined;
    _ = try rf.readAll(&buf);
    try testing.expectEqual(@as(u8, @intFromEnum(Tag.entry)), buf[0]);
    try testing.expectEqual(@as(u32, 34), std.mem.readInt(u32, buf[1..5], .little));
}

test "FileStorage: setHardState writes a hardstate record + mem follows" {
    var h = try Harness.init();
    defer h.deinit();

    const fs = try FileStorage.init(testing.allocator, &.{1}, h.path);
    defer fs.deinit();

    const hs: c.RaftHardStateFfi = .{ .term = 7, .vote = 3, .commit = 2 };
    try testing.expectEqual(@as(i32, 0), setHardStateCb(fs, &hs));

    try testing.expectEqual(@as(u64, 7), fs.mem.hs_term);
    try testing.expectEqual(@as(u64, 3), fs.mem.hs_vote);
    try testing.expectEqual(@as(u64, 2), fs.mem.hs_commit);
    // 5 (header) + 24 (payload) + 4 (crc).
    try testing.expectEqual(@as(u64, 5 + 24 + 4), fs.wal_offset);
}

test "FileStorage: truncate-tail rewinds the WAL + drops mem entries" {
    var h = try Harness.init();
    defer h.deinit();

    const fs = try FileStorage.init(testing.allocator, &.{1}, h.path);
    defer fs.deinit();

    // Append entries 1, 2, 3.
    inline for ([_]u64{ 1, 2, 3 }) |i| {
        const e = fakeEntry(1, i, "x");
        var arr = [_]c.RaftEntryFfi{e};
        try testing.expectEqual(@as(i32, 0), appendEntriesCb(fs, &arr, 1));
    }
    try testing.expectEqual(@as(u64, 3), fs.mem.lastIndex());
    const after_three = fs.wal_offset;
    const offset_of_two = fs.entry_offsets.items[2];
    try testing.expect(offset_of_two > 0 and offset_of_two < after_three);

    // Now overwrite from index 2 (leader-change conflict).
    const e2_new = fakeEntry(2, 2, "y");
    var arr2 = [_]c.RaftEntryFfi{e2_new};
    try testing.expectEqual(@as(i32, 0), appendEntriesCb(fs, &arr2, 1));

    // Mem dropped 2, 3 and re-appended new 2.
    try testing.expectEqual(@as(u64, 2), fs.mem.lastIndex());
    // Offset map: sentinel + 1 + new 2 = 3 slots; new 2's offset
    // is the position the old 2 sat at.
    try testing.expectEqual(@as(usize, 3), fs.entry_offsets.items.len);
    try testing.expectEqual(offset_of_two, fs.entry_offsets.items[2]);
    // WAL offset rewound then advanced by one new entry's worth.
    const expected = offset_of_two + 5 + (4 + 8 + 8 + 4 + 1 + 4 + 0 + 1) + 4;
    try testing.expectEqual(expected, fs.wal_offset);
}

test "FileStorage: flush is the durability boundary (pre-flush size on disk may lag)" {
    var h = try Harness.init();
    defer h.deinit();

    const fs = try FileStorage.init(testing.allocator, &.{1}, h.path);
    defer fs.deinit();

    const e = fakeEntry(1, 1, "x");
    var arr = [_]c.RaftEntryFfi{e};
    try testing.expectEqual(@as(i32, 0), appendEntriesCb(fs, &arr, 1));

    // Post-write, before flush: the writevAll call already
    // transferred the bytes; what flush() guarantees is the
    // *fsync*. We can't directly observe "in page cache only"
    // from Zig without OS-specific tricks, so we just assert the
    // flush itself succeeds and the file size matches the
    // expected logical extent. (The real value of flush is
    // measured by the benchmark, not asserted by a unit test.)
    try fs.flush();
    var rf = try std.fs.cwd().openFile(h.path, .{ .mode = .read_only });
    defer rf.close();
    const stat = try rf.stat();
    try testing.expectEqual(fs.wal_offset, stat.size);
}

test "FileStorage: vtable read callbacks delegate to MemStorage" {
    var h = try Harness.init();
    defer h.deinit();

    const fs = try FileStorage.init(testing.allocator, &.{ 1, 2, 3 }, h.path);
    defer fs.deinit();

    // initial_state: should reflect the voters we passed.
    var hs_out: c.RaftHardStateFfi = undefined;
    var cs_out: c.RaftConfStateFfi = undefined;
    try testing.expectEqual(@as(i32, 0), initialStateCb(fs, &hs_out, &cs_out));
    try testing.expectEqual(@as(usize, 3), cs_out.voters_len);
    try testing.expectEqual(@as(u64, 1), cs_out.voters[0]);

    // first/last_index on empty log.
    var first_out: u64 = 0;
    var last_out: u64 = 0;
    try testing.expectEqual(@as(i32, 0), firstIndexCb(fs, &first_out));
    try testing.expectEqual(@as(i32, 0), lastIndexCb(fs, &last_out));
    try testing.expectEqual(@as(u64, 1), first_out);
    try testing.expectEqual(@as(u64, 0), last_out);
}
