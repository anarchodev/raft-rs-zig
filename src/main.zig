//! raft-spike — the demo exe. Exercises the library's Manager +
//! MemStorage end-to-end: three groups elect leaders, propose
//! entries, apply via callback, then tear down. Useful as a
//! smoke test (`zig build run`) and as a starting reference for
//! consumers.

const std = @import("std");
const raft = @import("raft_rs_zig");

const ApplyCtx = struct {
    counts: [4]u32 = .{0} ** 4,
    total: u32 = 0,
};

fn applyCb(
    userdata: ?*anyopaque,
    group_id: u64,
    index: u64,
    term: u64,
    data: [*c]const u8,
    len: usize,
) callconv(.c) void {
    const ctx: *ApplyCtx = @ptrCast(@alignCast(userdata.?));
    const slice = data[0..len];
    std.debug.print("group={d} applied: index={d} term={d} data={s}\n", .{ group_id, index, term, slice });
    if (group_id < ctx.counts.len) ctx.counts[@intCast(group_id)] += 1;
    ctx.total += 1;
}

const Proposal = struct { gid: u64, data: []const u8 };

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .{};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var mgr = try raft.Manager.init();
    defer mgr.deinit();

    var ctx = ApplyCtx{};
    var buf: [16]u64 = undefined;
    const groups = [_]u64{ 1, 2, 3 };

    for (groups) |gid| {
        const st = try raft.MemStorage.init(allocator, &.{1});
        try mgr.createGroup(gid, 1, st);
        try mgr.campaign(gid);
    }

    var pump: u32 = 0;
    while (pump < 10) : (pump += 1) {
        const ready = mgr.pollReady(&buf);
        if (ready.len == 0) break;
        for (ready) |gid| try mgr.processReady(gid, applyCb, &ctx);
    }

    for (groups) |gid| {
        if (!mgr.isLeader(gid)) {
            std.debug.print("group {d} not leader\n", .{gid});
            return error.NoLeader;
        }
    }
    std.debug.print("all 3 groups elected leaders (storage owned by Zig)\n", .{});

    const proposals = [_]Proposal{
        .{ .gid = 1, .data = "g1-foo" },
        .{ .gid = 1, .data = "g1-bar" },
        .{ .gid = 2, .data = "g2-baz" },
        .{ .gid = 3, .data = "g3-quux" },
        .{ .gid = 3, .data = "g3-zap" },
    };
    for (proposals) |p| try mgr.propose(p.gid, p.data);

    var spin: u32 = 0;
    while (ctx.total < proposals.len and spin < 100) : (spin += 1) {
        mgr.tickAll();
        const ready = mgr.pollReady(&buf);
        for (ready) |gid| try mgr.processReady(gid, applyCb, &ctx);
    }

    std.debug.print(
        "done. total={d} (g1={d}, g2={d}, g3={d})\n",
        .{ ctx.total, ctx.counts[1], ctx.counts[2], ctx.counts[3] },
    );

    if (ctx.total != proposals.len) return error.NotAllApplied;
    if (ctx.counts[1] != 2 or ctx.counts[2] != 1 or ctx.counts[3] != 2) return error.WrongCounts;

    try mgr.destroyGroup(2);
    std.debug.print("group 2 destroyed; storage freed via destroy callback\n", .{});

    mgr.tickAll();
    std.debug.print("remaining groups: {d}\n", .{mgr.groupCount()});

    try mgr.destroyGroup(1);
    try mgr.destroyGroup(3);
}
