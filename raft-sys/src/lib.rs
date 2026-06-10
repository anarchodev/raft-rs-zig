use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::c_void;
use std::ptr;
use std::slice;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Mutex;

use protobuf::Message as PbMessage;
use raft::eraftpb::{ConfState, HardState, Snapshot, SnapshotMetadata};
use raft::prelude::*;
use raft::storage::Storage;
use raft::{Config, GetEntriesContext, RaftState, RawNode, StateRole};
use slog::{o, Discard, Logger};

// ─── Flat FFI types for crossing the boundary ───────────────────────────────

#[repr(C)]
pub struct RaftEntryFfi {
    /// 0 = Normal, 1 = ConfChange, 2 = ConfChangeV2
    pub entry_type: u32,
    pub term: u64,
    pub index: u64,
    pub data: *const u8,
    pub data_len: usize,
    pub context: *const u8,
    pub context_len: usize,
    pub sync_log: bool,
}

#[repr(C)]
pub struct RaftHardStateFfi {
    pub term: u64,
    pub vote: u64,
    pub commit: u64,
}

#[repr(C)]
pub struct RaftConfStateFfi {
    pub voters: *const u64,
    pub voters_len: usize,
    pub learners: *const u64,
    pub learners_len: usize,
    pub voters_outgoing: *const u64,
    pub voters_outgoing_len: usize,
    pub learners_next: *const u64,
    pub learners_next_len: usize,
    pub auto_leave: bool,
}

/// Returned from the `entries` callback. The descriptor array is borrowed until
/// `free_fn` is invoked (NULL means no free needed). The `data`/`context`
/// pointers inside each entry are borrowed for the duration of the call only —
/// Rust copies them immediately.
#[repr(C)]
pub struct RaftEntriesOut {
    pub entries: *const RaftEntryFfi,
    pub len: usize,
    pub free_fn: Option<unsafe extern "C" fn(entries: *const RaftEntryFfi, len: usize)>,
}

/// Storage callback table. All callbacks return 0 on success, non-zero on error.
/// `userdata` is opaque to Rust and passed back to every callback.
#[repr(C)]
pub struct RaftStorageVTable {
    pub initial_state: Option<
        unsafe extern "C" fn(*mut c_void, *mut RaftHardStateFfi, *mut RaftConfStateFfi) -> i32,
    >,
    pub entries: Option<
        unsafe extern "C" fn(*mut c_void, u64, u64, u64, *mut RaftEntriesOut) -> i32,
    >,
    pub term: Option<unsafe extern "C" fn(*mut c_void, u64, *mut u64) -> i32>,
    pub first_index: Option<unsafe extern "C" fn(*mut c_void, *mut u64) -> i32>,
    pub last_index: Option<unsafe extern "C" fn(*mut c_void, *mut u64) -> i32>,
    pub snapshot: Option<
        unsafe extern "C" fn(
            *mut c_void,
            u64,
            *mut *const u8,
            *mut usize,
            *mut u64,
            *mut u64,
        ) -> i32,
    >,
    pub append_entries:
        Option<unsafe extern "C" fn(*mut c_void, *const RaftEntryFfi, usize) -> i32>,
    pub set_hard_state: Option<unsafe extern "C" fn(*mut c_void, *const RaftHardStateFfi) -> i32>,
    pub apply_snapshot:
        Option<unsafe extern "C" fn(*mut c_void, *const u8, usize, u64, u64) -> i32>,
    /// Called exactly once when the group is destroyed. Lets the implementer
    /// release any resources tied to `userdata`.
    pub destroy: Option<unsafe extern "C" fn(*mut c_void)>,
}

// ─── Bridge: implement raft-rs Storage by trampolining through the vtable ────

pub struct FfiStorage {
    vtable: RaftStorageVTable,
    userdata: *mut c_void,
}

unsafe impl Send for FfiStorage {}
unsafe impl Sync for FfiStorage {}

impl Drop for FfiStorage {
    fn drop(&mut self) {
        if let Some(destroy) = self.vtable.destroy {
            unsafe { destroy(self.userdata) };
        }
    }
}

fn store_unavailable() -> raft::Error {
    raft::Error::Store(raft::StorageError::Unavailable)
}

/// Map a storage callback's return code to a raft error. rc -2 means
/// the requested index/range is below first_index — the entries were
/// compacted away, so raft must fall back to a snapshot (Compacted is a
/// very different signal from Unavailable, which means "not yet here").
fn store_err_from_rc(rc: i32) -> raft::Error {
    if rc == -2 {
        raft::Error::Store(raft::StorageError::Compacted)
    } else {
        store_unavailable()
    }
}

fn snap_unavailable() -> raft::Error {
    raft::Error::Store(raft::StorageError::SnapshotTemporarilyUnavailable)
}

impl Storage for FfiStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        let cb = self.vtable.initial_state.ok_or_else(store_unavailable)?;
        let mut hs = RaftHardStateFfi {
            term: 0,
            vote: 0,
            commit: 0,
        };
        let mut cs = RaftConfStateFfi {
            voters: ptr::null(),
            voters_len: 0,
            learners: ptr::null(),
            learners_len: 0,
            voters_outgoing: ptr::null(),
            voters_outgoing_len: 0,
            learners_next: ptr::null(),
            learners_next_len: 0,
            auto_leave: false,
        };
        let rc = unsafe { cb(self.userdata, &mut hs, &mut cs) };
        if rc != 0 {
            return Err(store_unavailable());
        }
        let mut hard_state = HardState::default();
        hard_state.set_term(hs.term);
        hard_state.set_vote(hs.vote);
        hard_state.set_commit(hs.commit);
        let mut conf_state = ConfState::default();
        if !cs.voters.is_null() && cs.voters_len > 0 {
            conf_state.set_voters(
                unsafe { slice::from_raw_parts(cs.voters, cs.voters_len) }.to_vec(),
            );
        }
        if !cs.learners.is_null() && cs.learners_len > 0 {
            conf_state.set_learners(
                unsafe { slice::from_raw_parts(cs.learners, cs.learners_len) }.to_vec(),
            );
        }
        if !cs.voters_outgoing.is_null() && cs.voters_outgoing_len > 0 {
            conf_state.set_voters_outgoing(
                unsafe { slice::from_raw_parts(cs.voters_outgoing, cs.voters_outgoing_len) }
                    .to_vec(),
            );
        }
        if !cs.learners_next.is_null() && cs.learners_next_len > 0 {
            conf_state.set_learners_next(
                unsafe { slice::from_raw_parts(cs.learners_next, cs.learners_next_len) }.to_vec(),
            );
        }
        conf_state.set_auto_leave(cs.auto_leave);
        Ok(RaftState {
            hard_state,
            conf_state,
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _ctx: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        let cb = self.vtable.entries.ok_or_else(store_unavailable)?;
        let max = max_size.into().unwrap_or(u64::MAX);
        let mut out = RaftEntriesOut {
            entries: ptr::null(),
            len: 0,
            free_fn: None,
        };
        let rc = unsafe { cb(self.userdata, low, high, max, &mut out) };
        if rc != 0 {
            return Err(store_err_from_rc(rc));
        }
        let entries: Vec<Entry> = if out.len > 0 && !out.entries.is_null() {
            let descriptors = unsafe { slice::from_raw_parts(out.entries, out.len) };
            descriptors
                .iter()
                .map(|e| {
                    let mut entry = Entry::default();
                    let entry_type = match e.entry_type {
                        1 => EntryType::EntryConfChange,
                        2 => EntryType::EntryConfChangeV2,
                        _ => EntryType::EntryNormal,
                    };
                    entry.set_entry_type(entry_type);
                    entry.set_term(e.term);
                    entry.set_index(e.index);
                    if !e.data.is_null() && e.data_len > 0 {
                        entry.set_data(
                            unsafe { slice::from_raw_parts(e.data, e.data_len) }
                                .to_vec()
                                .into(),
                        );
                    }
                    if !e.context.is_null() && e.context_len > 0 {
                        entry.set_context(
                            unsafe { slice::from_raw_parts(e.context, e.context_len) }
                                .to_vec()
                                .into(),
                        );
                    }
                    entry.set_sync_log(e.sync_log);
                    entry
                })
                .collect()
        } else {
            Vec::new()
        };
        if let Some(free) = out.free_fn {
            unsafe { free(out.entries, out.len) };
        }
        Ok(entries)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        let cb = self.vtable.term.ok_or_else(store_unavailable)?;
        let mut out = 0u64;
        let rc = unsafe { cb(self.userdata, idx, &mut out) };
        if rc != 0 {
            return Err(store_err_from_rc(rc));
        }
        Ok(out)
    }

    fn first_index(&self) -> raft::Result<u64> {
        let cb = self.vtable.first_index.ok_or_else(store_unavailable)?;
        let mut out = 0u64;
        let rc = unsafe { cb(self.userdata, &mut out) };
        if rc != 0 {
            return Err(store_unavailable());
        }
        Ok(out)
    }

    fn last_index(&self) -> raft::Result<u64> {
        let cb = self.vtable.last_index.ok_or_else(store_unavailable)?;
        let mut out = 0u64;
        let rc = unsafe { cb(self.userdata, &mut out) };
        if rc != 0 {
            return Err(store_unavailable());
        }
        Ok(out)
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        let cb = self.vtable.snapshot.ok_or_else(snap_unavailable)?;
        let mut data: *const u8 = ptr::null();
        let mut data_len: usize = 0;
        let mut meta_index: u64 = 0;
        let mut meta_term: u64 = 0;
        let rc = unsafe {
            cb(
                self.userdata,
                request_index,
                &mut data,
                &mut data_len,
                &mut meta_index,
                &mut meta_term,
            )
        };
        if rc != 0 {
            return Err(snap_unavailable());
        }
        let mut snap = Snapshot::default();
        if !data.is_null() && data_len > 0 {
            snap.set_data(
                unsafe { slice::from_raw_parts(data, data_len) }
                    .to_vec()
                    .into(),
            );
        }
        let mut meta = SnapshotMetadata::default();
        meta.set_index(meta_index);
        meta.set_term(meta_term);
        snap.set_metadata(meta);
        Ok(snap)
    }
}

// ─── Apply callback (Rust → caller, per committed entry) ─────────────────────

pub type RaftApplyCb = unsafe extern "C" fn(
    userdata: *mut c_void,
    group_id: u64,
    index: u64,
    term: u64,
    data: *const u8,
    len: usize,
);

/// Message callback (Rust → caller, per outbound raft message).
/// Invoked from `raft_manager_take_messages` for each message that
/// `processReady` buffered into the group's outbox. `to` is the
/// raft node id of the intended recipient (encoded inside
/// `msg_bytes` too, but exposed here so the caller can route
/// without parsing the protobuf). `msg_bytes`/`msg_len` is the
/// rust-protobuf serialization of a `raft::eraftpb::Message` and
/// is valid only for the duration of the callback.
pub type RaftMessageCb = unsafe extern "C" fn(
    userdata: *mut c_void,
    to: u64,
    msg_bytes: *const u8,
    msg_len: usize,
);

// ─── Manager ─────────────────────────────────────────────────────────────────

// ── Mailbox / ready-channel state machine ─────────────────────────────
//
// raft-rs is purely the consensus state machine; it has no concept of
// "this group has new work". Without help, callers must iterate every
// registered group on every pump cycle to find the few that do — which
// turned into a hard ceiling at K = thousands of mostly-idle groups
// (rewind2's K=10k zipf bench was bound by O(K) `poll_ready` scans).
//
// The fix, modeled on TiKV's batch-system, is a per-slot "notification"
// state plus a manager-level FIFO of groups known to have work:
//
//   * Every op that produces work (`propose`, `step`, `campaign`,
//     `tick`-that-fires-something) calls `notify_locked(slot,
//     group_id)`. The helper CAS-flips `slot.state` from IDLE to
//     NOTIFIED and on success pushes `group_id` onto the manager's
//     `pending` queue.
//
//   * `raft_manager_poll_ready` drains `pending` instead of iterating
//     `groups`. Cost is O(returned-count), independent of total K.
//
//   * After processing a group (process_ready + take_messages), the
//     caller must call `raft_manager_release(group_id)` so the slot
//     can transition NOTIFIED → IDLE. Release also checks whether new
//     work landed during the round (raft state advanced or outbox
//     refilled by a concurrent step/propose) and re-notifies if so;
//     this is the load-bearing trick that catches the race where
//     `notify` saw NOTIFIED + skipped the push because the slot was
//     "already in queue", but work arrived after process_ready had
//     drained it.
//
// The atomics make the design safe for concurrent notify callers (a
// future transport thread receiving inbound messages off-pump). With a
// single-threaded pump today the contention is zero; the atomics are
// free.

const NOTIFY_IDLE: u8 = 0;
const NOTIFY_NOTIFIED: u8 = 1;
const NOTIFY_DROP: u8 = 2;

/// Per-group state held by the manager: the raw raft node plus a
/// caller-drained outbox of outbound messages produced by the
/// most recent `processReady` cycles.
struct GroupSlot {
    node: RawNode<FfiStorage>,
    /// (to, serialized message bytes). Drained by
    /// `raft_manager_take_messages`. v1 multi-node transport
    /// queries this between every `processReady` to ferry
    /// messages to peer nodes.
    outbox: Vec<(u64, Vec<u8>)>,
    /// Mailbox state — `NOTIFY_IDLE` / `NOTIFY_NOTIFIED` / `NOTIFY_DROP`.
    /// See the `mailbox` block-comment above.
    state: AtomicU8,
    /// Readies appended-but-not-yet-fsynced: `(ready number,
    /// persisted messages)` stashed by `raft_manager_process_ready`'s
    /// async-append flow and released by `raft_manager_on_persist`
    /// once the caller's WAL fsync covers them. The messages here are
    /// the ones raft REQUIRES durable local state for (append acks,
    /// vote responses) — holding them until the fsync is what makes a
    /// follower's ack mean "durably stored", and acking raft's own
    /// persist watermark (`on_persist_ready`) only after the fsync is
    /// what keeps the LEADER's self-vote out of the commit quorum for
    /// entries that are still volatile.
    pending_persist: Vec<(u64, Vec<(u64, Vec<u8>)>)>,
    /// Migration fence epoch — see the `epoch / fence` block-comment
    /// below. The group's current incarnation number. `step_fenced`
    /// rejects any inbound message stamped with a strictly-older
    /// epoch, so traffic addressed to a pre-migration incarnation of
    /// this group id cannot revive or confuse the new one (raft's own
    /// term-based fencing can't do this: terms reset across an
    /// incarnation boundary, so a stale message can carry a higher
    /// term than the fresh group and wrongly depose its leader).
    epoch: u64,
}

// ── Epoch / fence ─────────────────────────────────────────────────────
//
// No-downtime tenant migration moves a group id from one node (one DP
// cluster) to another by detaching committed state and re-creating the
// group at the destination. The destination starts a *fresh* raft
// incarnation (term/commit-idx reset — see the migration learnings doc
// §4). That reset is exactly what makes stale in-flight messages
// dangerous: a message from the old incarnation can carry a term
// higher than the fresh group's, and raft would honour it (step down a
// just-elected leader, accept a stale append). raft's term mechanism
// fences *within* one incarnation; it cannot fence *across* one.
//
// The epoch is the cross-incarnation fence. It is a per-group counter
// the control plane assigns monotonically across moves (the CP's
// tenant registry owns allocation; the engine only stores + enforces).
// Outbound messages are stamped with the sending group's epoch at the
// transport layer (read via `raft_manager_group_epoch`); the receiver
// passes that stamp to `step_fenced`, which drops anything strictly
// older than the local epoch (TiKV's `is_epoch_stale` shape). A group
// that never migrates stays at epoch 0 and every message stamps 0, so
// the fence is a no-op (0 < 0 is false) and costs a single compare.

/// Epoch carried by an inbound message was older than the group's
/// current incarnation — the message is stale (addressed to a
/// pre-migration incarnation) and was dropped. Distinct from the other
/// `step` failure codes so the transport can tell a fence-drop apart
/// from a decode error or an unknown group.
const STEP_FENCED: i32 = -4;

/// Multi-raft manager. Caller-serialized for the high-volume per-
/// group ops (propose/step/process_ready/take_messages); `notify` is
/// safe to call from any thread.
///
/// cbindgen:opaque
pub struct RaftManager {
    groups: HashMap<u64, GroupSlot>,
    tombstones: HashSet<u64>,
    /// FIFO of group ids that have work pending — populated by
    /// `notify_locked`, drained by `raft_manager_poll_ready`. May
    /// contain stale ids (group destroyed between notify and drain);
    /// the per-id ops tolerate "unknown group" as a no-op.
    pending: Mutex<VecDeque<u64>>,
    logger: Logger,
}

/// CAS-flip a slot from IDLE→NOTIFIED. On success push the
/// `group_id` onto `pending` so the next `poll_ready` returns it.
/// On any other prior state (NOTIFIED → already in queue, DROP →
/// destroyed), no-op. The slot reference is borrowed for the CAS
/// only; the `pending` push happens after we release that borrow so
/// the lock order is consistent (`pending.lock()` is never held
/// across slot access).
fn notify_locked(slot_state: &AtomicU8, pending: &Mutex<VecDeque<u64>>, group_id: u64) {
    // `compare_exchange` semantics: only the IDLE→NOTIFIED transition
    // pushes onto `pending`. Concurrent notifies after the first see
    // NOTIFIED and skip.
    if slot_state
        .compare_exchange(
            NOTIFY_IDLE,
            NOTIFY_NOTIFIED,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .is_ok()
    {
        if let Ok(mut q) = pending.lock() {
            q.push_back(group_id);
        }
    }
}

#[no_mangle]
pub extern "C" fn raft_manager_new() -> *mut RaftManager {
    Box::into_raw(Box::new(RaftManager {
        groups: HashMap::new(),
        tombstones: HashSet::new(),
        pending: Mutex::new(VecDeque::new()),
        logger: Logger::root(Discard, o!()),
    }))
}

#[no_mangle]
pub unsafe extern "C" fn raft_manager_free(m: *mut RaftManager) {
    if !m.is_null() {
        drop(Box::from_raw(m));
    }
}

/// Create a raft group backed by a caller-supplied storage vtable.
///
/// `epoch` is the group's birth incarnation number for the migration
/// fence (see the `epoch / fence` block-comment). Fresh, never-migrated
/// groups pass 0. A migration attach passes the control-plane-assigned
/// epoch for this move so the fence is in force from the instant the
/// group exists — closing the window a separate `set_epoch` after
/// `create_group` would leave open.
///
/// Returns 0 on success, -1 if a group with `group_id` exists, -2 if it is
/// tombstoned, -3 on bad input or internal raft error.
#[no_mangle]
pub unsafe extern "C" fn raft_manager_create_group(
    m: *mut RaftManager,
    group_id: u64,
    node_id: u64,
    epoch: u64,
    vtable: *const RaftStorageVTable,
    storage_userdata: *mut c_void,
) -> i32 {
    if m.is_null() || vtable.is_null() || node_id == 0 {
        return -3;
    }
    let mgr = &mut *m;
    if mgr.tombstones.contains(&group_id) {
        return -2;
    }
    if mgr.groups.contains_key(&group_id) {
        return -1;
    }
    let storage = FfiStorage {
        vtable: ptr::read(vtable),
        userdata: storage_userdata,
    };
    let cfg = Config {
        id: node_id,
        election_tick: 10,
        heartbeat_tick: 3,
        ..Default::default()
    };
    match RawNode::new(&cfg, storage, &mgr.logger) {
        Ok(node) => {
            mgr.groups.insert(
                group_id,
                GroupSlot {
                    node,
                    outbox: Vec::new(),
                    state: AtomicU8::new(NOTIFY_IDLE),
                    pending_persist: Vec::new(),
                    epoch,
                },
            );
            // A fresh group has no committed entries / no outbound
            // messages — no notify on create. The first `propose` /
            // `step` / `campaign` is what kicks the group into the
            // ready channel.
            0
        }
        Err(_) => -3,
    }
}

#[no_mangle]
pub unsafe extern "C" fn raft_manager_destroy_group(
    m: *mut RaftManager,
    group_id: u64,
) -> i32 {
    let mgr = &mut *m;
    // Set DROP before removing so any in-flight `notify` racing with
    // destroy sees DROP and skips the push. (Notify holds the slot
    // borrow only across the CAS; the slot itself is gone after
    // `remove`, but stale group_ids in `pending` are harmless —
    // per-id ops treat unknown groups as no-ops.)
    if let Some(slot) = mgr.groups.get(&group_id) {
        slot.state.store(NOTIFY_DROP, Ordering::Release);
    }
    if mgr.groups.remove(&group_id).is_some() {
        mgr.tombstones.insert(group_id);
        0
    } else {
        -1
    }
}

#[no_mangle]
pub unsafe extern "C" fn raft_manager_has_group(m: *const RaftManager, group_id: u64) -> bool {
    let mgr = &*m;
    mgr.groups.contains_key(&group_id)
}

#[no_mangle]
pub unsafe extern "C" fn raft_manager_is_tombstoned(
    m: *const RaftManager,
    group_id: u64,
) -> bool {
    let mgr = &*m;
    mgr.tombstones.contains(&group_id)
}

/// Lift the tombstone for `group_id`, allowing a subsequent
/// `create_group` with that id to succeed. This is the migration
/// primitive's escape hatch: a tenant detached on one node (which
/// adds its id to the local tombstone set) and attached again
/// later (perhaps after a round-trip through `Bundle` serialization
/// to a different node) MUST be able to re-create the group. The
/// tombstone-prevents-reuse safety check is intended for accidental
/// id collisions; intentional reuse calls this first.
///
/// Returns 0 unconditionally — calling on a non-tombstoned id is
/// a no-op, not an error.
#[no_mangle]
pub unsafe extern "C" fn raft_manager_clear_tombstone(
    m: *mut RaftManager,
    group_id: u64,
) -> i32 {
    if m.is_null() {
        return -3;
    }
    let mgr = &mut *m;
    mgr.tombstones.remove(&group_id);
    0
}

/// Read a group's current fence epoch. The transport stamps each
/// outbound message with this so the receiving `step_fenced` can drop
/// stale (older-incarnation) traffic. Returns 0 for an unknown group —
/// indistinguishable from a legitimate epoch-0 group, so callers that
/// need the difference check `raft_manager_has_group` first.
#[no_mangle]
pub unsafe extern "C" fn raft_manager_group_epoch(
    m: *const RaftManager,
    group_id: u64,
) -> u64 {
    let mgr = &*m;
    mgr.groups.get(&group_id).map(|s| s.epoch).unwrap_or(0)
}

/// Set a group's fence epoch. The intended caller is the control
/// plane bumping the epoch on a live group — e.g. re-fencing after a
/// membership change, or raising it as part of a move when the group
/// is attached in place rather than re-created. Birth-epoch for a
/// migration attach should go through `create_group`'s `epoch`
/// parameter instead, which has no set-after-create window.
///
/// Monotonic: a request to lower the epoch is rejected, because going
/// backwards would re-admit traffic the fence has already excluded.
///
/// Returns 0 on success, -1 if the group is unknown, -2 if `epoch` is
/// strictly less than the group's current epoch (regression rejected).
#[no_mangle]
pub unsafe extern "C" fn raft_manager_set_epoch(
    m: *mut RaftManager,
    group_id: u64,
    epoch: u64,
) -> i32 {
    if m.is_null() {
        return -1;
    }
    let mgr = &mut *m;
    match mgr.groups.get_mut(&group_id) {
        Some(slot) => {
            if epoch < slot.epoch {
                return -2;
            }
            slot.epoch = epoch;
            0
        }
        None => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn raft_manager_group_count(m: *const RaftManager) -> usize {
    let mgr = &*m;
    mgr.groups.len()
}

#[no_mangle]
pub unsafe extern "C" fn raft_manager_tick(m: *mut RaftManager, group_id: u64) -> i32 {
    let mgr = &mut *m;
    let pending = &mgr.pending;
    match mgr.groups.get_mut(&group_id) {
        Some(slot) => {
            slot.node.tick();
            // Tick on an idle follower usually produces nothing; only
            // notify when raft state actually has new work. Cheap
            // `has_ready` + `is_empty` checks gate the CAS, so the
            // common idle-tick path doesn't even touch the atomic.
            if slot.node.has_ready() || !slot.outbox.is_empty() {
                notify_locked(&slot.state, pending, group_id);
            }
            0
        }
        None => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn raft_manager_campaign(m: *mut RaftManager, group_id: u64) -> i32 {
    let mgr = &mut *m;
    let pending = &mgr.pending;
    match mgr.groups.get_mut(&group_id) {
        Some(slot) => match slot.node.campaign() {
            Ok(()) => {
                // campaign generates vote requests — always notify.
                notify_locked(&slot.state, pending, group_id);
                0
            }
            Err(_) => -2,
        },
        None => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn raft_manager_propose(
    m: *mut RaftManager,
    group_id: u64,
    data: *const u8,
    len: usize,
) -> i32 {
    let mgr = &mut *m;
    let slot = match mgr.groups.get_mut(&group_id) {
        Some(s) => s,
        None => return -1,
    };
    // Leader gate: raft-rs 0.7's `step_follower` FORWARDS MsgPropose to
    // the known leader (this version has no disable_proposal_forwarding),
    // so a follower-side propose would commit cluster-wide while the
    // caller's local commit tracking faults the seq — "write failed" with
    // the write durable. The bridge contract is the inverse ("a rejected
    // propose never commits"): refuse here, before raft sees the message.
    // -2 maps to Error.NotLeader in manager.zig.
    if slot.node.raft.state != StateRole::Leader {
        return -2;
    }
    let bytes = if len == 0 {
        Vec::new()
    } else {
        slice::from_raw_parts(data, len).to_vec()
    };
    match slot.node.propose(vec![], bytes) {
        Ok(()) => {
            notify_locked(&slot.state, &mgr.pending, group_id);
            0
        }
        Err(_) => -2,
    }
}

/// Deliver an inbound raft message from a peer. `msg_bytes` is the
/// rust-protobuf serialization of a `raft::eraftpb::Message`
/// (typically produced by the peer's `raft_manager_take_messages`
/// callback). Returns 0 on success, -1 if the group is unknown,
/// -2 if the message fails to deserialize, -3 if `step` rejects it.
#[no_mangle]
pub unsafe extern "C" fn raft_manager_step(
    m: *mut RaftManager,
    group_id: u64,
    msg_bytes: *const u8,
    msg_len: usize,
) -> i32 {
    if msg_bytes.is_null() {
        return -2;
    }
    let mgr = &mut *m;
    let slot = match mgr.groups.get_mut(&group_id) {
        Some(s) => s,
        None => return -1,
    };
    let bytes = slice::from_raw_parts(msg_bytes, msg_len);
    let msg = match Message::parse_from_bytes(bytes) {
        Ok(m) => m,
        Err(_) => return -2,
    };
    match slot.node.step(msg) {
        Ok(()) => {
            notify_locked(&slot.state, &mgr.pending, group_id);
            0
        }
        Err(_) => -3,
    }
}

/// Deliver an inbound raft message, fenced by the migration epoch.
/// Identical to `raft_manager_step` except it first compares
/// `sender_epoch` (the epoch the sending group was stamped with, read
/// from `raft_manager_group_epoch` at the source) against the local
/// group's current epoch. If `sender_epoch` is strictly older, the
/// message belongs to a pre-migration incarnation of this group id and
/// is dropped — the message never reaches raft, so its (post-reset)
/// term cannot perturb the fresh group. See the `epoch / fence`
/// block-comment for why term-based fencing is insufficient here.
///
/// Returns 0 on success, -1 if the group is unknown, -2 if the message
/// fails to deserialize, -3 if `step` rejects it, -4 (`STEP_FENCED`)
/// if the message is fenced (stale epoch). The fence check precedes
/// the protobuf decode, so a flood of stale traffic costs only a
/// compare per message.
#[no_mangle]
pub unsafe extern "C" fn raft_manager_step_fenced(
    m: *mut RaftManager,
    group_id: u64,
    sender_epoch: u64,
    msg_bytes: *const u8,
    msg_len: usize,
) -> i32 {
    if msg_bytes.is_null() {
        return -2;
    }
    let mgr = &mut *m;
    let slot = match mgr.groups.get_mut(&group_id) {
        Some(s) => s,
        None => return -1,
    };
    if sender_epoch < slot.epoch {
        return STEP_FENCED;
    }
    let bytes = slice::from_raw_parts(msg_bytes, msg_len);
    let msg = match Message::parse_from_bytes(bytes) {
        Ok(m) => m,
        Err(_) => return -2,
    };
    match slot.node.step(msg) {
        Ok(()) => {
            notify_locked(&slot.state, &mgr.pending, group_id);
            0
        }
        Err(_) => -3,
    }
}

/// One entry in a batched step call: `(group_id, epoch, msg_bytes,
/// msg_len)`. `msg_bytes` is the rust-protobuf serialization of a
/// `raft::eraftpb::Message` (same as `raft_manager_step`). `epoch` is
/// the sender's stamped fence epoch (same role as `sender_epoch` in
/// `raft_manager_step_fenced`); pass 0 for never-migrated groups.
/// Layout is stable C ABI; callers building this array on the Zig side
/// use `extern struct` so the layout matches exactly.
#[repr(C)]
pub struct RaftStepBatchEntry {
    pub group_id: u64,
    pub epoch: u64,
    pub msg_bytes: *const u8,
    pub msg_len: usize,
}

/// Batch-step many inbound raft messages in one FFI call. For each
/// entry, locate the group, decode the message, step it. **Bad
/// entries are skipped silently** — unknown groups, decode failures,
/// and step-rejected messages all count as no-ops without aborting
/// the rest of the batch. Returns the count of successful steps.
///
/// Mirrors `raft_manager_tick_groups`' skip-bad semantics: the batch
/// API is for "fast path" delivery of trusted, well-formed messages
/// from a peer; for per-message error inspection use `raft_manager_step`
/// instead. The migration fence applies here too — an entry whose
/// `epoch` is strictly older than its group's current epoch is dropped
/// (counted as a skip, not a success), so the coalesced fast path is
/// no easier to bypass the fence on than single-message `step_fenced`.
///
/// One FFI call per batch — the Zig↔C↔Rust boundary crossing is the
/// bulk of the per-message cost; amortizing it across an envelope of
/// N messages compounds with transport coalescing.
#[no_mangle]
pub unsafe extern "C" fn raft_manager_step_batch(
    m: *mut RaftManager,
    entries: *const RaftStepBatchEntry,
    count: usize,
) -> usize {
    if m.is_null() || count == 0 || entries.is_null() {
        return 0;
    }
    let mgr = &mut *m;
    let entries = slice::from_raw_parts(entries, count);
    let mut stepped: usize = 0;
    let pending = &mgr.pending;
    for e in entries {
        if e.msg_bytes.is_null() && e.msg_len > 0 {
            continue; // malformed entry (null ptr with non-zero len)
        }
        let slot = match mgr.groups.get_mut(&e.group_id) {
            Some(s) => s,
            None => continue, // unknown group, skip
        };
        if e.epoch < slot.epoch {
            continue; // stale incarnation — fenced, skip
        }
        let bytes = if e.msg_len == 0 {
            &[]
        } else {
            slice::from_raw_parts(e.msg_bytes, e.msg_len)
        };
        let msg = match Message::parse_from_bytes(bytes) {
            Ok(m) => m,
            Err(_) => continue, // decode failure, skip
        };
        if slot.node.step(msg).is_ok() {
            notify_locked(&slot.state, pending, e.group_id);
            stepped += 1;
        }
    }
    stepped
}

/// Drain the group's outbox of outbound raft messages. `cb` fires
/// once per message in FIFO order; the message bytes are valid only
/// for the duration of the callback (Rust frees them after).
/// Returns 0 on success, -1 if the group is unknown.
#[no_mangle]
pub unsafe extern "C" fn raft_manager_take_messages(
    m: *mut RaftManager,
    group_id: u64,
    cb: RaftMessageCb,
    userdata: *mut c_void,
) -> i32 {
    let mgr = &mut *m;
    // Stale id tolerance: see `raft_manager_process_ready`.
    let slot = match mgr.groups.get_mut(&group_id) {
        Some(s) => s,
        None => return 0,
    };
    // drain(..) yields ownership of each (to, Vec<u8>); the Vec
    // is dropped after the callback returns, so the pointer
    // handed out has lifetime = callback scope only.
    for (to, bytes) in slot.outbox.drain(..) {
        cb(userdata, to, bytes.as_ptr(), bytes.len());
    }
    0
}

#[no_mangle]
pub unsafe extern "C" fn raft_manager_is_leader(m: *const RaftManager, group_id: u64) -> bool {
    let mgr = &*m;
    mgr.groups
        .get(&group_id)
        .map(|s| s.node.raft.state == StateRole::Leader)
        .unwrap_or(false)
}

#[no_mangle]
pub unsafe extern "C" fn raft_manager_process_ready(
    m: *mut RaftManager,
    group_id: u64,
    cb: RaftApplyCb,
    userdata: *mut c_void,
) -> i32 {
    let mgr = &mut *m;
    // Stale id (group destroyed between notify and drain) → no-op.
    // The ready-channel design guarantees the caller may see ids
    // for groups that have since vanished; treating that as success
    // lets the caller loop without filtering.
    let slot = match mgr.groups.get_mut(&group_id) {
        Some(s) => s,
        None => return 0,
    };
    if !slot.node.has_ready() {
        return 0;
    }

    // Capture stable pointers to the storage vtable + userdata before the
    // mutable borrow that `node.ready()` will impose.
    let (vtable_ptr, store_userdata) = {
        let s = slot.node.store();
        (&s.vtable as *const RaftStorageVTable, s.userdata)
    };

    let mut ready = slot.node.ready();
    let ready_number = ready.number();

    // Persist hard state.
    if let Some(hs) = ready.hs() {
        if let Some(set_hs) = (*vtable_ptr).set_hard_state {
            let hs_ffi = RaftHardStateFfi {
                term: hs.term,
                vote: hs.vote,
                commit: hs.commit,
            };
            if set_hs(store_userdata, &hs_ffi) != 0 {
                return -2;
            }
        }
    }

    // Persist new entries.
    if !ready.entries().is_empty() {
        if let Some(append) = (*vtable_ptr).append_entries {
            let ffi: Vec<RaftEntryFfi> = ready
                .entries()
                .iter()
                .map(|e| RaftEntryFfi {
                    entry_type: match e.get_entry_type() {
                        EntryType::EntryConfChange => 1,
                        EntryType::EntryConfChangeV2 => 2,
                        _ => 0,
                    },
                    term: e.term,
                    index: e.index,
                    data: e.data.as_ptr(),
                    data_len: e.data.len(),
                    context: e.context.as_ptr(),
                    context_len: e.context.len(),
                    sync_log: e.sync_log,
                })
                .collect();
            if append(store_userdata, ffi.as_ptr(), ffi.len()) != 0 {
                return -3;
            }
        }
    }

    // Drain outbound messages into local vecs first — pushing
    // them to slot.outbox here would conflict with the live
    // borrow `ready` holds on slot.node. We park them, drop
    // `ready`, then push.
    //
    // ASYNC-APPEND SPLIT (persist-before-quorum ordering): raft
    // distinguishes messages that may leave BEFORE this ready's
    // writes are durable (`messages()` — e.g. a leader's MsgAppend
    // fan-out, which asserts nothing about local durability) from
    // messages that must wait for the fsync (`persisted_messages()`
    // — append acks / vote responses, which DO assert it). The old
    // sync flow pushed both to the outbox and called
    // `advance(ready)`, which marks the ready persisted immediately
    // — so the LEADER's own un-fsynced append counted toward the
    // commit quorum, and on a single voter an entry could commit,
    // apply, and (until the node-level commit hook moved post-flush)
    // be acked entirely ahead of the fsync. Now the ready is only
    // RECORDED (`advance_append_async`); its number + persisted
    // messages stash on the slot until the caller's WAL fsync
    // completes and `raft_manager_on_persist` acks them — raft's
    // commit math then counts this node only once its entries are
    // truly durable. Committed entries handed out by THIS ready are
    // already gated by raft on the persist watermark (they were
    // fsynced in an earlier cycle), so applying them here is safe.
    let ready_msgs = ready.take_messages();
    let persisted_msgs = ready.take_persisted_messages();

    apply_committed(group_id, ready.take_committed_entries(), cb, userdata);

    slot.node.advance_append_async(ready);
    slot.node.advance_apply();

    // (The old sync flow's LightReady commit-index persistence is
    // gone with it: in the async flow a commit advance surfaces as a
    // changed `hs()` in a SUBSEQUENT ready — after `on_persist` —
    // and rides the normal hard-state persist above, one fsync
    // behind the advance. That lag is safe: raft only requires the
    // durable commit to be ≥ the compaction point, and compaction is
    // driven off the durabilized apply watermark, which itself trails
    // the persisted commit.)

    push_messages(&mut slot.outbox, ready_msgs);

    // Hold the persistence-asserting messages until the fsync; an
    // empty stash entry still records the number so `on_persist`
    // acks every ready in order.
    let mut stashed: Vec<(u64, Vec<u8>)> = Vec::with_capacity(persisted_msgs.len());
    push_messages(&mut stashed, persisted_msgs);
    slot.pending_persist.push((ready_number, stashed));

    0
}

/// Acknowledge that every ready `raft_manager_process_ready` produced
/// for this group SO FAR has been made durable (the caller's WAL
/// fsync covers them). Releases the stashed persistence-asserting
/// messages to the outbox and advances raft's persist watermark
/// (`on_persist_ready`) — the point at which this node's own entries
/// start counting toward the commit quorum. The caller MUST invoke
/// this only after an fsync that covers the corresponding
/// `process_ready` appends, and should poll readiness again
/// afterwards: entries that just became persisted commonly unlock a
/// commit advance + committed entries to apply (surfaced as a fresh
/// ready). Returns 0 on success (including "nothing pending" and
/// unknown group — both no-ops).
#[no_mangle]
pub unsafe extern "C" fn raft_manager_on_persist(m: *mut RaftManager, group_id: u64) -> i32 {
    let mgr = &mut *m;
    let pending = &mgr.pending;
    let slot = match mgr.groups.get_mut(&group_id) {
        Some(s) => s,
        None => return 0,
    };
    if slot.pending_persist.is_empty() {
        return 0;
    }
    let mut max_number: u64 = 0;
    for (number, msgs) in slot.pending_persist.drain(..) {
        if number > max_number {
            max_number = number;
        }
        for m in msgs {
            slot.outbox.push(m);
        }
    }
    slot.node.on_persist_ready(max_number);
    // The persist ack can unlock work (commit advance → committed
    // entries) and the outbox may now hold the released acks — make
    // sure the group surfaces in the ready channel either way.
    if slot.node.has_ready() || !slot.outbox.is_empty() {
        notify_locked(&slot.state, pending, group_id);
    }
    0
}

/// Serialize each `Message` via rust-protobuf and push into the
/// outbox keyed by recipient. Failed serializations are dropped
/// (logged in the future; currently silent — these should never
/// happen for well-formed raft messages).
fn push_messages(outbox: &mut Vec<(u64, Vec<u8>)>, msgs: Vec<Message>) {
    for msg in msgs {
        let to = msg.to;
        match msg.write_to_bytes() {
            Ok(bytes) => outbox.push((to, bytes)),
            Err(_) => { /* unreachable for valid messages */ }
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn raft_manager_tick_all(m: *mut RaftManager) -> usize {
    let mgr = &mut *m;
    let pending = &mgr.pending;
    let mut count = 0;
    for (gid, slot) in mgr.groups.iter_mut() {
        slot.node.tick();
        if slot.node.has_ready() || !slot.outbox.is_empty() {
            notify_locked(&slot.state, pending, *gid);
        }
        count += 1;
    }
    count
}

/// Batched per-group tick — caller supplies the set of group ids to
/// tick (typically the "active" set their hibernation policy
/// tracks). Returns the count of ids that resolved to live groups.
/// Single FFI call regardless of count, so the per-tenant FFI
/// overhead doesn't dominate when the active set is large.
///
/// Unknown ids are skipped silently (count excludes them) — lets
/// the caller pass a slightly stale active set without churn.
#[no_mangle]
pub unsafe extern "C" fn raft_manager_tick_groups(
    m: *mut RaftManager,
    group_ids: *const u64,
    count: usize,
) -> usize {
    if m.is_null() {
        return 0;
    }
    if count == 0 {
        return 0; // `from_raw_parts` rejects len=0 + possibly-null ptr.
    }
    if group_ids.is_null() {
        return 0;
    }
    let mgr = &mut *m;
    let ids = std::slice::from_raw_parts(group_ids, count);
    let mut ticked = 0;
    let pending = &mgr.pending;
    for &gid in ids {
        if let Some(slot) = mgr.groups.get_mut(&gid) {
            slot.node.tick();
            if slot.node.has_ready() || !slot.outbox.is_empty() {
                notify_locked(&slot.state, pending, gid);
            }
            ticked += 1;
        }
    }
    ticked
}

/// Drain up to `cap` group ids from the manager's ready channel into
/// `out_buf`. Returns the count written. Group ids in the channel
/// arrive via `notify_locked` — every op that produces work pushes
/// the owning group exactly once between successive `release`s for
/// that group.
///
/// O(returned count) — no scan of `mgr.groups`. The previous shape
/// iterated all registered groups every cycle and was the K=10k
/// scaling wall.
///
/// Stale ids (group destroyed between notify and drain) may be
/// returned; per-id ops (`process_ready`, `take_messages`,
/// `release`) tolerate unknown groups as no-ops. The caller doesn't
/// need to filter.
#[no_mangle]
pub unsafe extern "C" fn raft_manager_poll_ready(
    m: *const RaftManager,
    out_buf: *mut u64,
    cap: usize,
) -> usize {
    if out_buf.is_null() || cap == 0 {
        return 0;
    }
    let mgr = &*m;
    let mut q = match mgr.pending.lock() {
        Ok(q) => q,
        Err(_) => return 0,
    };
    let n = std::cmp::min(cap, q.len());
    for i in 0..n {
        // Unwrap: we drained no more than `q.len()`.
        ptr::write(out_buf.add(i), q.pop_front().unwrap());
    }
    n
}

/// Release a group back to IDLE after the caller has finished its
/// per-group work (process_ready + take_messages). If new work
/// landed during the round — propose / step / tick fired and saw
/// state NOTIFIED so didn't push — `release` catches it: after
/// state→IDLE, we check `has_ready` + outbox, and if either is
/// non-empty, re-notify so the next `poll_ready` returns this group.
///
/// Returns 0 on success, -1 if the group is unknown (treated as a
/// no-op: stale ids in the pending queue land here and we just drop
/// them).
#[no_mangle]
pub unsafe extern "C" fn raft_manager_release(
    m: *mut RaftManager,
    group_id: u64,
) -> i32 {
    let mgr = &mut *m;
    let pending = &mgr.pending;
    let slot = match mgr.groups.get_mut(&group_id) {
        Some(s) => s,
        None => return -1,
    };
    // Don't reset a DROP state — the slot is on its way out, and
    // anyone notifying it now should keep seeing DROP.
    let prev = slot
        .state
        .compare_exchange(
            NOTIFY_NOTIFIED,
            NOTIFY_IDLE,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .ok();
    if prev.is_none() {
        // State was DROP (or IDLE — caller shouldn't call release
        // on something they didn't drain, but tolerate it).
        return 0;
    }
    // Race check: did work land between process_ready returning and
    // our state→IDLE? If so, raft state reflects it; re-notify.
    if slot.node.has_ready() || !slot.outbox.is_empty() {
        notify_locked(&slot.state, pending, group_id);
    }
    0
}

unsafe fn apply_committed(
    group_id: u64,
    entries: Vec<Entry>,
    cb: RaftApplyCb,
    userdata: *mut c_void,
) {
    for entry in entries {
        if entry.data.is_empty() {
            continue;
        }
        if entry.get_entry_type() != EntryType::EntryNormal {
            continue;
        }
        cb(
            userdata,
            group_id,
            entry.index,
            entry.term,
            entry.data.as_ptr(),
            entry.data.len(),
        );
    }
}
