use std::collections::{HashMap, HashSet};
use std::ffi::c_void;
use std::ptr;
use std::slice;

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
            return Err(store_unavailable());
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
            return Err(store_unavailable());
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

// ─── Manager ─────────────────────────────────────────────────────────────────

/// Multi-raft manager. Caller-serialized.
///
/// cbindgen:opaque
pub struct RaftManager {
    groups: HashMap<u64, RawNode<FfiStorage>>,
    tombstones: HashSet<u64>,
    logger: Logger,
}

#[no_mangle]
pub extern "C" fn raft_manager_new() -> *mut RaftManager {
    Box::into_raw(Box::new(RaftManager {
        groups: HashMap::new(),
        tombstones: HashSet::new(),
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
/// Returns 0 on success, -1 if a group with `group_id` exists, -2 if it is
/// tombstoned, -3 on bad input or internal raft error.
#[no_mangle]
pub unsafe extern "C" fn raft_manager_create_group(
    m: *mut RaftManager,
    group_id: u64,
    node_id: u64,
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
            mgr.groups.insert(group_id, node);
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

#[no_mangle]
pub unsafe extern "C" fn raft_manager_group_count(m: *const RaftManager) -> usize {
    let mgr = &*m;
    mgr.groups.len()
}

#[no_mangle]
pub unsafe extern "C" fn raft_manager_tick(m: *mut RaftManager, group_id: u64) -> i32 {
    let mgr = &mut *m;
    match mgr.groups.get_mut(&group_id) {
        Some(node) => {
            node.tick();
            0
        }
        None => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn raft_manager_campaign(m: *mut RaftManager, group_id: u64) -> i32 {
    let mgr = &mut *m;
    match mgr.groups.get_mut(&group_id) {
        Some(node) => match node.campaign() {
            Ok(()) => 0,
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
    let node = match mgr.groups.get_mut(&group_id) {
        Some(n) => n,
        None => return -1,
    };
    let bytes = if len == 0 {
        Vec::new()
    } else {
        slice::from_raw_parts(data, len).to_vec()
    };
    match node.propose(vec![], bytes) {
        Ok(()) => 0,
        Err(_) => -2,
    }
}

#[no_mangle]
pub unsafe extern "C" fn raft_manager_is_leader(m: *const RaftManager, group_id: u64) -> bool {
    let mgr = &*m;
    mgr.groups
        .get(&group_id)
        .map(|n| n.raft.state == StateRole::Leader)
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
    let node = match mgr.groups.get_mut(&group_id) {
        Some(n) => n,
        None => return -1,
    };
    if !node.has_ready() {
        return 0;
    }

    // Capture stable pointers to the storage vtable + userdata before the
    // mutable borrow that `node.ready()` will impose.
    let (vtable_ptr, store_userdata) = {
        let s = node.store();
        (&s.vtable as *const RaftStorageVTable, s.userdata)
    };

    let mut ready = node.ready();

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

    // Single-node: drop messages.
    let _ = ready.take_messages();
    let _ = ready.take_persisted_messages();

    apply_committed(group_id, ready.take_committed_entries(), cb, userdata);

    let mut light_rd = node.advance(ready);
    let _ = light_rd.take_messages();
    apply_committed(group_id, light_rd.take_committed_entries(), cb, userdata);

    node.advance_apply();
    0
}

#[no_mangle]
pub unsafe extern "C" fn raft_manager_tick_all(m: *mut RaftManager) -> usize {
    let mgr = &mut *m;
    for node in mgr.groups.values_mut() {
        node.tick();
    }
    mgr.groups.len()
}

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
    let mut count = 0usize;
    for (gid, node) in mgr.groups.iter() {
        if count >= cap {
            break;
        }
        if node.has_ready() {
            ptr::write(out_buf.add(count), *gid);
            count += 1;
        }
    }
    count
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
