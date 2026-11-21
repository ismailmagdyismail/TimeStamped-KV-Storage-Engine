use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use crate::storage::{
    mvcc_control_plane::MVCCControlPlane,
    occ_comitter::{OccCommitResult, OccCommitter},
    versioned_store::VersionedStore,
};

/*
    - Could be better desinged by making RWLock a verionsed storage concern
        * thus If/When Atomic updates are supported, changes are local to VersionedStore
    - Commit Mutex should be kept where it is, since it is OCC concern
*/

/* OCC, MVCC
    - Core Steps Ideally
        1. Access
            - Lock-Free N Readers, N Writers
        2. Validation
            - Readers progressing with 1 Writer concurrently
        3. Publication after validation
            - Lock-Free (N Readers, 1 Writer)
        * this is the max theortical throughput we can get
        * we are restricting it a bit due to not having atomic versioned storage | atomic ptrs / COW / RCU
        * any 1 of those steps could be improved indepedntly
            Ex: publication
                - instead of R/W Lock could use AtomicPtr Swap / Cow + Deep Copy
                - could optimzie an extra level and make storage atomic and avoid deep copy

    - Readers, Writers should never block each other
        * done by having versions exist, never modifying in place and not violating Golden Rule
        * done by having updates to underlying strucutre being atomic
            - atomic head publishing
            - COW
            - No in-place multi-step updates
            ! this is the reason we are using a R/W lock below, since version storage
            is not yet a list of atomic chains that are updated in single step
            Thus readers may observer a Tree being re-balanced, links being invalidated etc...
    - Allows Readers, Writers to not have to state all their objects of interest up front
            - since all versions, don't have to hold lock to capture state
            - thus locks are not held at all, and if held are for a short period of time
            - since we are not worried about transactional capture, since all values exist can be accessed
    -  Allow Querying at a snapshot in time (maybe even in the past)

    - Commit point is the only syncing point where Writers run sequentially to avoid changing
    underlying storage while being validated
        * can be Done by a Global Commit Mutex or a Queue + CSP
*/

/*
    OCC, MVCC Policy VS implementation
    - Policy states
        * readers, writers don't interfere, wait, block
        * readers observer consistent snapshot
        * writers serialize at commit, validation point only
    - Implementation
        - Ideally to acheive all goals
            * COW/Atomic updates to underlying strucutre to avoid intermediate state
            published by writer to be observed by readers
        - In our case
            * since not yet atomic storage
            * we keep same algorithm , framwork, policy with more hardcore barriers till atomiciy is implemented
                - R/W Lock instead of atomic ptr swap
*/

pub struct StorageEngine {
    mvcc_control_plane: MVCCControlPlane,
    /*
    - [Not with accordance to OCC, MVCC]
    - RW lock here is not necessary
    - it could be done with Atomic Ptr, Acqurie+Release barriers to coordinate between readers, writer
    - kept temporarily: readers take the read lock and clone the Arc snapshot,
      writers publish a brand new Arc under the write lock (COW, never mutate in place)
    */
    versioned_storage: RwLock<Arc<VersionedStore>>,

    // Serializes commit validation + application + publication.
    commit_mutex: Mutex<()>,

    committer: OccCommitter,
}

impl StorageEngine {
    pub fn new() -> Self {
        let committer = OccCommitter::new();
        let versioned_storage = RwLock::new(Arc::new(VersionedStore::new()));
        let mvcc_control_plane = MVCCControlPlane::new();
        Self {
            versioned_storage,
            committer,
            mvcc_control_plane,
            commit_mutex: Mutex::new(()),
        }
    }

    pub fn acquire_ts(&self) -> u64 {
        self.mvcc_control_plane.acquire_ts()
    }

    pub fn read_key(&self, ts: u64, key: &str) -> Option<(u64, String)> {
        /* [Not with accordance to OCC, MVCC]
        - readers now block writers, which shouldn't happen in MVCC,
        - this happens cause of the RW Lock
        - but with AtomicPtr + Acquire,Release barriers, this shouldn't happen
        - owned (ts, value) is returned instead of references, because
          references cannot outlive the RwLock read guard
        */
        let storage_snapshot = self.versioned_storage.read().unwrap();
        storage_snapshot
            .get_at_or_before_ts(key, ts)
            .map(|(stored_ts, value)| (*stored_ts, value.clone()))
    }

    pub fn commit(
        &mut self,
        read_set: HashMap<String, (u64, String)>,
        write_set: HashMap<String, (String, u64)>,
    ) -> OccCommitResult {
        // Global commit point: only one writer validates + applies + publishes at a time,
        // so validation can never observe a store that changed underneath it.
        // Only 1 Writer is proceeding at this point (according to OCC)
        let _commit_guard = self.commit_mutex.lock().unwrap();

        /*
        - Copying the whole storage heavy to do, especially during critical path
            * We need it to avoid modifying BTrees/Maps in Place while being accessed by readers causing races
            * Readers could see chains/nodes in the middle while being modified
        - A more optimized solution is to make the publication itself to the chain storage Atomic OR COW
            * this makes writes efficient, not hold lock for a while being accessed
            * publication will just be adjusting atomic header OR tail ptr to point at the newly created node
        */

        // 1. Snapshot the currently published store (cheap Arc clone under the read lock),
        //    then deep-copy it so validation/application happen off the published store.
        let published_snapshot = {
            let storage_guard = self.versioned_storage.read().unwrap();
            storage_guard.clone()
        };

        // [Not with accordance to OCC, MVCC]
        // Should be updated with atomic chain updates (kinda liked linked list ptr updates)
        // instead of a whole deep copy
        // this is to avoid Readers obersving corrupted VerionsedStore (being rebalanced / or chains updated)
        let mut local_copied_storage = (*published_snapshot).clone();

        // 2. Validate + apply against the private copy (OCC).
        //    The committer borrows the read set, so adapt the owned read set to references.
        let commit_result = self
            .committer
            .commit(&mut local_copied_storage, read_set, write_set);

        match commit_result {
            // [Not with accordance to OCC, MVCC]
            // Writer now blocks readers due to RW lock
            // to be in accordance with OCC, MVCC should be done with RW Lock
            // 3. Publish: atomically swap the new store in under the write lock.
            //    Readers holding the old Arc keep reading their consistent snapshot.
            OccCommitResult::Comitted => {
                let mut storage_guard = self.versioned_storage.write().unwrap();
                *storage_guard = Arc::new(local_copied_storage);
                OccCommitResult::Comitted
            }
            // Aborted: the private copy is dropped, published store stays untouched.
            _ => commit_result,
        }
    }
}
