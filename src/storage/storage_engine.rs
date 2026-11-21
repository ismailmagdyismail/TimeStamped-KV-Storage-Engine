use arc_swap::ArcSwap;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
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

#[derive(Debug)]
pub struct StorageEngine {
    mvcc_control_plane: MVCCControlPlane,
    /*
    - [Not with accordance to OCC, MVCC]
    - RW lock here is not necessary
    - it could be done with Atomic Ptr, Acqurie+Release barriers to coordinate between readers, writer
    - kept temporarily: readers take the read lock and clone the Arc snapshot,
      writers publish a brand new Arc under the write lock (COW, never mutate in place)
    */
    versioned_storage: ArcSwap<VersionedStore>, // lock-free reads, atomic publish

    // Serializes commit validation + application + publication.
    committer: Mutex<OccCommitter>,
}

impl StorageEngine {
    pub fn new() -> Self {
        let committer = OccCommitter::new();
        let versioned_storage = ArcSwap::from_pointee(VersionedStore::new());
        let mvcc_control_plane = MVCCControlPlane::new();
        Self {
            versioned_storage,
            committer: Mutex::new(committer),
            mvcc_control_plane,
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
        let snapshot = self.versioned_storage.load();
        snapshot
            .get_at_or_before_ts(key, ts)
            .map(|(stored_ts, v)| (*stored_ts, v.clone()))
    }

    pub fn commit(
        &self,
        read_set: HashMap<String, (u64, String)>,
        write_set: HashMap<String, (String, u64)>,
    ) -> OccCommitResult {
        // Global commit point: only one writer validates + applies + publishes at a time,
        // so validation can never observe a store that changed underneath it.
        // Only 1 Writer is proceeding at this point (according to OCC)
        let mut committer = self.committer.lock().unwrap();

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

        // [Not with accordance to OCC, MVCC]
        // Should be updated with atomic chain updates (kinda liked linked list ptr updates)
        // instead of a whole deep copy
        // this is to avoid Readers obersving corrupted VerionsedStore (being rebalanced / or chains updated)
        let mut local_copied_storage = (**self.versioned_storage.load()).clone();

        // 2. Validate + apply against the private copy (OCC).
        //    The committer borrows the read set, so adapt the owned read set to references.
        let commit_result = committer.commit(&mut local_copied_storage, read_set, write_set);

        match commit_result {
            // [Not with accordance to OCC, MVCC]
            // Writer now blocks readers due to RW lock
            // to be in accordance with OCC, MVCC should be done with RW Lock
            // 3. Publish: atomically swap the new store in under the write lock.
            //    Readers holding the old Arc keep reading their consistent snapshot.
            OccCommitResult::Comitted => {
                self.versioned_storage.store(Arc::new(local_copied_storage));
                OccCommitResult::Comitted
            }
            // Aborted: the private copy is dropped, published store stays untouched.
            _ => commit_result,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread,
    };

    use crate::storage::{
        occ_comitter::{ConflictType, OccCommitResult},
        storage_engine::StorageEngine,
    };

    fn single_write(key: &str, value: &str, ts: u64) -> HashMap<String, (String, u64)> {
        let mut write_set = HashMap::new();
        write_set.insert(key.to_string(), (value.to_string(), ts));
        write_set
    }

    #[test]
    fn test_read_missing_key_returns_none() {
        let engine = StorageEngine::new();
        assert_eq!(engine.read_key(10, "missing"), None);
    }

    #[test]
    fn test_commit_publishes_version() {
        let engine = StorageEngine::new();
        let commit_ts = engine.acquire_ts();

        let result: OccCommitResult =
            engine.commit(HashMap::new(), single_write("k", "v1", commit_ts));
        assert_eq!(result, OccCommitResult::Comitted);

        // visible at and after the commit ts
        assert_eq!(
            engine.read_key(commit_ts, "k"),
            Some((commit_ts, "v1".to_string()))
        );
        assert_eq!(
            engine.read_key(u64::MAX, "k"),
            Some((commit_ts, "v1".to_string()))
        );
        // not visible before it
        assert_eq!(engine.read_key(commit_ts - 1, "k"), None);
    }

    #[test]
    fn test_snapshot_isolation_keeps_older_versions() {
        let engine = StorageEngine::new();

        let ts1 = engine.acquire_ts();
        engine.commit(HashMap::new(), single_write("k", "v1", ts1));

        let ts2 = engine.acquire_ts();
        engine.commit(HashMap::new(), single_write("k", "v2", ts2));

        // earlier snapshots still see the old version (COW never rewrites history)
        assert_eq!(engine.read_key(ts1, "k"), Some((ts1, "v1".to_string())));
        assert_eq!(engine.read_key(ts2, "k"), Some((ts2, "v2".to_string())));
        assert_eq!(
            engine.read_key(u64::MAX, "k"),
            Some((ts2, "v2".to_string()))
        );
    }

    #[test]
    fn test_aborted_commit_leaves_store_unchanged() {
        let engine = StorageEngine::new();

        let ts1 = engine.acquire_ts();
        engine.commit(HashMap::new(), single_write("k", "v1", ts1));

        // W/W conflict: commit at an older ts than what is already stored
        let result = engine.commit(HashMap::new(), single_write("k", "stale", ts1 - 1));
        match result {
            OccCommitResult::Aborted(status) => {
                assert_eq!(status.confilict_type, ConflictType::WW);
                assert_eq!(status.key, "k");
            }
            OccCommitResult::Comitted => panic!("expected abort on W/W conflict"),
        }

        // the failed commit must not have leaked any partial state
        assert_eq!(
            engine.read_key(u64::MAX, "k"),
            Some((ts1, "v1".to_string()))
        );
    }

    #[test]
    fn test_rww_read_modify_write_conflict() {
        let engine = StorageEngine::new();

        let ts1 = engine.acquire_ts();
        engine.commit(HashMap::new(), single_write("k", "v1", ts1));

        // reader observed v1 at ts1, then another txn committed v2 at ts2
        let read_ts = engine.acquire_ts();
        let ts2 = engine.acquire_ts();
        engine.commit(HashMap::new(), single_write("k", "v2", ts2));

        // the stale reader now tries to commit a write -> must abort (R/W conflict)
        let mut read_set = HashMap::new();
        read_set.insert("k".to_string(), (read_ts, "v1".to_string()));
        let result = engine.commit(read_set, single_write("k", "v1+", ts2 + 1));
        match result {
            OccCommitResult::Aborted(status) => assert_eq!(status.confilict_type, ConflictType::RW),
            OccCommitResult::Comitted => panic!("expected abort on R/W conflict"),
        }

        assert_eq!(
            engine.read_key(u64::MAX, "k"),
            Some((ts2, "v2".to_string()))
        );
    }

    #[test]
    fn test_concurrent_commits_no_conflicts() {
        let engine = Arc::new(StorageEngine::new());
        const THREADS: usize = 8;
        const ITERS: usize = 100;

        {
            let ts = engine.acquire_ts();
            let result = engine.commit(HashMap::new(), single_write("counter", "0", ts));
            assert_eq!(result, OccCommitResult::Comitted);
        }

        let mut handles = Vec::new();
        for id in 0..THREADS {
            let engine_clone = Arc::clone(&engine);
            handles.push(thread::spawn(move || {
                for i in 0..ITERS {
                    let unique_key =
                        format!("counter_threadid_iteration_id_{}_{:?}", id, i.to_string());
                    let result = engine_clone.commit(
                        HashMap::new(),
                        single_write(&unique_key, &i.to_string(), engine_clone.acquire_ts()),
                    );
                    assert_eq!(result, OccCommitResult::Comitted);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_concurrent_readers_never_see_versions_from_the_future() {
        let engine = Arc::new(StorageEngine::new());
        {
            let ts = engine.acquire_ts();
            engine.commit(HashMap::new(), single_write("hot", "seed", ts));
        }

        let stop = Arc::new(AtomicBool::new(false));
        let mut readers = Vec::new();
        for _ in 0..4 {
            let stop = Arc::clone(&stop);
            let reader_engine_ref = Arc::clone(&engine);
            readers.push(thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let r_ts = reader_engine_ref.acquire_ts();
                    if let Some((stored_ts, _)) = reader_engine_ref.read_key(r_ts, "hot") {
                        assert!(
                            stored_ts <= r_ts,
                            "reader at ts {r_ts} saw version from ts {stored_ts}"
                        );
                    }
                }
            }));
        }

        // writer: publish many new versions while readers are scanning
        {
            for i in 0..200 {
                let ts = engine.acquire_ts();
                let result =
                    engine.commit(HashMap::new(), single_write("hot", &format!("w{i}"), ts));
                assert_eq!(result, OccCommitResult::Comitted);
            }
        }

        stop.store(true, Ordering::Relaxed);
        for r in readers {
            r.join().unwrap();
        }
    }
}
