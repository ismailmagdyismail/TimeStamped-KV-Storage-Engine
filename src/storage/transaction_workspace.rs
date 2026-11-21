use std::collections::HashMap;

use crate::storage::{
    mvcc_control_plane::PHANTOM_TS, occ_comitter::OccCommitResult, storage_engine::StorageEngine,
};

#[derive(Debug)]
pub struct TransactionWorkSpace<'a> {
    storage_engine: &'a StorageEngine,
    begin_ts: u64,
    // owned copies: the engine guards its store with an RwLock,
    // so reads return owned (ts_at_access, value) pairs instead of references
    read_set: HashMap<String, (u64, String)>,
    write_set: HashMap<String, (u64, String)>,
}

/*
    - For SI MVCC performs W/W validation only
    - For SSI MVCC perorms both W/W and R/W so we can combine both read_set, write_set into 1 accessed_set
    * BUT NOT sepeartion helps with accessing / reading you own reads, simplifies logic
*/

impl<'a> TransactionWorkSpace<'a> {
    pub fn new(storage_engine: &'a StorageEngine) -> Self {
        let begin_ts = storage_engine.acquire_ts();
        Self {
            begin_ts,
            read_set: HashMap::new(),
            write_set: HashMap::new(),
            storage_engine,
        }
    }

    pub fn read_key<'b>(&'b mut self, key: &str) -> Option<(&'b u64, &'b String)> {
        // A. If recent write (read your own write)
        // B. If already read / cached (avoid going to storage again)
        if self.write_set.contains_key(key) {
            let res = self.write_set.get(key).unwrap();
            return Some((&res.0, &res.1));
        } else if self.read_set.contains_key(key) {
            let res = self.read_set.get(key).unwrap();
            return Some((&res.0, &res.1));
        }
        let ts_value = self.storage_engine.read_key(self.begin_ts, key)?;
        self.read_set.insert(key.to_string(), ts_value);
        let res = self.read_set.get(key).unwrap();
        Some((&res.0, &res.1))
    }

    // buffer-writes
    // stamp them with same timestamp later on commit
    // if aborted, then no need to stamp them, buffer is just cleared
    pub fn write_key(&mut self, key: String, value: String) {
        let res = self.read_key(&key);
        let ts_at_access = if let Some((read_ts, _)) = res {
            *read_ts
        } else {
            /*
            - we force non existent keys with 0 Time-Stamp
            - to avoid some phantom problems of checking keys, then comitting
            to find out they have been inserted with ts_at_access 0
            - BUT THIS SHOULDN'T BE HERE
            - a Use case discussion more than anything
            - a transaction IFF interactive, reading non existent values is not a problem
            - it is simply not added to R/W sets, client can chose to abort if needed
            IDEALLY SHOULD BE IN OCC_VALIDATION/COMITTER
            */
            PHANTOM_TS
        };
        self.write_set.insert(key, (ts_at_access, value));
    }

    pub fn commit(self) -> (OccCommitResult, u64) {
        let status: (OccCommitResult, u64) =
            self.storage_engine.commit(self.read_set, self.write_set);
        status
    }

    pub fn abort(mut self) {
        self.write_set.clear();
        self.read_set.clear();
    }

    pub fn begin_timestamp(&self) -> u64 {
        return self.begin_ts;
    }
}

#[cfg(test)]
pub mod test {
    use std::{
        sync::{
            Arc,
            atomic::{self, AtomicU8},
        },
        thread,
    };

    use crate::storage::{
        occ_comitter::OccCommitResult, storage_engine::StorageEngine,
        test_utils::create_versioned_value, transaction_workspace::TransactionWorkSpace,
    };

    pub fn populate_store_with_multiple_versions(
        storage_engine: &StorageEngine,
        starting_record: u64,
        num_records: u64,
        num_versions: u64,
    ) {
        for version in 1..num_versions + 1 {
            let mut tx = TransactionWorkSpace::new(&storage_engine);
            for i in starting_record..starting_record + num_records {
                let key = i.to_string();
                let value = create_versioned_value(&i.to_string(), version);
                tx.write_key(key.clone(), value);
            }
            let res = tx.commit();
            assert_eq!(res.0, OccCommitResult::Comitted);
        }
    }

    #[test]
    pub fn test_non_existent_keys() {
        let storage_engine = StorageEngine::new();
        let mut tx = TransactionWorkSpace::new(&storage_engine);
        let kv = tx.read_key("1");
        assert_eq!(kv, None);
        let kv = tx.read_key("1");
        assert_eq!(kv, None);
        let kv = tx.read_key("2");
        assert_eq!(kv, None);
        let res = tx.commit();
        assert_eq!(res.0, OccCommitResult::Comitted);
    }

    #[test]
    pub fn test_read_path_prefers_buffered_writes_over_cached_and_stored_values() {
        let storage_engine = StorageEngine::new();
        let mut seed_tx = TransactionWorkSpace::new(&storage_engine);
        seed_tx.write_key("key".to_string(), "stored".to_string());
        assert_eq!(seed_tx.commit().0, OccCommitResult::Comitted);

        let mut tx = TransactionWorkSpace::new(&storage_engine);
        assert_eq!(
            tx.read_key("key").map(|(_, value)| value.as_str()),
            Some("stored")
        );
        assert_eq!(
            tx.read_set.get("key").map(|(_, value)| value.as_str()),
            Some("stored")
        );

        tx.write_key("key".to_string(), "buffered".to_string());

        assert_eq!(
            tx.read_key("key").map(|(_, value)| value.as_str()),
            Some("buffered")
        );
        assert_eq!(
            tx.read_set.get("key").map(|(_, value)| value.as_str()),
            Some("stored")
        );
    }

    #[test]
    pub fn test_read_path_uses_cached_read_before_storage() {
        let storage_engine = StorageEngine::new();
        let mut tx = TransactionWorkSpace::new(&storage_engine);
        tx.read_set
            .insert("key".to_string(), (42, "cached".to_string()));

        assert_eq!(
            tx.read_key("key")
                .map(|(timestamp, value)| (*timestamp, value.as_str())),
            Some((42, "cached"))
        );
    }

    #[test]
    pub fn test_read_path_loads_from_storage_and_caches_result() {
        let storage_engine = StorageEngine::new();
        let mut seed_tx = TransactionWorkSpace::new(&storage_engine);
        seed_tx.write_key("key".to_string(), "stored".to_string());
        assert_eq!(seed_tx.commit().0, OccCommitResult::Comitted);

        let mut tx = TransactionWorkSpace::new(&storage_engine);
        let stored_read = tx
            .read_key("key")
            .map(|(timestamp, value)| (*timestamp, value.clone()));

        assert_eq!(
            stored_read.as_ref().map(|(_, value)| value.as_str()),
            Some("stored")
        );
        assert_eq!(tx.read_set.get("key"), stored_read.as_ref());
    }

    #[test]
    pub fn test_read_only() {
        let storage_engine = StorageEngine::new();
        let num_records = 100;
        let num_versions = 10;
        let starting_record = 1;
        {
            populate_store_with_multiple_versions(
                &storage_engine,
                starting_record,
                num_records,
                num_versions,
            );
        }
        let mut tx = TransactionWorkSpace::new(&storage_engine);
        for i in starting_record..num_records + 1 {
            let res = tx.read_key(&i.to_string());
            assert_ne!(res, None);
        }
        let res = tx.commit();
        assert_eq!(res.0, OccCommitResult::Comitted);
    }

    #[test]
    pub fn test_blind_writes() {
        let storage_engine = StorageEngine::new();
        populate_store_with_multiple_versions(&storage_engine, 1, 100, 100);
    }

    #[test]
    pub fn test_no_conflicting_writes_no_read_intersection() {
        let storage_engine = StorageEngine::new();
        let num_records = 10;
        let num_versions = 10;
        let starting_record = 1;

        {
            populate_store_with_multiple_versions(
                &storage_engine,
                starting_record,
                num_records,
                num_versions,
            );
        }

        let mut tx1 = TransactionWorkSpace::new(&storage_engine);
        let mut tx2 = TransactionWorkSpace::new(&storage_engine);

        tx1.read_key("1");
        tx1.read_key("2");
        tx1.write_key("1".to_string(), "updated_1".to_string());
        tx1.write_key("2".to_string(), "updared_2".to_string());

        tx2.read_key("3");
        tx2.read_key("4");
        tx2.write_key("3".to_string(), "updated_3".to_string());
        tx2.write_key("4".to_string(), "updared_4".to_string());

        assert_eq!(tx1.commit().0, OccCommitResult::Comitted);
        assert_eq!(tx2.commit().0, OccCommitResult::Comitted);
    }

    #[test]
    pub fn test_no_conflicting_writes_with_read_intersection() {
        let storage_engine = StorageEngine::new();
        let num_records = 10;
        let num_versions = 10;
        let starting_record = 1;

        {
            populate_store_with_multiple_versions(
                &storage_engine,
                starting_record,
                num_records,
                num_versions,
            );
        }

        let mut tx1 = TransactionWorkSpace::new(&storage_engine);
        let mut tx2 = TransactionWorkSpace::new(&storage_engine);

        tx1.read_key("1");
        tx1.read_key("2");
        tx1.write_key("3".to_string(), "updated_1".to_string());
        tx1.write_key("4".to_string(), "updared_2".to_string());

        tx2.read_key("1");
        tx2.read_key("2");
        tx2.write_key("5".to_string(), "updated_3".to_string());
        tx2.write_key("6".to_string(), "updared_4".to_string());

        assert_eq!(tx1.commit().0, OccCommitResult::Comitted);
        assert_eq!(tx2.commit().0, OccCommitResult::Comitted);
    }

    #[test]
    pub fn test_stale_snapshots_isolation() {
        let storage_engine = StorageEngine::new();
        let num_records = 10;
        let num_versions = 10;
        let starting_record = 1;

        {
            populate_store_with_multiple_versions(
                &storage_engine,
                starting_record,
                num_records,
                num_versions,
            );
        }

        // read tx started early with earlier timestamp that write transactions
        // should read a consistent snapshot of old state of the db
        // shouldn't read / access new versions written at later timestamps
        let mut read_tx = TransactionWorkSpace::new(&storage_engine);
        let begin_timestamp = read_tx.begin_timestamp();

        let new_write_value = "new_write_value";

        for i in 1..num_records + 1 {
            let mut write_tx = TransactionWorkSpace::new(&storage_engine);
            write_tx.write_key(i.to_string(), new_write_value.to_string());
            let res = write_tx.commit();
            assert_eq!(res.0, OccCommitResult::Comitted);
        }

        for i in 1..num_records + 1 {
            let res = read_tx.read_key(&i.to_string()).unwrap();
            assert!(*res.0 <= begin_timestamp);
            assert!(res.1 != new_write_value);
        }
    }

    #[test]
    pub fn test_phantom_reads() {
        let storage_engine = StorageEngine::new();
        let num_records = 10;
        let num_versions = 10;
        let starting_record = 1;

        {
            populate_store_with_multiple_versions(
                &storage_engine,
                starting_record,
                num_records,
                num_versions,
            );
        }

        // readers timestamp is older than any values that are to be inserted in the future
        // thus any non existing values at that point should not be observable
        let mut read_tx = TransactionWorkSpace::new(&storage_engine);
        let begin_timestamp = read_tx.begin_timestamp();

        let new_write_value = "new_write_value";

        // insert new records not just modify existing ones
        let existing_records_end = num_records + 1;
        let new_records_end = existing_records_end + 100; // extra records, not existing in original snapshot
        for i in 1..new_records_end {
            let mut write_tx = TransactionWorkSpace::new(&storage_engine);
            write_tx.write_key(i.to_string(), new_write_value.to_string());
            let res = write_tx.commit();
            assert_eq!(res.0, OccCommitResult::Comitted);
        }

        for i in 1..existing_records_end {
            let res = read_tx.read_key(&i.to_string()).unwrap();
            assert!(*res.0 <= begin_timestamp);
            assert!(res.1 != new_write_value);
        }

        let new_records_start = existing_records_end;
        for i in new_records_start..existing_records_end {
            let res = read_tx.read_key(&i.to_string());
            assert_eq!(res, None);
        }
    }

    #[test]
    pub fn test_write_skew() {
        let storage_engine = StorageEngine::new();
        let num_records = 10;
        let num_versions = 10;
        let starting_record = 1;

        {
            populate_store_with_multiple_versions(
                &storage_engine,
                starting_record,
                num_records,
                num_versions,
            );
        }

        let mut tx1 = TransactionWorkSpace::new(&storage_engine);
        let mut tx2 = TransactionWorkSpace::new(&storage_engine);

        tx1.read_key("1");
        tx1.read_key("2");

        tx2.read_key("1");
        tx2.read_key("2");

        tx1.write_key("1".to_string(), "new".to_string());
        tx2.write_key("2".to_string(), "new".to_string());

        let res = tx1.commit();
        assert_eq!(res.0, OccCommitResult::Comitted);
        let res = tx2.commit();
        assert_ne!(res.0, OccCommitResult::Comitted);
    }

    #[test]
    pub fn test_concurrent_rw_no_conflicts() {
        let storage_engine = Arc::new(StorageEngine::new());
        let num_records = 10;
        let num_versions = 10;
        let starting_record = 1;

        {
            populate_store_with_multiple_versions(
                &storage_engine,
                starting_record,
                num_records,
                num_versions,
            );
        }

        let mut threads = Vec::new();
        for i in 0..4 {
            let clone = Arc::clone(&storage_engine);
            let handle = thread::spawn(move || {
                let mut tx = TransactionWorkSpace::new(&clone);
                tx.read_key(&i.to_string());
                tx.write_key(i.to_string(), "new".to_string());
                let res = tx.commit();
                assert_eq!(res.0, OccCommitResult::Comitted);
            });
            threads.push(handle);
        }
        for thread in threads {
            thread.join().unwrap();
        }
    }

    #[test]
    pub fn test_write_phantoms() {
        let storage_engine = Arc::new(StorageEngine::new());
        let mut tx1 = TransactionWorkSpace::new(&storage_engine);
        let mut tx2 = TransactionWorkSpace::new(&storage_engine);
        tx1.write_key("key".to_string(), "value".to_string());
        tx2.write_key("key".to_string(), "value".to_string());

        let (res, _) = tx1.commit();
        assert_eq!(res, OccCommitResult::Comitted);
        let (res, _) = tx2.commit();
        assert_ne!(res, OccCommitResult::Comitted);
    }
}
