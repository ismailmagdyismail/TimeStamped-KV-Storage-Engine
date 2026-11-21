use std::collections::HashMap;

use crate::storage::{occ_comitter::OccCommitResult, storage_engine::StorageEngine};

#[derive(Debug)]
pub struct TransactionWorkSpace<'a> {
    storage_engine: &'a StorageEngine,
    begin_ts: u64,
    // owned copies: the engine guards its store with an RwLock,
    // so reads return owned (ts, value) pairs instead of references
    read_set: HashMap<String, (u64, String)>,
    buffered_writes: HashMap<String, String>,
    write_set: HashMap<String, (String, u64)>,
}

impl<'a> TransactionWorkSpace<'a> {
    pub fn new(storage_engine: &'a StorageEngine) -> Self {
        let begin_ts = storage_engine.acquire_ts();
        Self {
            begin_ts,
            read_set: HashMap::new(),
            write_set: HashMap::new(),
            buffered_writes: HashMap::new(),
            storage_engine,
        }
    }

    /*
    - we force non existent keys with 0 Time-Stamp
    - to avoid some phantom problems of checking keys, then comitting
    to find out they have been inserted with ts 0
    - BUT THIS SHOULDN'T BE HERE
    - a Use case discussion more than anything
    - a transaction IFF interactive, reading non existent values is not a problem
    - it is simply not added to R/W sets, client can chose to abort if needed
    */
    pub fn read_key<'b>(&'b mut self, key: &str) -> Option<(&'b u64, &'b String)> {
        // A. If recent write (read your own write)
        // B. If already read / cached (avoid going to storage again)
        if self.write_set.contains_key(key) {
            let res = self.write_set.get(key).unwrap();
            return Some((&res.1, &res.0));
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
        self.buffered_writes.insert(key, value);
    }

    pub fn commit(mut self) -> OccCommitResult {
        let commit_ts = self.storage_engine.acquire_ts();
        for (key, val) in self.buffered_writes.into_iter() {
            self.write_set.insert(key, (val, commit_ts));
        }
        self.storage_engine.commit(self.read_set, self.write_set)
    }

    pub fn abort(mut self) {
        self.buffered_writes.clear();
        self.read_set.clear();
    }

    pub fn begin_timestamp(&self) -> u64 {
        return self.begin_ts;
    }
}

#[cfg(test)]
pub mod test {
    use std::{sync::Arc, thread};

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
            assert_eq!(res, OccCommitResult::Comitted);
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
        assert_eq!(res, OccCommitResult::Comitted);
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
        assert_eq!(res, OccCommitResult::Comitted);
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

        assert_eq!(tx1.commit(), OccCommitResult::Comitted);
        assert_eq!(tx2.commit(), OccCommitResult::Comitted);
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

        assert_eq!(tx1.commit(), OccCommitResult::Comitted);
        assert_eq!(tx2.commit(), OccCommitResult::Comitted);
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
            assert_eq!(res, OccCommitResult::Comitted);
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
            assert_eq!(res, OccCommitResult::Comitted);
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
        assert_eq!(res, OccCommitResult::Comitted);
        let res = tx2.commit();
        assert_ne!(res, OccCommitResult::Comitted);
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
                assert_eq!(res, OccCommitResult::Comitted);
            });
            threads.push(handle);
        }
        for thread in threads {
            thread.join().unwrap();
        }
    }
}
