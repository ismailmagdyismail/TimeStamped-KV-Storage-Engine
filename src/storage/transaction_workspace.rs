use std::collections::HashMap;

use crate::storage::{occ_comitter::OccCommitResult, storage_engine::StorageEngine};

pub struct TransactionWorkSpace<'a> {
    storage_engine: &'a mut StorageEngine,
    begin_ts: u64,
    // owned copies: the engine guards its store with an RwLock,
    // so reads return owned (ts, value) pairs instead of references
    read_set: HashMap<String, (u64, String)>,
    buffered_writes: HashMap<String, String>,
    write_set: HashMap<String, (String, u64)>,
}

impl<'a> TransactionWorkSpace<'a> {
    pub fn new(storage_engine: &'a mut StorageEngine) -> Self {
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
    pub fn read_key<'b>(&'b mut self, key: &str) -> Option<(&'b u64, &'b String)>
    where
        'b: 'a,
    {
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
}

#[cfg(test)]
pub mod test {
    #[test]
    pub fn test_no_conflicts() {
    }
}
