use std::collections::HashMap;

use crate::storage::versioned_store::VersionedStore;

#[derive(Debug, PartialEq, Eq)]
pub enum ConflictType {
    RW,
    WW,
}

#[derive(Debug, PartialEq, Eq)]
pub struct AbortStatus {
    pub confilict_type: ConflictType,
    pub key: String,
    pub value: String,
    pub stored_ts: u64,
    pub ts_to_commit: u64,
}
#[derive(Debug, PartialEq, Eq)]
pub enum OccCommitResult {
    Comitted,
    Aborted(AbortStatus),
}

#[derive(Debug)]
pub struct OccCommitter;

impl OccCommitter {
    pub fn new() -> Self {
        Self {}
    }

    pub fn commit<'a>(
        &mut self,
        versioned_storage: &mut VersionedStore,
        read_set: HashMap<String, (u64, String)>,
        write_set: HashMap<String, (u64, String)>,
        commit_ts: u64,
    ) -> OccCommitResult {
        // Write Skew Handling -- Detect R/W conflicts, not just W/W conflicts
        // EX-1:
        // * Tx_A
        //      - Reads A
        //      - Reads B
        //      - If A+B = 2 => Set A to 0
        // * Tx_B
        //      - Reads A
        //      - Reads B
        //      - If A+B = 2 => Set B to 0

        // if transaction is read only
        // it can commit even if other transactions has already committed
        // since it is a snapshot only read (no modifications)
        // as we were reading db at time TN
        let tx_is_read_only = write_set.is_empty();
        if tx_is_read_only {
            return OccCommitResult::Comitted;
        }

        // W/W Validation
        // validating that Keys Written (Updated or added) haven't been changed since accessed
        // updated when observed at time TN, cannot have stored version TX, where X > N
        // in order to not violate assumption of writes (mostly blind writes)
        // This acheives SI
        for (key, (ts, val)) in &write_set {
            if let Some(stored_ts) = versioned_storage.get_max_timestamp_for_record(&key) {
                if *stored_ts > *ts {
                    return OccCommitResult::Aborted(AbortStatus {
                        confilict_type: ConflictType::WW,
                        key: key.clone(),
                        value: val.clone(),
                        stored_ts: *stored_ts,
                        ts_to_commit: *ts,
                    });
                }
            }
            // Key doesn't exist, so we are guranteed to have a Ts higher than it
            // else {
            // }
        }

        // R/W validation
        // validating that Keys Read haven't been changed since accessed
        // Read&updated when observed at time TN, cannot have stored version TX, where X > N
        // in order to not violate assumption of (Writes based on Reads assumptions)
        // This acheives SSI
        for (key, (ts, val)) in read_set {
            if let Some(stored_ts) = versioned_storage.get_max_timestamp_for_record(&key) {
                if *stored_ts > ts {
                    return OccCommitResult::Aborted(AbortStatus {
                        confilict_type: ConflictType::RW,
                        key: key.clone(),
                        value: val.to_string(),
                        stored_ts: *stored_ts,
                        ts_to_commit: ts,
                    });
                }
            }
            // key doesn't exist, shouldn't happen
            else {
                return OccCommitResult::Aborted(AbortStatus {
                    confilict_type: ConflictType::RW,
                    key: key.clone(),
                    value: val.clone(),
                    stored_ts: 0,
                    ts_to_commit: ts,
                });
            }
        }
        for (key, (_, value)) in write_set {
            versioned_storage.put(key, value, commit_ts);
        }
        OccCommitResult::Comitted
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::storage::{
        occ_comitter::{OccCommitResult, OccCommitter},
        test_utils::populate_store_with_multiple_versions,
        versioned_store::VersionedStore,
    };

    #[test]
    pub fn test_non_existing_key() {
        let mut versioned_storage = VersionedStore::new();
        let mut occ_comitter = OccCommitter::new();

        let key = "1".to_string();
        let val = "1".to_string();
        let ts = 1;

        // reads
        let mut read_set = HashMap::new();
        read_set.insert(key, (ts, val));

        let write_set = HashMap::new();

        let res = occ_comitter.commit(&mut versioned_storage, read_set, write_set, ts);

        assert_eq!(res, OccCommitResult::Comitted);
    }

    #[test]
    pub fn test_insert() {
        let mut versioned_storage = VersionedStore::new();
        let mut occ_comitter = OccCommitter::new();

        let key = "1".to_string();
        let val = "1".to_string();
        let snapshot_ts = 1;

        // reads
        let read_set = HashMap::new();

        // updates
        let mut write_set = HashMap::new();
        write_set.insert(key, (snapshot_ts, val));

        let res = occ_comitter.commit(
            &mut versioned_storage,
            read_set,
            write_set,
            snapshot_ts + 10,
        );

        assert_eq!(res, OccCommitResult::Comitted);
    }

    #[test]
    // store have values up to time TN
    // but reads / snapshot is reading from time TX, where X < N
    pub fn test_snapshot_reads() {
        let mut versioned_storage = VersionedStore::new();
        populate_store_with_multiple_versions(&mut versioned_storage, 1, 10, 10);
        let mut occ_comitter = OccCommitter::new();

        let key = "1".to_string();
        let val = "1".to_string();
        let snapshot_ts = 1;

        let mut read_set = HashMap::new();
        read_set.insert(key, (snapshot_ts, val));

        let write_set = HashMap::new();

        let commit_result = occ_comitter.commit(
            &mut versioned_storage,
            read_set,
            write_set,
            snapshot_ts + 100,
        );
        assert_eq!(commit_result, OccCommitResult::Comitted);
    }

    #[test]
    pub fn test_write_skew() {
        let mut versioned_storage = VersionedStore::new();
        let snapshot_ts = 10;
        populate_store_with_multiple_versions(&mut versioned_storage, 1, 10, snapshot_ts);
        let mut occ_comitter = OccCommitter::new();

        // tx-1
        let tx_1_snapshot = snapshot_ts + 1;
        let tx_2_snapshot = snapshot_ts + 2;
        let tx_1_commit_ts = snapshot_ts + 3;
        let tx_2_commit_ts = snapshot_ts + 4;

        let key1_t1 = "1".to_string();
        let val1_t1 = "1".to_string();

        let key2_t1 = "2".to_string();
        let val2_t1 = "2".to_string();
        {
            let mut read_set = HashMap::new();
            read_set.insert(key1_t1.clone(), (tx_1_snapshot, val1_t1));
            read_set.insert(key2_t1.clone(), (tx_1_snapshot, val2_t1));

            let mut write_set = HashMap::new();
            write_set.insert(key1_t1, (tx_1_snapshot, "0".to_string()));

            let commit_result =
                occ_comitter.commit(&mut versioned_storage, read_set, write_set, tx_1_commit_ts);

            assert_eq!(commit_result, OccCommitResult::Comitted);
        }

        {
            let key1 = "1".to_string();
            let val1 = "1".to_string();

            let key2 = "2".to_string();
            let val2 = "2".to_string();

            let mut read_set = HashMap::new();
            read_set.insert(key1.clone(), (tx_2_snapshot, val1));
            read_set.insert(key2.clone(), (tx_2_snapshot, val2));

            let mut write_set = HashMap::new();
            write_set.insert(key2, (tx_2_snapshot, "0".to_string()));

            let commit_result =
                occ_comitter.commit(&mut versioned_storage, read_set, write_set, tx_2_commit_ts);

            assert_ne!(commit_result, OccCommitResult::Comitted);
        }
    }
}
