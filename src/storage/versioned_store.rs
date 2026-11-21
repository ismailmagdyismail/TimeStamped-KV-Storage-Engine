use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone)]
pub struct VersionedStore {
    store: HashMap<String, BTreeMap<u64, String>>,
}

/*
    Ideally to avoid
        - Locking
        - Deep Copy + COW
        - we granulize the COW to be per key, per verision, instead of per VersionedStore
            to avoid the huge INCREASING cost of deep copy as store grows in size
            keeping COW | CAS | Shadow Copying at Chain Node Level Keeps cost constant regardless of store size

            -----> Key_A-Chain  => [ (ts_1, Value, atomic_next) , (ts_2, Value, atomic_next) , (ts_3, Value, atomic_next) ]
          |
          |
    Index |----> Key_B-Chain => [ (ts_1, Value, atomic_next) , (ts_2, Value, atomic_next) ]
    (COW) |
          |
          |----> Key_C-Chain => [ (ts_1, Value, atomic_next) , (ts_2, Value, atomic_next) ]

*/

impl VersionedStore {
    pub fn new() -> Self {
        VersionedStore {
            store: HashMap::new(),
        }
    }

    pub fn put(&mut self, key: String, value: String, timestamp: u64) {
        let versions = self.store.entry(key).or_insert(BTreeMap::new());
        versions.insert(timestamp, value);
    }

    pub fn get_max_timestamp_for_record(&self, key: &str) -> Option<&u64> {
        let max_ts = self.store.get(key)?.keys().max();
        max_ts
    }

    pub fn get_at_or_before_ts(&self, key: &str, timestamp: u64) -> Option<(&u64, &String)> {
        let versions = self.store.get(key)?;
        let lower_bound_match = versions.range(..=timestamp).next_back();
        lower_bound_match
    }

    pub fn get_latest(&self, key: &str) -> Option<(&u64, &String)> {
        let versions = self.store.get(key)?;
        let max_ts = versions.keys().max()?;
        let value = versions.get(max_ts)?;
        Some((max_ts, value))
    }

    pub fn snapshot_at(&self, timestamp: u64) -> HashMap<&String, &String> {
        let mut snapshot = HashMap::new();
        for (key, versions) in &self.store {
            if let Some(value) = versions.get(&timestamp) {
                snapshot.insert(key, value);
            }
        }
        snapshot
    }
}

#[cfg(test)]

mod tests {
    use crate::storage::{
        test_utils::{
            create_versioned_value, populate_store, populate_store_with_multiple_versions,
            validate_store, validate_store_with_multiple_versions,
        },
        versioned_store::VersionedStore,
    };

    #[test]
    pub fn test_read_ts() {
        let mut store = VersionedStore::new();
        let count = 5;
        let max_expected_timestamp = 1;
        populate_store(&mut store, count);
        validate_store(&store, max_expected_timestamp, count);
        let key = "1".to_string();
        let value = "1".to_string();
        let timestamp = 1;
        store.put(key.clone(), value.clone(), timestamp);
    }

    #[test]
    pub fn test_snapshot() {
        let mut store = VersionedStore::new();
        let count = 10;
        populate_store(&mut store, count);
        let snapshot = store.snapshot_at(1);
        assert_eq!(snapshot.len(), count as usize);

        for i in 1..count + 1 {
            let key = i.to_string();
            let value = i.to_string();
            assert_eq!(snapshot.get(&key), Some(&&value));
        }
    }

    #[test]
    pub fn test_reading_multi_versioned_records() {
        let mut store = VersionedStore::new();
        let num_records = 100;
        let num_versions = 20;
        populate_store_with_multiple_versions(&mut store, 1, num_records, num_versions);

        validate_store_with_multiple_versions(&store, num_versions, 1, num_records);

        let snapshot = store.snapshot_at(20);
        assert_eq!(snapshot.len(), num_records as usize);

        for i in 1..num_records + 1 {
            let key = i.to_string();
            let value = create_versioned_value(&i.to_string(), 20);
            assert_eq!(snapshot.get(&key), Some(&&value));
        }
    }

    #[test]
    pub fn test_sparse_timestamps() {
        let mut store = VersionedStore::new();
        let num_records = 100;
        let num_versions = 20;

        populate_store_with_multiple_versions(&mut store, 1, num_records, num_versions);
        validate_store_with_multiple_versions(&store, num_versions, 1, num_records);

        let num_versions_for_second_batch = 10;
        populate_store_with_multiple_versions(
            &mut store,
            101,
            num_records,
            num_versions_for_second_batch,
        );
        validate_store_with_multiple_versions(
            &store,
            num_versions_for_second_batch,
            101,
            num_records,
        );
    }
}
