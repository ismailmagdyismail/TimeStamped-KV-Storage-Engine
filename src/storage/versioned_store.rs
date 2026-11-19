use std::collections::{BTreeMap, HashMap};

pub struct VersionedStore {
    store: HashMap<String, BTreeMap<u64, String>>,
}

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

    pub fn get(&self, key: &str, timestamp: u64) -> Option<&String> {
        let versions = self.store.get(key)?;
        versions.get(&timestamp)
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
    use crate::storage::versioned_store::VersionedStore;

    fn populate_store(store: &mut VersionedStore, num_records: u64) {
        for i in 1..num_records + 1 {
            let key = i.to_string();
            let value = i.to_string();
            let timestamp = 1;
            store.put(key, value, timestamp);
        }
    }

    fn create_versioned_value(val_base_name: &str, version: u64) -> String {
        format!("{}_v{}", val_base_name, version)
    }

    fn populate_store_with_multiple_versions(
        store: &mut VersionedStore,
        starting_record: u64,
        num_records: u64,
        num_versions: u64,
    ) {
        for i in starting_record..starting_record + num_records {
            let key = i.to_string();
            for j in 1..num_versions + 1 {
                let value = create_versioned_value(&i.to_string(), j);
                let timestamp = j;
                store.put(key.clone(), value, timestamp);
            }
        }
    }

    fn validate_store(store: &VersionedStore, expected_max_timestamp: u64, num_records: u64) {
        for i in 1..num_records + 1 {
            let key = i.to_string();
            let value = i.to_string();

            let val = store.get(&key, expected_max_timestamp);
            assert_eq!(val, Some(&value));

            let max_ts = store.get_max_timestamp_for_record(&key);
            assert_eq!(max_ts, Some(&expected_max_timestamp));

            let latests_kv_pair = store.get_latest(&key);
            assert_eq!(latests_kv_pair, Some((&expected_max_timestamp, &value)));
        }
    }

    fn validate_store_with_multiple_versions(
        store: &VersionedStore,
        expected_max_timestamp: u64,
        starting_record: u64,
        num_records: u64,
    ) {
        for i in starting_record..starting_record + num_records {
            let key = i.to_string();
            let value = create_versioned_value(&i.to_string(), expected_max_timestamp);

            let val = store.get(&key, expected_max_timestamp);
            assert_eq!(val, Some(&value));

            let max_ts = store.get_max_timestamp_for_record(&key);
            assert_eq!(max_ts, Some(&expected_max_timestamp));

            let latests_kv_pair = store.get_latest(&key);
            assert_eq!(latests_kv_pair, Some((&expected_max_timestamp, &value)));
        }
    }

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
        populate_store_with_multiple_versions(&mut store, 101, num_records, num_versions_for_second_batch);
        validate_store_with_multiple_versions(&store, num_versions_for_second_batch, 101, num_records);

    }
}
