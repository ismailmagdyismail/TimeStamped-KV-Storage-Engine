use crate::storage::versioned_store::VersionedStore;

pub fn populate_store(store: &mut VersionedStore, num_records: u64) {
    for i in 1..num_records + 1 {
        let key = i.to_string();
        let value = i.to_string();
        let timestamp = 1;
        store.put(key, value, timestamp);
    }
}

pub fn create_versioned_value(val_base_name: &str, version: u64) -> String {
    format!("{}_v{}", val_base_name, version)
}

pub fn populate_store_with_multiple_versions(
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

pub fn validate_store(store: &VersionedStore, expected_max_timestamp: u64, num_records: u64) {
    for i in 1..num_records + 1 {
        let key = i.to_string();
        let value = i.to_string();

        let val = store.get_at_or_before_ts(&key, expected_max_timestamp);
        assert!(val.is_some());
        assert_eq!(val.unwrap().1, &value);

        let max_ts = store.get_max_timestamp_for_record(&key);
        assert_eq!(max_ts, Some(&expected_max_timestamp));

        let latests_kv_pair = store.get_latest(&key);
        assert_eq!(latests_kv_pair, Some((&expected_max_timestamp, &value)));
    }
}

pub fn validate_store_with_multiple_versions(
    store: &VersionedStore,
    expected_max_timestamp: u64,
    starting_record: u64,
    num_records: u64,
) {
    for i in starting_record..starting_record + num_records {
        let key = i.to_string();
        let value = create_versioned_value(&i.to_string(), expected_max_timestamp);

        let val = store.get_at_or_before_ts(&key, expected_max_timestamp);
        assert!(val.is_some());
        assert_eq!(val.unwrap().1, &value);

        let max_ts = store.get_max_timestamp_for_record(&key);
        assert_eq!(max_ts, Some(&expected_max_timestamp));

        let latests_kv_pair = store.get_latest(&key);
        assert_eq!(latests_kv_pair, Some((&expected_max_timestamp, &value)));
    }
}
