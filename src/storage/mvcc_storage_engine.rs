use crate::storage::{storage_engine::StorageEngine, transaction::Transaction};

pub struct MVCCStorageEngine {
    storage_engine: StorageEngine,
}

impl MVCCStorageEngine {
    pub fn new() -> Self {
        Self {
            storage_engine: StorageEngine::new(),
        }
    }

    pub fn begin_transaction(&self) -> Transaction<'_> {
        Transaction::new(&self.storage_engine)
    }
}
