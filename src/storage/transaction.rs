use crate::storage::{
    occ_comitter::OccCommitResult, storage_engine::StorageEngine,
    transaction_workspace::TransactionWorkSpace,
};

pub struct Transaction<'a> {
    workspace: TransactionWorkSpace<'a>,
}

impl<'a> Transaction<'a> {
    pub fn new(storage_engine: &'a StorageEngine) -> Self {
        Self {
            workspace: TransactionWorkSpace::new(&storage_engine),
        }
    }

    pub fn get<'b>(&'b mut self, key: &str) -> Option<(&'b u64, &'b String)> {
        self.workspace.read_key(key)
    }

    pub fn put(&mut self, key: String, value: String) {
        self.workspace.write_key(key, value);
    }

    pub fn commit(self) -> (OccCommitResult, u64) {
        self.workspace.commit()
    }

    pub fn abort(self) {
        self.workspace.abort();
    }

    pub fn begin_timestamp(&self) -> u64 {
        return self.workspace.begin_timestamp();
    }
}
