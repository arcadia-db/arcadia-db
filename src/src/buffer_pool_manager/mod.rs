use self::{
    buffer_pool::{contracts::BufferPool, BufferPoolImpl},
    contracts::BufferPoolManager,
};
use crate::shared::{config::BufferPoolManagerConfig, definitions::TableId};
use parking_lot::RwLock;
use std::{collections::HashMap, sync::Arc};

pub mod contracts {
    use super::*;

    pub use buffer_pool::contracts::*;

    pub trait BufferPoolManager: Send + Sync + 'static {
        fn contains_unique_buffer_pool(&self, table_id: TableId) -> bool;
        fn get_buffer_pool(&self, table_id: TableId) -> Arc<dyn BufferPool>;
        /// # Safety
        /// The buffer pool manager must not contain `table_id`.
        fn create_unique_buffer_pool(
            &self,
            table_id: TableId,
            page_size: usize,
            entries_count: usize,
        );
    }

    pub fn create_default_buffer_pool_manager(
        config: BufferPoolManagerConfig,
    ) -> Arc<dyn BufferPoolManager> {
        Arc::new(BufferPoolManagerImpl::new(config))
    }
}

mod buffer_pool;

#[derive(Debug)]
/* private */
struct BufferPoolManagerImpl {
    default_buffer_pool: Arc<BufferPoolImpl>,
    unique_buffer_pools: RwLock<HashMap<TableId, Arc<BufferPoolImpl>>>,
}

impl BufferPoolManagerImpl {
    pub fn new(config: BufferPoolManagerConfig) -> Self {
        Self {
            default_buffer_pool: Arc::new(BufferPoolImpl::new(
                config.default_page_size,
                config.default_entries_count,
            )),
            unique_buffer_pools: RwLock::new(HashMap::new()),
        }
    }
}

impl BufferPoolManager for BufferPoolManagerImpl {
    fn contains_unique_buffer_pool(&self, table_id: TableId) -> bool {
        let unique_buffer_pools_read_lock = self.unique_buffer_pools.read();
        unique_buffer_pools_read_lock.contains_key(&table_id)
    }

    fn get_buffer_pool(&self, table_id: TableId) -> Arc<dyn BufferPool> {
        let unique_buffer_pools_read_lock = self.unique_buffer_pools.read();

        let buffer_pool =
            if let Some(unique_buffer_pool) = unique_buffer_pools_read_lock.get(&table_id) {
                Arc::clone(&unique_buffer_pool)
            } else {
                Arc::clone(&self.default_buffer_pool)
            };

        buffer_pool
    }

    fn create_unique_buffer_pool(&self, table_id: TableId, page_size: usize, entries_count: usize) {
        let mut unique_buffer_pools_write_lock = self.unique_buffer_pools.write();
        unique_buffer_pools_write_lock.insert(
            table_id,
            Arc::new(BufferPoolImpl::new(page_size, entries_count)),
        );
    }
}
