use self::buffer_pool::*;
use crate::shared::{config::*, contracts::*, definitions::*};
use std::{collections::*, sync::*};

mod buffer_pool;

#[derive(Debug)]
pub struct BufferPoolManagerImpl {
    config: BufferPoolManagerConfig,
    pools: RwLock<HashMap<TableId, Arc<BufferPoolImpl>>>,
}

impl BufferPoolManagerImpl {
    pub fn new(config: BufferPoolManagerConfig) -> Self {
        Self {
            config,
            pools: RwLock::new(HashMap::new()),
        }
    }
}

impl BufferPoolManager for BufferPoolManagerImpl {
    fn contains_buffer_pool(&self, table_id: TableId) -> bool {
        let pools = self.pools.read().unwrap();
        pools.contains_key(&table_id)
    }

    fn create_buffer_pool(
        &self,
        table_id: TableId,
        page_size: Option<usize>,
        entries_count: Option<usize>,
    ) {
        let page_size = if let Some(page_size) = page_size {
            page_size
        } else {
            self.config.default_page_size
        };

        let entries_count = if let Some(entries_count) = entries_count {
            entries_count
        } else {
            self.config.default_entries_count
        };

        let mut pools = self.pools.write().unwrap();
        pools.insert(
            table_id,
            Arc::new(BufferPoolImpl::new(page_size, entries_count)),
        );
    }

    fn get_buffer_pool(&self, table_id: TableId) -> Arc<dyn BufferPool> {
        let pools = self.pools.read().unwrap();
        let buffer_pool = pools.get(&table_id).unwrap();
        Arc::clone(buffer_pool) as Arc<dyn BufferPool>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_test_without_load() {
        let buffer_pool_manager_config = BufferPoolManagerConfig {
            default_page_size: 16,
            default_entries_count: 8,
        };

        let person_table_id = TableId(String::from("Person"));
        let phone_number_table_id = TableId(String::from("PhoneNumber"));

        let page0_id = PageId(0);

        let buffer_pool_manager: Arc<dyn BufferPoolManager> =
            Arc::new(BufferPoolManagerImpl::new(buffer_pool_manager_config));

        buffer_pool_manager.create_buffer_pool(person_table_id.clone(), None, None);

        assert!(buffer_pool_manager.contains_buffer_pool(person_table_id.clone()));
        assert!(!buffer_pool_manager.contains_buffer_pool(phone_number_table_id.clone()));

        let person_buffer_pool = buffer_pool_manager.get_buffer_pool(person_table_id.clone());

        buffer_pool_manager.create_buffer_pool(phone_number_table_id.clone(), Some(8), Some(16));

        assert!(buffer_pool_manager.contains_buffer_pool(person_table_id.clone()));
        assert!(buffer_pool_manager.contains_buffer_pool(phone_number_table_id.clone()));

        assert!(!person_buffer_pool.contains_page(page0_id.clone()));
    }
}
