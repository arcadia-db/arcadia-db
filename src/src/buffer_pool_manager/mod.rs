use crate::shared::definitions::*;
use buffer_pool::*;
use std::collections::HashMap;

mod buffer_pool;
mod frame_buffer;
mod page_table;

#[derive(Default, Debug)]
pub struct BufferPoolManager {
    pools: HashMap<TableId, BufferPool>,
}

impl BufferPoolManager {
    pub fn create_buffer_pool(&mut self, table_id: TableId, page_size: usize) {
        self.pools.insert(table_id, BufferPool::new(page_size));
    }

    pub fn get_buffer_pool_mut(&mut self, table_id: TableId) -> Option<&mut BufferPool> {
        self.pools.get_mut(&table_id)
    }
}
