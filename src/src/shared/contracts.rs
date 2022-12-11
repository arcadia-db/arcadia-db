use super::definitions::{page::*, *};
use std::sync::*;

pub trait BufferPool: Send + Sync + 'static {
    fn contains_page(&self, page_id: PageId) -> bool;
    /// # Safety
    /// The buffer pool must contain `page_id`.
    fn get_page(&self, page_id: PageId) -> Page<'_>;
    /// # Safety
    /// The buffer pool must contain `page_id`.
    fn set_page_pin(&self, page_id: PageId, pinned: bool);
    /// # Safety
    /// `data.len()` must be equal to `page_size`.
    fn load_page(&self, page_id: PageId, data: Vec<u8>);
    /// # Safety
    /// The buffer pool must contain `page_id`.
    /// `offset + data.len()` must not exceed `page_size`.
    fn mutate_page(&self, page_id: PageId, offset: usize, data: Vec<u8>);
}

pub trait BufferPoolManager: Send + Sync + 'static {
    fn contains_buffer_pool(&self, table_id: TableId) -> bool;
    /// # Safety
    /// The buffer pool manager must not contain `table_id`.
    /// # Parameters
    /// If `page_size` or `entries_count` are not specified, it will use defaults specified in `BufferPoolManagerConfig`.
    fn create_buffer_pool(
        &self,
        table_id: TableId,
        page_size: Option<usize>,
        entries_count: Option<usize>,
    );
    /// # Safety
    /// The buffer pool manager must contain `table_id`.
    fn get_buffer_pool(&self, table_id: TableId) -> Arc<dyn BufferPool>;
}

pub trait StorageHandler: Send + Sync + 'static {
    /// # Safety
    /// `return.len()` must equal `table_id`'s `page_size`.
    fn read_page(&self, table_id: TableId, page_id: PageId) -> Vec<u8>;
    fn write_page(&self, table_id: TableId, page_id: PageId, page: Page<'_>);
}
