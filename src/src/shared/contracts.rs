use super::definitions::*;

pub trait StorageEngineExports {
    /// # Safety
    /// The number of bytes in the array pointed to by the return value must equal the page size of the table referred to by `table_id`.
    fn read_page(&self, table_id: TableId, page_id: PageId) -> *const u8;
}
