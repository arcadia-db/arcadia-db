use super::{frame_buffer::*, page_table::*};
use crate::shared::definitions::{page::*, *};

#[derive(Debug)]
pub struct BufferPool {
    frame_buffer: FrameBuffer,
    page_table: PageTable,
}

impl BufferPool {
    pub fn new(page_size: usize) -> Self {
        Self {
            frame_buffer: FrameBuffer::new(page_size),
            page_table: PageTable::default(),
        }
    }

    pub fn get_page(&self, page_id: PageId) -> Option<Page<'_>> {
        let page_table_item = self.page_table.get_item(page_id.clone())?;
        let page_data = self
            .frame_buffer
            .get_page_data(page_table_item.frame_buffer_index)?;
        Some(Page::new_safe(
            page_id,
            self.frame_buffer.page_size,
            page_data,
        ))
    }

    /// # Safety
    /// The buffer pool must contain `page_id`.
    pub fn set_pin_page(&mut self, page_id: PageId, pinned: bool) {
        self.page_table.get_item_mut(page_id).unwrap().pinned = pinned;
    }

    /* private */
    fn get_index(&mut self, page_id: PageId) -> usize {
        let index: usize;

        if let Some(page_table_item) = self.page_table.get_item_mut(page_id.clone()) {
            // Page already in Buffer Pool
            index = page_table_item.frame_buffer_index;
            page_table_item.ref_count += 1;
        } else {
            // Need to load Page into Buffer Pool
            if let Some(free_index) = self.frame_buffer.find_first_free_index() {
                index = free_index;
            } else {
                // Evict Pages when no free space in the Frame Buffer
                todo!()
            }

            self.page_table.insert(page_id, index);
        }

        index
    }

    /// # Safety
    /// `data` must point to an array of size `self.frame_buffer.page_size`.
    pub fn safe_load_page(&mut self, page_id: PageId, data: &[u8]) {
        let index = self.get_index(page_id);
        self.frame_buffer.safe_load_page(index, data);
    }

    /// # Safety
    /// `data` must point to an array of size `self.frame_buffer.page_size`.
    pub unsafe fn unsafe_load_page(&mut self, page_id: PageId, data: *const u8) {
        let index = self.get_index(page_id);
        self.frame_buffer.unsafe_load_page(index, data);
    }
}
