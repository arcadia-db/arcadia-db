use crate::shared::definitions::*;
use std::collections::HashMap;

#[derive(Debug)]
pub enum PageState {
    Clean,
    Dirty,
}

#[derive(Debug)]
pub struct PageTableItem {
    pub pinned: bool,
    pub ref_count: usize,
    pub state: PageState,
    pub frame_buffer_index: usize,
}

impl PageTableItem {
    pub fn new(frame_buffer_index: usize) -> Self {
        Self {
            pinned: false,
            ref_count: 0,
            state: PageState::Clean,
            frame_buffer_index,
        }
    }
}

#[derive(Default, Debug)]
pub struct PageTable {
    table: HashMap<PageId, PageTableItem>,
}

impl PageTable {
    pub fn get_item(&self, page_id: PageId) -> Option<&PageTableItem> {
        self.table.get(&page_id)
    }

    pub fn get_item_mut(&mut self, page_id: PageId) -> Option<&mut PageTableItem> {
        self.table.get_mut(&page_id)
    }

    pub fn insert(&mut self, page_id: PageId, frame_buffer_index: usize) {
        let mut page_table_item = PageTableItem::new(frame_buffer_index);
        page_table_item.ref_count += 1;
        self.table.insert(page_id, page_table_item);
    }
}
