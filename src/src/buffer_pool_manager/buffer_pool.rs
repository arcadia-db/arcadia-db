use crate::shared::{
    contracts::*,
    definitions::{page::*, *},
};
use std::{collections::*, sync::*};

#[derive(Debug)]
/* private */
struct FrameBuffer {
    pub page_size: usize,
    pub entries_count: usize,
    buffer: Vec<u8>,
}

impl FrameBuffer {
    pub fn new(page_size: usize, entries_count: usize) -> Self {
        assert!(page_size > 0);
        assert!(entries_count > 0);
        Self {
            page_size,
            entries_count,
            buffer: vec![0x00u8; page_size * entries_count],
        }
    }
}

#[derive(Debug)]
/* private */
enum PageState {
    Clean,
    Dirty,
}

#[derive(Debug)]
/* private */
struct PageTableItem {
    pub pinned_count: usize,
    pub access_count: usize,
    pub state: PageState,
    pub frame_buffer_index: usize,
}

impl PageTableItem {
    pub fn new(frame_buffer_index: usize) -> Self {
        Self {
            pinned_count: 0,
            access_count: 0,
            state: PageState::Clean,
            frame_buffer_index,
        }
    }
}

#[derive(Debug)]
pub struct BufferPoolImpl {
    frame_buffer: FrameBuffer,
    page_table: RwLock<HashMap<PageId, RwLock<PageTableItem>>>,
}

impl BufferPoolImpl {
    pub fn new(page_size: usize, entries_count: usize) -> Self {
        Self {
            frame_buffer: FrameBuffer::new(page_size, entries_count),
            page_table: RwLock::new(HashMap::new()),
        }
    }
}

impl BufferPool for BufferPoolImpl {
    fn contains_page(&self, page_id: PageId) -> bool {
        let page_table = self.page_table.read().unwrap();
        page_table.contains_key(&page_id)
    }

    fn get_page(&self, page_id: PageId) -> Page<'_> {
        let page_table = self.page_table.read().unwrap();
        let page_table_item = page_table.get(&page_id).unwrap();
        let mut page_table_item = page_table_item.write().unwrap();

        page_table_item.access_count += 1;

        let frame_buffer_index = page_table_item.frame_buffer_index;

        todo!()
    }

    fn set_page_pin(&self, page_id: PageId, pinned: bool) {
        let page_table = self.page_table.read().unwrap();
        let page_table_item = page_table.get(&page_id).unwrap();
        let mut page_table_item = page_table_item.write().unwrap();

        if pinned {
            page_table_item.pinned_count += 1;
        } else {
            page_table_item.pinned_count -= 1;
        }
    }

    fn load_page(&self, page_id: PageId, data: Vec<u8>) {
        todo!()
    }

    fn mutate_page(&self, page_id: PageId, offset: usize, data: Vec<u8>) {
        let page_table = self.page_table.read().unwrap();
        let page_table_item = page_table.get(&page_id).unwrap();
        let mut page_table_item = page_table_item.write().unwrap();

        page_table_item.access_count += 1;
        page_table_item.state = PageState::Dirty;

        let frame_buffer_index = page_table_item.frame_buffer_index;

        todo!()
    }
}
