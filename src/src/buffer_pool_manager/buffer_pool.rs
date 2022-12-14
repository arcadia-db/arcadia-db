use self::contracts::{BufferPool, PageReadLock};
use crate::shared::definitions::{PageId, TableId};
use parking_lot::{
    lock_api::{RawRwLock, RawRwLockDowngrade, RawRwLockUpgrade},
    RawRwLock as PLRawRwLock, RwLock,
};
use std::{cell::UnsafeCell, collections::HashMap, ops::Range};

pub mod contracts {
    use super::*;

    pub struct PageReadLock<'a> {
        page_table_item_read_lock_raw: &'a PLRawRwLock,
        pub data: &'a [u8],
    }

    impl<'a> PageReadLock<'a> {
        pub(super) fn new(page_table_item_read_lock_raw: &'a PLRawRwLock, data: &'a [u8]) -> Self {
            Self {
                page_table_item_read_lock_raw,
                data,
            }
        }
    }

    impl<'a> std::fmt::Debug for PageReadLock<'a> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("PageReadLock")
                .field("data", &self.data)
                .finish()
        }
    }

    impl<'a> Drop for PageReadLock<'a> {
        fn drop(&mut self) {
            // Safety: We assume that the locks were created properly.
            unsafe {
                // Release inner lock first
                self.page_table_item_read_lock_raw.unlock_shared();
                // self.page_table_read_lock_raw.unlock_shared();
            }
        }
    }

    // pub(super) struct PageWriteLock<'a> {
    //     page_table_read_lock_raw: &'a PLRawRwLock,
    //     frame_buffer_index_write_lock_raw: &'a PLRawRwLock,
    //     pub data: &'a mut [u8],
    // }

    // impl<'a> PageWriteLock<'a> {
    //     pub(super) fn new(
    //         page_table_read_lock_raw: &'a PLRawRwLock,
    //         frame_buffer_index_write_lock_raw: &'a PLRawRwLock,
    //         data: &'a mut [u8],
    //     ) -> Self {
    //         Self {
    //             page_table_read_lock_raw,
    //             frame_buffer_index_write_lock_raw,
    //             data,
    //         }
    //     }
    // }

    // impl<'a> std::fmt::Debug for PageWriteLock<'a> {
    //     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    //         f.debug_struct("PageWriteLock")
    //             .field("data", &self.data)
    //             .finish()
    //     }
    // }

    // impl<'a> Drop for PageWriteLock<'a> {
    //     fn drop(&mut self) {
    //         // Safety: We assume that the locks were created properly.
    //         unsafe {
    //             // Release inner lock first
    //             self.frame_buffer_index_write_lock_raw.unlock_exclusive();
    //             self.page_table_read_lock_raw.unlock_shared();
    //         }
    //     }
    // }

    pub trait BufferPool: Send + Sync + 'static {
        fn contains_page(&self, id: (TableId, PageId)) -> bool;
        /// # Safety
        /// The buffer pool must contain `table_id/page_id`.
        fn get_page_read(&self, id: (TableId, PageId)) -> PageReadLock;
        /// # Safety
        /// The buffer pool must contain `table_id/page_id`.
        fn set_page_pinned_state(&self, id: (TableId, PageId), pinned: bool);
        /// # Safety
        /// `data.len() == page_size`.
        fn load_page(&self, id: (TableId, PageId), data: Vec<u8>);
        /// # Safety
        /// The bufer pool must contain `table_id/page_id`.
        /// `offset + data.len() <= page_size`.
        fn mutate_page(&self, id: (TableId, PageId), offset: usize, data: Vec<u8>);
    }
}

#[derive(Debug)]
/* private */
struct FrameBuffer {
    pub page_size: usize,
    pub entries_count: usize,
    buffer: UnsafeCell<Vec<u8>>,
}

impl FrameBuffer {
    pub fn new(page_size: usize, entries_count: usize) -> Self {
        assert!(page_size > 0);
        assert!(entries_count > 0);

        Self {
            page_size,
            entries_count,
            buffer: UnsafeCell::new(vec![0x00u8; page_size * entries_count]),
        }
    }

    /* private */
    fn get_range_to_next(&self, index: usize) -> Range<usize> {
        (index * self.page_size)..((index + 1) * self.page_size)
    }

    /// # Safety
    /// The item in `page_table` with `frame_buffer_index == index` must be at least read locked.
    /* private */
    fn get_frame(&self, index: usize) -> &[u8] {
        let buffer = unsafe { self.buffer.get().as_ref().unwrap() };
        &buffer[self.get_range_to_next(index)]
    }

    /// # Safety
    /// The item in `page_table` with `frame_buffer_index == index` must be write locked.
    /* private */
    fn get_frame_mut(&self, index: usize) -> &mut [u8] {
        let buffer = unsafe { self.buffer.get().as_mut().unwrap() };
        &mut buffer[self.get_range_to_next(index)]
    }
}

unsafe impl Send for FrameBuffer {}
unsafe impl Sync for FrameBuffer {}

#[derive(Debug)]
#[repr(u8)]
pub enum PageState {
    Clean = 0u8,
    Dirty = 1u8,
}

#[derive(Debug)]
/* private */
struct PageTableItem {
    pub pinned_count: usize,
    pub access_count_lock: RwLock<usize>,
    pub state: PageState,
    pub frame_buffer_index: usize,
}

impl PageTableItem {
    pub fn new(frame_buffer_index: usize) -> Self {
        Self {
            pinned_count: 0,
            access_count_lock: RwLock::new(0),
            state: PageState::Clean,
            frame_buffer_index,
        }
    }
}

#[derive(Debug)]
/* private */
struct PageTable {
    ref_table: RwLock<HashMap<(TableId, PageId), usize>>,
    table: UnsafeCell<HashMap<(TableId, PageId), RwLock<PageTableItem>>>,
}

/* private */
struct PageTableItemReadLock<'a> {
    parent: &'a PageTable,
    id: (TableId, PageId),
    page_table_item_lock: &'a RwLock<PageTableItem>,
}

impl<'a> PageTableItemReadLock<'a> {
    pub fn new(parent: &'a PageTable, id: (TableId, PageId), page_table_item_lock: &'a RwLock<PageTableItem>) -> Self {
        Self {
            parent,
            id,
            page_table_item_lock,
        }
    }
}

impl<'a> Drop for PageTableItemReadLock<'a> {
    fn drop(&mut self) {
        // Decrement ref count here
        let mut ref_table_write_lock = self.parent.ref_table.write();
        let ref_table_item = ref_table_write_lock.get_mut(&self.id).unwrap();
        *ref_table_item -= 1;
    }
}

impl PageTable {
    pub fn new() -> Self {
        Self {
            ref_table: RwLock::new(HashMap::new()),
            table: UnsafeCell::new(HashMap::new()),
        }
    }

    fn get_read(&self, id: (TableId, PageId)) -> PageTableItemReadLock {
        let mut ref_table_write_lock = self.ref_table.write();
        if let Some(ref_table_item) = ref_table_write_lock.get_mut(&id) {
            *ref_table_item += 1;
        } else {
            ref_table_write_lock.insert(id.clone(), 1);
        }

        let table = unsafe { self.table.get().as_ref().unwrap() };
        let page_table_item_lock = table.get(&id).unwrap();

        PageTableItemReadLock::new(self, id, page_table_item_lock)
    }

    fn remove(&self, id: (TableId, PageId)) {
        // Loop until ref count is 0
        loop {
            let ref_table_read_lock_raw = unsafe { self.ref_table.raw() };
            ref_table_read_lock_raw.lock_upgradable();
            let ref_table_read_lock_data = self.ref_table.data_ptr();

            let ref_table_read_lock_data_ref = unsafe { ref_table_read_lock_data.as_ref().unwrap() };

            if *ref_table_read_lock_data_ref.get(&id).unwrap() == 0 {
                // Upgrade read lock to write lock
                unsafe { ref_table_read_lock_raw.upgrade(); }
                let ref_table_write_lock_raw = ref_table_read_lock_raw;
                let ref_table_write_lock_data = self.ref_table.data_ptr();

                let ref_table_write_lock_data_ref = unsafe { ref_table_write_lock_data.as_mut().unwrap() };
                ref_table_write_lock_data_ref.remove(&id);

                let table = unsafe { self.table.get().as_mut().unwrap() };
                table.remove(&id);

                return;
            }

            unsafe { ref_table_read_lock_raw.unlock_upgradable(); }

            std::thread::sleep(std::time::Duration::from_micros(5));
        }
    }
}

unsafe impl Send for PageTable {}
unsafe impl Sync for PageTable {}

#[derive(Debug)]
pub struct BufferPoolImpl {
    frame_buffer: FrameBuffer,
    page_table: PageTable,
}

impl BufferPoolImpl {
    pub fn new(page_size: usize, entries_count: usize) -> Self {
        Self {
            frame_buffer: FrameBuffer::new(page_size, entries_count),
            page_table: PageTable::new(),
        }
    }

    // /// # Safety
    // /// The buffer pool must contain `table_id/page_id`.
    // /* private */
    // fn get_page_write(&self, id: (TableId, PageId)) -> PageWriteLock {
    //     // Safety: We have to assume that the state is clean.
    //     let page_table_write_lock_raw = unsafe { self.page_table.raw() };
    //     page_table_write_lock_raw.lock_exclusive();
    //     let page_table_write_lock_data = self.page_table.data_ptr();

    //     // Safety: We have a write lock over `self.page_table`.
    //     let page_table_write_lock_data_ref =
    //         unsafe { page_table_write_lock_data.as_mut().unwrap() };
    //     let page_table_item_mut = page_table_write_lock_data_ref.get_mut(&id).unwrap();
    //     page_table_item_mut.access_count += 1;
    //     page_table_item_mut.state = PageState::Dirty;

    //     // Safety: We have a write lock over `self.page_table`.
    //     unsafe { page_table_write_lock_raw.downgrade() };
    //     let page_table_read_lock_raw = page_table_write_lock_raw;
    //     let page_table_read_lock_data = self.page_table.data_ptr();

    //     // Safety: We have a read lock over `self.page_table`.
    //     let page_table_read_lock_data_ref = unsafe { page_table_read_lock_data.as_ref().unwrap() };
    //     let page_table_item = page_table_read_lock_data_ref.get(&id).unwrap();

    //     // Safety: We have a read lock over `self.page_table`.
    //     let frame_buffer_index_write_lock_raw =
    //         unsafe { page_table_item.frame_buffer_index_lock.raw() };
    //     frame_buffer_index_write_lock_raw.lock_exclusive();
    //     let frame_buffer_index_write_lock_data = page_table_item.frame_buffer_index_lock.data_ptr();

    //     // Safety: We have a write lock over `page_table_item.frame_buffer_index`.
    //     let frame_buffer_index_write_lock_data_ref =
    //         unsafe { frame_buffer_index_write_lock_data.as_mut().unwrap() };
    //     let frame_buffer_index = *frame_buffer_index_write_lock_data_ref;

    //     // Safety: We can assume we have a write lock over the frame with index `frame_buffer_index`.
    //     let data = self.frame_buffer.get_frame_mut(frame_buffer_index);

    //     PageWriteLock::new(
    //         page_table_read_lock_raw,
    //         frame_buffer_index_write_lock_raw,
    //         data,
    //     )
    // }
}

impl BufferPool for BufferPoolImpl {
    fn contains_page(&self, id: (TableId, PageId)) -> bool {
        todo!()

        // let page_table_read_lock = self.page_table.read();
        // page_table_read_lock.contains_key(&id)
    }

    fn get_page_read(&self, id: (TableId, PageId)) -> PageReadLock {
        todo!()

        // let page_table_read_lock_raw = unsafe { self.page_table.raw() };
        // page_table_read_lock_raw.lock_shared();
        // let page_table_read_lock_data = self.page_table.data_ptr();

        // let page_table_read_lock_data_ref = unsafe { page_table_read_lock_data.as_ref().unwrap() };
        // let page_table_item_lock = page_table_read_lock_data_ref.get(&id).unwrap();

        // let page_table_item_read_lock_raw = unsafe { page_table_item_lock.raw() };
        // page_table_item_read_lock_raw.lock_shared();
        // let page_table_item_read_lock_data = page_table_item_lock.data_ptr();

        // let page_table_item_read_lock_data_ref =
        //     unsafe { page_table_item_read_lock_data.as_ref().unwrap() };
        // let frame_buffer_index = page_table_item_read_lock_data_ref.frame_buffer_index;

        // let page_table_item_access_count_write_lock_raw =
        //     unsafe { page_table_item_read_lock_data_ref.access_count_lock.raw() };
        // page_table_item_access_count_write_lock_raw.lock_exclusive();
        // let page_table_item_access_count_write_lock_data = page_table_item_read_lock_data_ref
        //     .access_count_lock
        //     .data_ptr();

        // let page_table_item_access_count_write_lock_data_ref = unsafe {
        //     page_table_item_access_count_write_lock_data
        //         .as_mut()
        //         .unwrap()
        // };
        // *page_table_item_access_count_write_lock_data_ref += 1;

        // unsafe {
        //     page_table_item_access_count_write_lock_raw.unlock_exclusive();
        // }

        // // Safety: We can assume we have a read lock over the frame with index `frame_buffer_index`.
        // let data = self.frame_buffer.get_frame(frame_buffer_index);

        // unsafe {
        //     page_table_read_lock_raw.unlock_shared();
        // }

        // PageReadLock::new(page_table_item_read_lock_raw, data)
    }

    fn set_page_pinned_state(&self, id: (TableId, PageId), pinned: bool) {
        todo!()

        // let mut page_table_write_lock = self.page_table.write();
        // let page_table_item_mut = page_table_write_lock.get_mut(&id).unwrap();

        // if pinned {
        //     page_table_item_mut.pinned_count += 1;
        // } else {
        //     page_table_item_mut.pinned_count -= 1;
        // }
    }

    fn load_page(&self, id: (TableId, PageId), data: Vec<u8>) {
        todo!()
    }

    fn mutate_page(&self, id: (TableId, PageId), offset: usize, data: Vec<u8>) {
        assert!(offset % self.frame_buffer.page_size == 0);
        assert!(offset + data.len() <= self.frame_buffer.page_size);

        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::random;
    use std::{
        sync::Arc,
        thread::{sleep, spawn, JoinHandle},
        time::Duration,
    };

    #[test]
    fn test() {
        const PAGE_SIZE: usize = 16;
        const ENTRIES_COUNT: usize = 8;

        let person_table_id = TableId(String::from("Person"));
        let page0_id = PageId(0);
        let page4_id = PageId(4);

        let buffer_pool = BufferPoolImpl::new(PAGE_SIZE, ENTRIES_COUNT);

        // {
        //     let mut page_table_write_lock = buffer_pool.page_table.write();
        //     page_table_write_lock.insert(
        //         (person_table_id.clone(), page0_id.clone()),
        //         RwLock::new(PageTableItem::new(page0_id.0)),
        //     );
        //     page_table_write_lock.insert(
        //         (person_table_id.clone(), page4_id.clone()),
        //         RwLock::new(PageTableItem::new(page4_id.0)),
        //     );
        // }

        // {
        //     let frame_buffer = buffer_pool.frame_buffer.buffer.get();
        //     let page0_data = vec![0xFAu8; PAGE_SIZE];
        //     let page4_data = vec![0x23u8; PAGE_SIZE];
        //     let frame_buffer = unsafe { frame_buffer.as_mut().unwrap() };
        //     frame_buffer[(page0_id.0 * PAGE_SIZE)..((page0_id.0 + 1) * PAGE_SIZE)]
        //         .copy_from_slice(&page0_data);
        //     frame_buffer[(page4_id.0 * PAGE_SIZE)..((page4_id.0 + 1) * PAGE_SIZE)]
        //         .copy_from_slice(&page4_data);
        // }

        // {
        //     let frame_buffer = buffer_pool.frame_buffer.buffer.get();
        //     let frame_buffer = unsafe { frame_buffer.as_ref().unwrap() };
        //     println!("{:?}", frame_buffer);
        // }

        // println!("{:?}", buffer_pool);

        // {
        //     let page0_read_lock =
        //         buffer_pool.get_page_read((person_table_id.clone(), page0_id.clone()));
        //     println!("{:?}", page0_read_lock);
        // }

        // {
        //     let page4_read_lock =
        //         buffer_pool.get_page_read((person_table_id.clone(), page4_id.clone()));
        //     println!("{:?}", page4_read_lock);
        // }

        // {
        //     let page0_write_lock =
        //         buffer_pool.get_page_write((person_table_id.clone(), page0_id.clone()));
        //     println!("{:?}", page0_write_lock);
        // }

        // {
        //     let page4_write_lock =
        //         buffer_pool.get_page_write((person_table_id.clone(), page4_id.clone()));
        //     println!("{:?}", page4_write_lock);
        // }

        // let buffer_pool_arc: Arc<BufferPoolImpl> = Arc::new(buffer_pool);

        // {
        //     let mut page0_read_lock_threads: Vec<JoinHandle<_>> = Vec::new();

        //     for i in 0..25 {
        //         let buffer_pool_arc_clone = Arc::clone(&buffer_pool_arc);
        //         let id = (person_table_id.clone(), page0_id.clone());
        //         let page0_read_lock_thread = spawn(move || {
        //             let page0_read_lock = buffer_pool_arc_clone.get_page_read(id);
        //             println!("Got Read Page0 {:?}: {:?}", i, page0_read_lock);

        //             sleep(Duration::from_secs(10));

        //             println!("Done Read Page0 {:?}: {:?}", i, page0_read_lock);
        //         });
        //         page0_read_lock_threads.push(page0_read_lock_thread);
        //     }
        // }

        // {
        //     sleep(Duration::from_millis(500));
        //     let mut page_table_write_lock = buffer_pool_arc.page_table.write();
        //     println!("Got Write Lock Page4");
        //     page_table_write_lock.remove(&(person_table_id.clone(), page4_id.clone()));
        //     // sleep(Duration::from_secs(10));
        // }

        // {
        //     sleep(Duration::from_secs(5));
        //     let mut page_table_write_lock = buffer_pool_arc.page_table.write();
        //     println!("Got Write Lock Page0");
        //     page_table_write_lock.remove(&(person_table_id.clone(), page0_id.clone()));
        //     let mut frame_buffer =
        //         unsafe { buffer_pool_arc.frame_buffer.buffer.get().as_mut().unwrap() };
        //     let page0_data = vec![0xAFu8; PAGE_SIZE];
        //     frame_buffer[(page0_id.0 * PAGE_SIZE)..((page0_id.0 + 1) * PAGE_SIZE)]
        //         .copy_from_slice(&page0_data);
        // }

        // {
        //     let mut page0_write_lock_threads: Vec<JoinHandle<()>> = Vec::new();

        //     for i in 0..25 {
        //         let buffer_pool_arc_clone = Arc::clone(&buffer_pool_arc);
        //         let id = (person_table_id.clone(), page0_id.clone());
        //         let page0_write_lock_thread = spawn(move || {
        //             let page0_write_lock = buffer_pool_arc_clone.get_page_write(id);
        //             println!("{:?}: {:?}", i, page0_write_lock);

        //             sleep(Duration::from_micros((random::<u8>().checked_add(i).unwrap_or(i)) as u64));
        //         });
        //         page0_write_lock_threads.push(page0_write_lock_thread);
        //     }
        // }

        sleep(Duration::from_secs(1000));
    }
}
