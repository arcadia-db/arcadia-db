use self::contracts::{BufferPool, BufferPoolKey, PageReadLock};
use crate::shared::definitions::{PageId, TableId};
use dashmap::DashMap;
use parking_lot::{lock_api::RawRwLock, Mutex, RawRwLock as PLRawRwLock, RwLock};
use std::{cell::UnsafeCell, ops::Range};

pub mod contracts {
    use super::*;

    pub struct PageReadLock<'a> {
        page_table_item_read_lock_raw: &'a PLRawRwLock,
        frame_read_lock_raw: &'a PLRawRwLock,
        pub data: &'a [u8],
    }

    impl<'a> PageReadLock<'a> {
        pub(super) fn new(
            page_table_item_read_lock_raw: &'a PLRawRwLock,
            frame_read_lock_raw: &'a PLRawRwLock,
            data: &'a [u8],
        ) -> Self {
            Self {
                page_table_item_read_lock_raw,
                frame_read_lock_raw,
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
                // Safety: We unlock the inner lock first.
                self.frame_read_lock_raw.unlock_shared();
                self.page_table_item_read_lock_raw.unlock_shared();
            }
        }
    }

    pub type BufferPoolKey = (TableId, PageId);

    pub trait BufferPool: Send + Sync + 'static {
        fn get_page(&self, id: BufferPoolKey) -> PageReadLock;
        /// # Safety
        /// The buffer pool must contain `id`.
        /// `offset % page_size == 0`.
        /// `offset + data.len() <= page_size`.
        fn mutate_page(&self, id: BufferPoolKey, offset: usize, data: Vec<u8>);
    }
}

#[derive(Debug)]
#[repr(u8)]
/* private */
enum PageTableItemMetadataFlagsMasks {
    IsDirty = 0b00000001,
}

#[derive(Default, Debug)]
#[repr(transparent)]
/* private */
struct PageTableItemMetadataFlags(u8);

macro_rules! create_page_table_item_metadata_flags_get_set {
    ($mask: expr, $get_name: ident, $set_name: ident) => {
        pub const fn $get_name(&self) -> bool {
            (self.0 & ($mask as u8)) != 0
        }

        pub fn $set_name(&mut self, value: bool) {
            self.0 = if value {
                self.0 | ($mask as u8)
            } else {
                self.0 | (!($mask as u8))
            };
        }
    };
}

impl PageTableItemMetadataFlags {
    create_page_table_item_metadata_flags_get_set!(
        PageTableItemMetadataFlagsMasks::IsDirty,
        get_is_dirty,
        set_is_dirty
    );
}

#[derive(Default, Debug)]
/* private */
struct PageTableItemMetadata {
    access_count: usize,
    pin_count: usize,
    flags: PageTableItemMetadataFlags,
}

#[derive(Debug)]
/* private */
enum PageTableItem {
    Free {
        next_free_index: Mutex<Option<usize>>,
    },
    Occupied {
        metadata: Mutex<PageTableItemMetadata>,
        frame_lock: RwLock<()>,
    },
}

impl PageTableItem {
    pub fn new_free(next_free_index: Option<usize>) -> Self {
        Self::Free {
            next_free_index: Mutex::new(next_free_index),
        }
    }

    pub fn new_occupied() -> Self {
        Self::Occupied {
            metadata: Mutex::new(PageTableItemMetadata::default()),
            frame_lock: RwLock::new(()),
        }
    }
}

#[derive(Debug)]
/* private */
struct PageTable {
    root: Mutex<usize>,
    table: UnsafeCell<Vec<RwLock<PageTableItem>>>,
}

impl PageTable {
    pub fn new(entries_count: usize) -> Self {
        assert!(entries_count > 0);

        Self {
            root: Mutex::new(0),
            // TODO: Look into eager initializing this.
            table: UnsafeCell::new(Vec::with_capacity(entries_count)),
        }
    }
}

unsafe impl Send for PageTable {}
unsafe impl Sync for PageTable {}

#[derive(Debug)]
/* private */
struct FrameBuffer {
    pub page_size: usize,
    buffer: UnsafeCell<Vec<u8>>,
}

impl FrameBuffer {
    pub fn new(entries_count: usize, page_size: usize) -> Self {
        assert!(entries_count > 0);
        assert!(page_size > 0);

        Self {
            page_size,
            buffer: UnsafeCell::new(vec![0x00u8; entries_count * page_size]),
        }
    }

    /* private */
    fn get_frame_range(&self, index: usize, offset: usize) -> Range<usize> {
        (self.page_size * index + offset)..(self.page_size * (index + 1))
    }
}

unsafe impl Send for FrameBuffer {}
unsafe impl Sync for FrameBuffer {}

#[derive(Debug)]
pub struct BufferPoolImpl {
    pub entries_count: usize,
    buffer_map: DashMap<BufferPoolKey, usize>,
    page_table: PageTable,
    frame_buffer: FrameBuffer,
}

impl BufferPoolImpl {
    pub fn new(entries_count: usize, page_size: usize) -> Self {
        assert!(entries_count > 0);
        assert!(page_size > 0);

        Self {
            entries_count,
            buffer_map: DashMap::default(),
            page_table: PageTable::new(entries_count),
            frame_buffer: FrameBuffer::new(entries_count, page_size),
        }
    }
}

impl BufferPool for BufferPoolImpl {
    fn get_page(&self, id: BufferPoolKey) -> PageReadLock {
        if let Some(buffer_id) = self.buffer_map.get(&id) {
            // Safety: `page_table` will never reallocate over the lifetime of the program.
            let page_table = unsafe { self.page_table.table.get().as_ref().unwrap() };
            let page_table_item_lock = page_table.get(*buffer_id).unwrap();

            let page_table_item_read_lock_raw = unsafe { page_table_item_lock.raw() };
            page_table_item_read_lock_raw.lock_shared();

            // Safety: We have a read lock over `page_table_item_read_lock`.
            let page_table_item_read_lock_data = page_table_item_lock.data_ptr();
            let page_table_item_read_lock_data_ref =
                unsafe { page_table_item_read_lock_data.as_ref().unwrap() };

            // Safety: We have a read lock over `page_table_item_read_lock`.
            let PageTableItem::Occupied { metadata, frame_lock } = page_table_item_read_lock_data_ref else { panic!("Not occupied!") };
            let frame_read_lock_raw = unsafe { frame_lock.raw() };
            frame_read_lock_raw.lock_shared();

            // Safety: We have a read lock over `frame_read_lock`.
            let buffer = unsafe { self.frame_buffer.buffer.get().as_ref().unwrap() };
            let data = buffer
                .get(self.frame_buffer.get_frame_range(*buffer_id, 0))
                .unwrap();

            {
                let mut page_table_item_metadata_lock = metadata.lock();
                page_table_item_metadata_lock.access_count += 1;
            }

            PageReadLock::new(page_table_item_read_lock_raw, frame_read_lock_raw, data)
        } else {
            // TODO: Not in the buffer pool.
            todo!()
        }
    }

    fn mutate_page(&self, id: BufferPoolKey, offset: usize, data: Vec<u8>) {
        assert!(offset % self.frame_buffer.page_size == 0);
        assert!(offset + data.len() <= self.frame_buffer.page_size);

        let buffer_id = self.buffer_map.get(&id).unwrap();

        // Safety: `page_table` will never reallocate over the lifetime of the program.
        let page_table = unsafe { self.page_table.table.get().as_ref().unwrap() };
        let page_table_item_lock = page_table.get(*buffer_id).unwrap();

        let page_table_item_read_lock = page_table_item_lock.read();

        let PageTableItem::Occupied { ref metadata, ref frame_lock } = *page_table_item_read_lock else { panic!("Not occupied!") };
        let frame_write_lock = frame_lock.write();

        // Safety: We have a write lock over `frame_write_lock`.
        let buffer = unsafe { self.frame_buffer.buffer.get().as_mut().unwrap() };
        buffer
            .get_mut(self.frame_buffer.get_frame_range(*buffer_id, offset))
            .unwrap()
            .copy_from_slice(&data);

        {
            let mut page_table_item_metadata_lock = metadata.lock();
            page_table_item_metadata_lock.access_count += 1;
            page_table_item_metadata_lock.flags.set_is_dirty(true);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::Arc,
        thread::{sleep, spawn, JoinHandle},
        time::Duration,
    };

    fn print_buffer_pool(buffer_pool: &BufferPoolImpl) {
        println!("PageTable {:#?}", unsafe {
            buffer_pool.page_table.table.get().as_ref().unwrap()
        });
        println!("FrameBuffer {:?}", unsafe {
            buffer_pool.frame_buffer.buffer.get().as_ref().unwrap()
        });
        println!("{:#?}", buffer_pool);
    }

    #[test]
    fn test() {
        const PAGE_SIZE: usize = 8;
        const ENTRIES_COUNT: usize = 4;

        let person_table_id = TableId(String::from("Person"));
        let page0_id = PageId(0);
        let person_page0_id = (person_table_id.clone(), page0_id.clone());
        let page4_id = PageId(4);
        let person_page4_id = (person_table_id.clone(), page4_id.clone());

        let buffer_pool = BufferPoolImpl::new(ENTRIES_COUNT, PAGE_SIZE);

        {
            // Setup

            {
                buffer_pool.buffer_map.insert(person_page0_id.clone(), 0);
                buffer_pool.buffer_map.insert(person_page4_id.clone(), 1);
            }

            {
                let mut page_table_root_lock = buffer_pool.page_table.root.lock();
                *page_table_root_lock = 2;
            }

            {
                let page_table = unsafe { buffer_pool.page_table.table.get().as_mut().unwrap() };

                page_table.insert(0, RwLock::new(PageTableItem::new_occupied()));
                page_table.insert(1, RwLock::new(PageTableItem::new_occupied()));
            }

            {
                let person_page0_data = vec![0xFAu8; PAGE_SIZE];
                let person_page4_data = vec![0x23u8; PAGE_SIZE];

                let frame_buffer =
                    unsafe { buffer_pool.frame_buffer.buffer.get().as_mut().unwrap() };

                frame_buffer[buffer_pool.frame_buffer.get_frame_range(0, 0)]
                    .copy_from_slice(&person_page0_data);
                frame_buffer[buffer_pool.frame_buffer.get_frame_range(1, 0)]
                    .copy_from_slice(&person_page4_data);
            }
        }

        let buffer_pool_arc = Arc::new(buffer_pool);

        {
            // Tests that multiple threads can get read lock on Page0 at the same time

            let mut page0_read_lock_threads: Vec<JoinHandle<_>> = Vec::new();

            for i in 0..25 {
                let buffer_pool_arc_clone = Arc::clone(&buffer_pool_arc);
                let person_page0_id_clone = person_page0_id.clone();
                let page0_read_lock_thread = spawn(move || {
                    sleep(Duration::from_nanos(825));
                    println!("Trying Read Page0 {:?}", i);
                    let page0_read_lock = buffer_pool_arc_clone.get_page(person_page0_id_clone);
                    println!("Got Read Page0 {:?}: {:?}", i, page0_read_lock);
                    sleep(Duration::from_secs(10));
                    println!("Done Read Page0 {:?}", i);
                });
                page0_read_lock_threads.push(page0_read_lock_thread);
            }
        }

        {
            // Tests that getting a read lock on Page0 doesn't interfere with getting a read lock on Page4

            let mut page4_read_lock_threads: Vec<JoinHandle<_>> = Vec::new();

            for i in 0..25 {
                let buffer_pool_arc_clone = Arc::clone(&buffer_pool_arc);
                let person_page4_id_clone = person_page4_id.clone();
                let page4_read_lock_thread = spawn(move || {
                    println!("Trying Read Page4 {:?}", i);
                    let page4_read_lock = buffer_pool_arc_clone.get_page(person_page4_id_clone);
                    println!("Got Read Page4 {:?}: {:?}", i, page4_read_lock);
                    sleep(Duration::from_millis(150));
                    println!("Done Read Page4 {:?}", i);
                });
                page4_read_lock_threads.push(page4_read_lock_thread);
            }
        }

        {
            // Tests that mutate is possible on Page4 when Page0 has read locks but not Page4

            let buffer_pool_arc_clone = Arc::clone(&buffer_pool_arc);
            let person_page4_id_clone = person_page4_id.clone();
            let person_page4_data = vec![0x50u8; PAGE_SIZE];
            spawn(move || {
                sleep(Duration::from_millis(500));
                println!("Trying Mutate Page4");
                buffer_pool_arc_clone.mutate_page(person_page4_id_clone, 0, person_page4_data);
                println!("Done Mutate Page4");
            });
        }

        {
            // Tests that mutate Page0 is called after the read locks on Page0 are done

            let buffer_pool_arc_clone = Arc::clone(&buffer_pool_arc);
            let person_page0_id_clone = person_page0_id.clone();
            let person_page0_data = vec![0x05u8; PAGE_SIZE];
            spawn(move || {
                sleep(Duration::from_secs(5));
                println!("Trying Mutate Page0");
                buffer_pool_arc_clone.mutate_page(person_page0_id_clone, 0, person_page0_data);
                println!("Done Mutate Page0");
            });
        }

        {
            // Tests that mutates on Page4 are strictly sequential and don't happen in parallel

            let mut page4_mutate_threads: Vec<JoinHandle<_>> = Vec::new();

            sleep(Duration::from_secs(20));

            for i in 0..25 {
                let buffer_pool_arc_clone = Arc::clone(&buffer_pool_arc);
                let person_page4_id_clone = person_page4_id.clone();
                let person_page4_data = vec![0xAAu8; PAGE_SIZE];
                let page4_mutate_thread = spawn(move || {
                    // Doesn't print properly, but does actually synchronize properly (test with delay in `mutate_page`)
                    println!("Trying Mutate Page4 {:?}", i);
                    buffer_pool_arc_clone.mutate_page(person_page4_id_clone, 0, person_page4_data);
                    println!("Done Mutate Page4 {:?}", i);
                });
                page4_mutate_threads.push(page4_mutate_thread);
            }
        }

        sleep(Duration::from_secs(30));

        println!("\n\n");
        print_buffer_pool(&*buffer_pool_arc);
    }
}
