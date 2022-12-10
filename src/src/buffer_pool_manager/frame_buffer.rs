use std::ptr::*;

#[derive(Debug)]
pub struct FrameBuffer {
    pub page_size: usize,
    occupied: Vec<bool>,
    buffer: Vec<u8>,
}

impl FrameBuffer {
    pub const DEFAULT_BYTE: u8 = 0x00u8;
    pub const FRAME_BUFFER_PAGES_SIZE: usize = 16;

    pub fn new(page_size: usize) -> Self {
        assert!(page_size > 0);
        Self {
            page_size,
            occupied: vec![false; Self::FRAME_BUFFER_PAGES_SIZE],
            buffer: vec![Self::DEFAULT_BYTE; page_size * Self::FRAME_BUFFER_PAGES_SIZE],
        }
    }

    #[inline]
    /* private */
    const fn index(&self, index: usize) -> usize {
        self.page_size * index
    }

    pub(super) fn find_first_free_index(&self) -> Option<usize> {
        for i in 0..Self::FRAME_BUFFER_PAGES_SIZE {
            if !self.occupied[i] {
                return Some(i);
            }
        }

        None
    }

    /// # Safety
    /// `index` must be less than `Self::FRAME_BUFFER_PAGES_SIZE`.
    pub(super) fn get_page_data(&self, index: usize) -> Option<&[u8]> {
        if !self.occupied[index] {
            None
        } else {
            Some(&self.buffer[self.index(index)..self.index(index + 1)])
        }
    }

    /// # Safety
    /// `index` must be less than `Self::FRAME_BUFFER_PAGES_SIZE`.
    pub fn clear_page(&mut self, index: usize) {
        self.occupied[index] = false;
    }

    /* private */
    fn shared_load_page(&mut self, index: usize) {
        self.occupied[index] = true;
    }

    /// # Safety
    /// `index` must be less than `Self::FRAME_BUFFER_PAGES_SIZE`.
    /// `data` must point to an array of size `self.page_size`.
    pub fn safe_load_page(&mut self, index: usize, data: &[u8]) {
        self.shared_load_page(index);
        let index = self.index(index)..self.index(index + 1);
        self.buffer[index].copy_from_slice(data);
    }

    /// # Safety
    /// `index` must be less than `Self::FRAME_BUFFER_PAGES_SIZE`.
    /// `data` must point to an array of size `self.page_size`.
    pub unsafe fn unsafe_load_page(&mut self, index: usize, data: *const u8) {
        assert!(!data.is_null());
        self.shared_load_page(index);
        let index = self.index(index);
        let dst_ptr = &mut self.buffer[index] as *mut u8;
        copy_nonoverlapping(data, dst_ptr, self.page_size);
    }
}
