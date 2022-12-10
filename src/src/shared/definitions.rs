#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct TableId(pub String);

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct PageId(pub usize);

pub mod page {
    use super::*;

    #[derive(Debug)]
    pub enum PageData<'a> {
        Safe(&'a [u8]),
        Unsafe(*const u8),
    }

    #[derive(Debug)]
    pub struct Page<'a> {
        pub id: PageId,
        pub size: usize,
        data: PageData<'a>,
    }

    impl<'a> Page<'a> {
        pub fn new_safe(id: PageId, size: usize, data: &'a [u8]) -> Self {
            Self {
                id,
                size,
                data: PageData::Safe(data),
            }
        }

        /// # Safety
        /// The number of bytes in the array pointed to by `data` must be equal to `size`.
        pub unsafe fn new_unsafe(id: PageId, size: usize, data: *const u8) -> Self {
            Self {
                id,
                size,
                data: PageData::Unsafe(data),
            }
        }

        pub fn is_unsafe(&self) -> bool {
            matches!(self.data, PageData::Unsafe(_))
        }

        /// # Safety
        /// The value of `index` must be less than `self.size`.
        pub fn index(&self, index: usize) -> Option<u8> {
            match self.data {
                PageData::Safe(safe) => Some(safe[index]),
                PageData::Unsafe(r#unsafe) => {
                    if index >= self.size {
                        None
                    } else {
                        Some(unsafe { *r#unsafe.offset(index.try_into().ok()?).as_ref().unwrap() })
                    }
                }
            }
        }

        pub fn iter(&'a self) -> PageIterator<'a> {
            PageIterator::from_beginning(self)
        }
    }

    #[derive(Debug)]
    pub struct PageIterator<'a> {
        current: usize,
        page: &'a Page<'a>,
    }

    impl<'a> PageIterator<'a> {
        pub fn from_beginning(page: &'a Page<'a>) -> Self {
            Self { current: 0, page }
        }
    }

    impl<'a> Iterator for PageIterator<'a> {
        type Item = u8;

        fn next(&mut self) -> Option<Self::Item> {
            if self.current >= self.page.size {
                None
            } else {
                let value = self.page.index(self.current)?;
                self.current += 1;
                Some(value)
            }
        }
    }
}
