#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct TableId(pub String);

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct PageId(pub usize);

pub mod page {
    #[derive(Debug)]
    pub struct Page<'a> {
        pub size: usize,
        data: &'a [u8],
    }

    impl<'a> Page<'a> {
        /// # Safety
        /// `data.len()` must be equal to `size`.
        pub fn new(size: usize, data: &'a [u8]) -> Self {
            assert!(size > 0);
            Self { size, data }
        }

        /// # Safety
        /// `index` must be less than `size`.
        pub fn index(&self, index: usize) -> Option<u8> {
            if index >= self.size {
                None
            } else {
                Some(self.data[index])
            }
        }

        pub fn iter(&self) -> PageIterator<'_> {
            PageIterator {
                current: 0,
                page: self,
            }
        }
    }

    #[derive(Debug)]
    pub struct PageIterator<'a> {
        current: usize,
        page: &'a Page<'a>,
    }

    impl<'a> Iterator for PageIterator<'a> {
        type Item = u8;

        fn next(&mut self) -> Option<Self::Item> {
            if self.current >= self.page.size {
                None
            } else {
                let value = self.page.index(self.current).unwrap();
                self.current += 1;
                Some(value)
            }
        }
    }

    impl<'a> ExactSizeIterator for PageIterator<'a> {
        fn len(&self) -> usize {
            self.page.size - self.current
        }
    }
}
