#[derive(Debug)]
pub struct BufferPoolManagerConfig {
    pub default_page_size: usize,
    pub default_entries_count: usize,
}
