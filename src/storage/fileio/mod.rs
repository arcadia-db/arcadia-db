use std::{fs::File, io};

use self::unix::UnixFileIO;
#[cfg(target_family = "unix")]
mod unix;

pub trait FileIO {
    fn read(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()>;
    fn write(file: &File, buf: &[u8], offset: u64) -> io::Result<()>;
    fn open_file(path: String, write: bool) -> io::Result<File>;
    fn create_file(path: String) -> io::Result<File>;
}

pub fn default() -> impl FileIO {
    if cfg!(target_family = "unix") {
        UnixFileIO {}
    } else {
        todo!()
    }
}
