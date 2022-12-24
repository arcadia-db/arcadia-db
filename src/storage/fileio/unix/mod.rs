use nix::libc;
use std::fs::OpenOptions;
use std::os::unix::prelude::FileExt;
use std::{fs::File, io};

use super::FileIO;

pub struct UnixFileIO;

impl UnixFileIO {
    #[cfg(target_os = "macos")]
    fn open_or_create_file(path: String, write: bool, create: bool) -> io::Result<File> {
        use std::os::unix::prelude::AsRawFd;

        let file = OpenOptions::new()
            .read(true)
            .write(write)
            .create(create)
            .open(&path);

        if file.is_err() {
            return file;
        }

        let file_fd = file.as_ref().unwrap().as_raw_fd();
        // On OSX, there is no O_DIRECT flag to avoid cache, so we use
        // alternative F_NOCACHE that is fnctl called on the fd
        unsafe { libc::fcntl(file_fd, libc::F_NOCACHE, 1) };

        file
    }

    #[cfg(not(target_os = "macos"))]
    fn open_or_create_file(path: String, write: bool, create: bool) -> io::Result<File> {
        use std::os::unix::prelude::OpenOptionsExt;

        let mut flags = 0;

        // when write finishes, we want to be sure that contents have been persisted
        flags |= libc::O_SYNC;

        // we want to avoid the cache since we are managing it ourselves
        flags |= libc::O_DIRECT;

        let file = OpenOptions::new()
            .custom_flags(flags)
            .read(true)
            .write(write)
            .create(create)
            .open(&path);

        file
    }
}

impl FileIO for UnixFileIO {
    fn read(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
        // uses pread syscall internally (only available on unix-like os)
        // allows for parallel reads at offset without lseek
        file.read_exact_at(buf, offset)
    }

    fn write(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
        // uses pwrite syscall internally (only available on unix-like os)
        // allows for parallel writes at offset without lseek
        file.write_all_at(buf, offset)
    }

    fn open_file(path: String, write: bool) -> io::Result<File> {
        UnixFileIO::open_or_create_file(path, write, false)
    }

    fn create_file(path: String) -> io::Result<File> {
        UnixFileIO::open_or_create_file(path, true, true)
    }
}

#[cfg(target_family = "unix")]
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::{ErrorKind, Read};

    struct TmpFiles {
        paths: Vec<String>,
    }
    impl Drop for TmpFiles {
        fn drop(&mut self) {
            for path in &self.paths {
                let _ = fs::remove_file(path);
            }
        }
    }

    // use this macro before tests to clean up files after test finishes
    macro_rules! set_up_files {
        ($($x:expr),+ $(,)?) => {
            let _tmp = TmpFiles{paths: vec![$($x.to_string()),+]};
            for path in &_tmp.paths {
                let _  = UnixFileIO::create_file(path.clone());
            }
        };
    }

    fn allocate_buffer<'a>(size: usize) -> &'a mut [u8] {
        let mut buf: *mut libc::c_void = std::ptr::null_mut();
        unsafe {
            libc::posix_memalign(&mut buf as *mut *mut libc::c_void, 512, size);
        }
        let buf = buf as *mut u8;
        let buf = unsafe { std::slice::from_raw_parts_mut(buf, size) };
        buf
    }

    #[test]
    fn open_file_not_found() {
        let file = UnixFileIO::open_file("test".to_string(), false);
        assert!(file.is_err() && file.unwrap_err().kind() == ErrorKind::NotFound);
    }

    #[test]
    fn open_file_create() {
        set_up_files!("test2");

        let file = UnixFileIO::open_file("test2".to_string(), true);
        assert!(file.is_ok());
    }

    #[test]
    fn file_write() {
        set_up_files!("test3");

        let file = UnixFileIO::open_file("test3".to_string(), true);
        let file = file.unwrap();

        let buf = allocate_buffer(4096);
        buf[0..12].copy_from_slice("hello world\n".as_bytes());

        let res = UnixFileIO::write(&file, &buf, 0);
        assert!(res.is_ok());

        // use std to check result
        let mut std_file = File::open("test3").unwrap();
        let mut buf2: Vec<u8> = Vec::new();
        let res2 = std_file.read_to_end(&mut buf2).unwrap();
        assert_eq!(res2, 4096);

        assert_eq!(&buf2[0..12], "hello world\n".as_bytes());
    }

    #[test]
    fn file_read() {
        set_up_files!("test4");

        let file = UnixFileIO::open_file("test4".to_string(), true);
        drop(file);

        let file = UnixFileIO::open_file("test4".to_string(), false);
        let file = file.unwrap();

        let mut buf = allocate_buffer(1024);

        // read should fail
        let res = UnixFileIO::read(&file, &mut buf, 0);
        assert_eq!(res.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
        drop(file);
        buf[0..12].copy_from_slice("hello world\n".as_bytes());

        // let's write some bytes
        let file = UnixFileIO::open_file("test4".to_string(), true);
        let res = UnixFileIO::write(&file.unwrap(), &buf, 0);
        assert!(res.is_ok());

        let file = UnixFileIO::open_file("test4".to_string(), false).unwrap();

        // read should succeed
        let res = UnixFileIO::read(&file, &mut buf, 0);
        assert!(res.is_ok());

        assert_eq!(&buf[0..12], "hello world\n".as_bytes());
    }

    #[test]
    fn file_direct_io() {
        set_up_files!("test_io");

        let buf = allocate_buffer(1024);

        buf[0..12].copy_from_slice("hello world\n".as_bytes());

        let file = UnixFileIO::open_file("test_io".to_string(), true).unwrap();
        UnixFileIO::write(&file, buf, 0).unwrap();
    }
}
