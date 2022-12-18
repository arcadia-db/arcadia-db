use std::{
    fs::{File, OpenOptions},
    io,
};

#[cfg(target_family = "unix")]
use std::os::unix::prelude::AsRawFd;

#[cfg(target_family = "unix")]
use std::os::unix::prelude::FileExt;

pub struct FileIO {}

#[cfg(target_family = "unix")]
impl FileIO {
    pub fn read(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
        // uses pread syscall internally (only available on unix-like os)
        // allows for parallel reads at offset without lseek
        file.read_exact_at(buf, offset)
    }

    pub fn write(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
        // uses pwrite syscall internally (only available on unix-like os)
        // allows for parallel writes at offset without lseek
        file.write_all_at(buf, offset)
    }

    #[cfg(target_os = "macos")]
    pub fn open_file(path: String, write: bool, create: bool) -> io::Result<File> {
        use nix::libc;

        // when write finishes, we want to be sure that contents have been persisted
        // TODO: O_SYNC not defined on mac OSX open syscall - cannot find equivalent
        // flags |= libc::O_SYNC;

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

    #[cfg(target_os = "linux")]
    fn open_file(path: String, write: bool, create: bool) -> io::Result<File> {
        use nix::libc;
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
            .open(&path);

        if file.is_err() {
            return file;
        }

        let file_fd = file.as_ref().unwrap().as_raw_fd();
        file
    }
}

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

    // use this struct to clean up files after test finishes
    macro_rules! set_up_files {
        ($($x:expr),+ $(,)?) => {
            let _tmp = TmpFiles{paths: vec![$($x.to_string()),+]};
        }
    }

    #[test]
    fn open_file_not_found() {
        let file = FileIO::open_file("test".to_string(), false, false);
        assert!(file.is_err() && file.unwrap_err().kind() == ErrorKind::NotFound);
    }

    #[test]
    fn open_file_create() {
        set_up_files!("test2");

        let file = FileIO::open_file("test2".to_string(), true, true);
        assert!(file.is_ok());
    }

    #[test]
    fn file_write() {
        set_up_files!("test3");

        let file = FileIO::open_file("test3".to_string(), true, true);
        let file = file.unwrap();

        let buf = "hello world\n".as_bytes();
        let res = FileIO::write(&file, &buf, 0);
        assert!(res.is_ok());

        // use std to check result
        let mut std_file = File::open("test3").unwrap();
        let mut buf2: Vec<u8> = Vec::new();
        let res2 = std_file.read_to_end(&mut buf2).unwrap();
        assert_eq!(res2, 12);

        assert_eq!(buf2.as_slice(), buf);
    }

    #[test]
    fn file_read() {
        set_up_files!("test4");

        let file = FileIO::open_file("test4".to_string(), true, true);
        drop(file);

        let file = FileIO::open_file("test4".to_string(), false, false);
        let file = file.unwrap();

        let mut buf: [u8; 13] = [0; 13];

        // read should fail
        let res = FileIO::read(&file, &mut buf, 0);
        assert_eq!(res.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);

        drop(file);

        // let's write some bytes
        let file = FileIO::open_file("test4".to_string(), true, false);
        let res = FileIO::write(&file.unwrap(), "hello world \n".as_bytes(), 0);
        assert!(res.is_ok());

        let file = FileIO::open_file("test4".to_string(), false, false).unwrap();

        // read should succeed
        let res = FileIO::read(&file, &mut buf, 0);
        assert!(res.is_ok());

        assert_eq!(buf, "hello world \n".as_bytes());
    }
}
