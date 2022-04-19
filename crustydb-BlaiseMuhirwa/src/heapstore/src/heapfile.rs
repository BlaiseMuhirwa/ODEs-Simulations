use crate::page::Page;
use common::ids::PageId;
use common::{CrustyError, PAGE_SIZE};
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, RwLock};

use std::io::BufWriter;
use std::io::{Seek, SeekFrom};
use std::os::unix::fs::FileExt;

/// The struct for a heap file.  
///
/// HINT: You likely will want to design for interior mutability for concurrent accesses.
/// eg Arc<RwLock<>> on some internal members
///
/// HINT: You will probably not be able to serialize HeapFile, as it needs to maintain a link to a
/// File object, which cannot be serialized/deserialized/skipped by serde. You don't need to worry
/// about persisting read_count/write_count during serialization.
///
/// Your code should persist what information is needed to recreate the heapfile.
///
pub(crate) struct HeapFile {
    pub file: Arc<RwLock<File>>,
    // The following are for profiling/ correctness checks
    pub read_count: AtomicU16,
    pub write_count: AtomicU16,
}

/// HeapFile required functions
impl HeapFile {
    /// Create a new heapfile for the given path and container Id. Return Result<Self> if able to create.
    /// Errors could arise from permissions, space, etc when trying to create the file used by HeapFile.
    pub(crate) fn new(file_path: PathBuf) -> Result<Self, CrustyError> {
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
        {
            Ok(f) => f,
            Err(error) => {
                return Err(CrustyError::CrustyError(format!(
                    "Cannot open or create heap file: {} {} {:?}",
                    file_path.to_string_lossy(),
                    error.to_string(),
                    error
                )))
            }
        };

        let pages: u64 = 0;
        file.write_all_at(&pages.to_le_bytes(), 0)?;

        Ok(HeapFile {
            file: Arc::new(RwLock::new(file)),
            read_count: AtomicU16::new(0),
            write_count: AtomicU16::new(0),
        })
    }

    /// Return the number of pages for this HeapFile.
    /// Return type is PageId (alias for another type) as we cannot have more
    /// pages than PageId can hold.
    pub fn num_pages(&self) -> PageId {
        let mut file_lock = (*self.file).read().unwrap();

        let mut page_number_buff: [u8; 8] = [0; 8];
        file_lock.read_exact_at(&mut page_number_buff, 0);
        let pages = u64::from_le_bytes(page_number_buff.try_into().unwrap());
        return pages as PageId;
    }

    /// Read the page from the file.
    /// Errors could arise from the filesystem or invalid pageId
    pub(crate) fn read_page_from_file(&self, pid: PageId) -> Result<Page, CrustyError> {
        //If profiling count reads
        #[cfg(feature = "profile")]
        {
            self.read_count.fetch_add(1, Ordering::Relaxed);
        }
        let mut file_lock = (*self.file).read().unwrap();

        /* read number of pages in the heapfile */
        let mut page_number_buff: [u8; 8] = [0; 8];
        let mut file = &*file_lock;

        file.read_exact_at(&mut page_number_buff, 0)?;
        let pages = u64::from_le_bytes(page_number_buff.try_into().unwrap());
        if u64::from(pid) >= pages {
            return Err(CrustyError::CrustyError(format!("Invalid PageId")));
        }
        let mut page: Page = Page::new(pid);
        let mut buffer: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        file.seek(SeekFrom::Start(8 + (pid as usize * PAGE_SIZE) as u64))?;
        match file.read_exact(&mut buffer) {
            Ok(_) => {
                return {
                    let page = Page::from_bytes(&buffer);
                    Ok(page)
                }
            }
            Err(_) => return Err(CrustyError::CrustyError(format!("Error reading file"))),
        }
    }

    /// Take a page and write it to the underlying file.
    /// This could be an existing page or a new page
    pub(crate) fn write_page_to_file(&self, page: Page) -> Result<(), CrustyError> {
        //If profiling count writes
        #[cfg(feature = "profile")]
        {
            self.write_count.fetch_add(1, Ordering::Relaxed);
        }
        let page_id: PageId = page.header.page_id;

        let file_lock = (*self.file).write().unwrap();
        let mut page_number_buff: [u8; 8] = [0; 8];
        file_lock.read_exact_at(&mut page_number_buff, 0)?;

        let pages = u64::from_le_bytes(page_number_buff.try_into().unwrap());
        let result;
        if page.header.page_id < pages as u16 {
            /* write to an existing page in heapfile */
            result = file_lock
                .write_all_at(&page.get_bytes(), 8 + (page_id as usize * PAGE_SIZE) as u64);
        } else {
            /* write a new page to heapfile */
            file_lock.write_all_at(&(pages + 1).to_le_bytes(), 0);
            result = file_lock
                .write_all_at(&page.get_bytes(), 8 + (page_id as usize * PAGE_SIZE) as u64);
        }

        match result {
            Ok(()) => return Ok(()),
            Err(err) => {
                return Err(CrustyError::CrustyError(format!(
                    "Error writing file: {}",
                    err.to_string()
                )))
            }
        }
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use common::testutil::*;
    use temp_testdir::TempDir;

    #[test]
    fn hs_hf_insert() {
        init();

        //Create a temp file
        let f = gen_random_dir();
        let tdir = TempDir::new(f, true);
        let mut f = tdir.to_path_buf();
        f.push(gen_rand_string(4));
        f.set_extension("hf");

        let mut hf = HeapFile::new(f.to_path_buf()).unwrap();

        // Make a page and write
        let mut p0 = Page::new(0);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let p0_bytes = p0.get_bytes();

        hf.write_page_to_file(p0);
        //check the page

        assert_eq!(1, hf.num_pages());

        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.get_bytes());

        //Add another page
        let mut p1 = Page::new(1);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let p1_bytes = p1.get_bytes();

        hf.write_page_to_file(p1);

        assert_eq!(2, hf.num_pages());
        //Recheck page0

        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.get_bytes());

        //check page 1
        let checkp1 = hf.read_page_from_file(1).unwrap();
        assert_eq!(p1_bytes, checkp1.get_bytes());

        #[cfg(feature = "profile")]
        {
            assert_eq!(*hf.read_count.get_mut(), 3);
            assert_eq!(*hf.write_count.get_mut(), 2);
        }
    }
}
