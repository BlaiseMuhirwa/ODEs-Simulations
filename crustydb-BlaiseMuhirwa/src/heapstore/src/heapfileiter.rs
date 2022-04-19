use crate::heapfile::HeapFile;
use crate::page::{Page, PageIter};
use common::ids::{ContainerId, PageId, TransactionId};
use std::sync::Arc;

#[allow(dead_code)]
/// The struct for a HeapFileIterator.
/// We use a slightly different approach for HeapFileIterator than
/// standard way of Rust's IntoIter for simplicity (avoiding lifetime issues).
/// This should store the state/metadata required to iterate through the file.
///
/// HINT: This will need an Arc<HeapFile>
pub struct HeapFileIterator {
    heapfile: Arc<HeapFile>,
    pub tid: TransactionId,
    pub container_id: ContainerId,
    pub records: Vec<Vec<u8>>,
    pub record_index: usize,
    pub num_records: usize,
}

/// Required HeapFileIterator functions
impl HeapFileIterator {
    /// Create a new HeapFileIterator that stores the container_id, tid, and heapFile pointer.
    /// This should initialize the state required to iterate through the heap file.
    pub(crate) fn new(container_id: ContainerId, tid: TransactionId, hf: Arc<HeapFile>) -> Self {
        let mut records: Vec<Vec<u8>> = Vec::new();
        let num_pages = hf.num_pages();
        for i in 0..num_pages {
            let page = hf.read_page_from_file(i as PageId).unwrap();
            let bytes: Vec<Vec<u8>> = page.into_iter().collect();
            records.extend_from_slice(&bytes[..]);
        }
        let num_records: usize = records.len();

        //let mut page_iter: PageIter = hf.read_page_from_file(0).unwrap().into_iter();
        HeapFileIterator {
            heapfile: hf,
            tid: tid,
            container_id: container_id,
            records: records,
            record_index: 0,
            num_records: num_records,
        }
    }
}

/// Trait implementation for heap file iterator.
/// Note this will need to iterate through the pages and their respective iterators.
impl Iterator for HeapFileIterator {
    type Item = Vec<u8>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.record_index < self.num_records {
            let record = self.records[self.record_index].clone();
            self.record_index += 1;
            return Some(record); 
        }

        return None;
    }
}
