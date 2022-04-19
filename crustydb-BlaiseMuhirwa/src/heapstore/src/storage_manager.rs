use crate::heapfile::HeapFile;
use crate::heapfileiter::HeapFileIterator;
use crate::page::Page;
use common::prelude::*;
use common::storage_trait::StorageTrait;
use common::testutil::gen_random_dir;
use common::PAGE_SIZE;
use std::collections::HashMap;
use std::fs;
use std::io::BufReader;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, RwLock};

/// The StorageManager struct
#[derive(Serialize, Deserialize)]
pub struct StorageManager {
    /// Path to database metadata files.
    pub storage_path: String,
    is_temp: bool,
    #[serde(skip)]
    container_files: Arc<RwLock<HashMap<ContainerId, Arc<HeapFile>>>>,
}

/// The required functions in HeapStore's StorageManager that are specific for HeapFiles
impl StorageManager {
    /// Get a page if exists for a given container.
    pub(crate) fn get_page(
        &self,
        container_id: ContainerId,
        page_id: PageId,
        _tid: TransactionId,
        _perm: Permissions,
        _pin: bool,
    ) -> Option<Page> {
        let container_files = (*self.container_files).read().unwrap();
        let heapfile: &HeapFile = &*(*(*container_files).get(&container_id).unwrap());

        match (*heapfile).read_page_from_file(page_id) {
            Ok(page) => Some(page),
            Err(_) => None,
        }
    }

    /// Write a page
    pub(crate) fn write_page(
        &self,
        container_id: ContainerId,
        page: Page,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> {
        let container_files = (*self.container_files).write().unwrap();
        let cid_hashmap = &*container_files;
        let heapfile = &*cid_hashmap.get(&container_id).unwrap();

        return heapfile.write_page_to_file(page);
    }

    /// Get the number of pages for a container
    fn get_num_pages(&self, container_id: ContainerId) -> PageId {
        let container_files = (*self.container_files).read().unwrap();
        let cid_hashmap = &*container_files;
        if !cid_hashmap.contains_key(&container_id) {
            return 0;
        }
        let heapfile = &*cid_hashmap.get(&container_id).unwrap();
        return heapfile.num_pages();
    }

    /// Test utility function for counting reads and writes served by the heap file.
    /// Can return 0,0 for invalid container_ids
    #[allow(dead_code)]
    pub(crate) fn get_hf_read_write_count(&self, container_id: ContainerId) -> (u16, u16) {
        let container_files = (*self.container_files).read().unwrap();
        let cid_hashmap = &*container_files;

        if !cid_hashmap.contains_key(&container_id) {
            return (0, 0);
        }
        let heapfile = &*cid_hashmap.get(&container_id).unwrap();
        (
            heapfile.read_count.load(Ordering::Relaxed),
            heapfile.write_count.load(Ordering::Relaxed),
        )
    }
}

/// Implementation of storage trait
impl StorageTrait for StorageManager {
    type ValIterator = HeapFileIterator;

    /// Create a new storage manager that will use storage_path as the location to persist data
    /// (if the storage manager persists records on disk)
    fn new(storage_path: String) -> Self {
        fs::create_dir_all(&storage_path);

        // create storage manager
        let mut id_to_files = HashMap::new();
        debug!(
            "Looking for storage manager in {}/serde_containers",
            &storage_path
        );
        let path = format!("{}/serde_containers/all_containers", &storage_path);

        if Path::new(&path).exists() {
            let serde_file = fs::File::open(path).unwrap();
            {
                let mappings: HashMap<String, String> =
                    serde_json::from_reader(serde_file).unwrap();

                for (_id, filepath) in mappings.iter() {
                    let c_id: ContainerId = _id.parse().unwrap();
                    let file: fs::File = fs::File::open(filepath).unwrap();
                    let heapfile: HeapFile = HeapFile {
                        file: Arc::new(RwLock::new(file)),
                        read_count: AtomicU16::new(0),
                        write_count: AtomicU16::new(0),
                    };
                    id_to_files.insert(c_id, Arc::new(heapfile));
                }
            }
        }

        let storage_manager = StorageManager {
            storage_path: storage_path,
            is_temp: false,
            container_files: Arc::new(RwLock::new(id_to_files)),
        };
        return storage_manager;
    }

    /// Create a new storage manager for testing. If this creates a temporary directory it should be cleaned up
    /// when it leaves scope.
    fn new_test_sm() -> Self {
        let storage_path = gen_random_dir().to_string_lossy().to_string();
        debug!("Making new temp storage_manager {}", storage_path);

        fs::create_dir_all(storage_path.clone());
        return StorageManager {
            storage_path: storage_path,
            is_temp: true,
            container_files: Arc::new(RwLock::new(HashMap::new())),
        };
    }

    fn get_simple_config() -> common::ContainerConfig {
        common::ContainerConfig::simple_container()
    }

    /// Insert some bytes into a container for a particular value (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns the value id associated with the stored value.
    /// Function will need to find the first page that can hold the value.
    /// A new page may need to be created if no space on existing pages can be found.
    fn insert_value(
        &self,
        container_id: ContainerId,
        value: Vec<u8>,
        tid: TransactionId,
    ) -> ValueId {
        if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        }

        /* searches for the first page that can hold value */
        let mut page_found: bool = false;
        let num_pages: PageId = self.get_num_pages(container_id);
        for page_index in 0..num_pages {
            if let Some(mut page) = self.get_page(
                container_id,
                page_index as PageId,
                tid,
                Permissions::ReadWrite,
                false,
            ) {
                if let Some(slot_id) = page.add_value(&value) {
                    self.write_page(container_id, page, tid);
                    page_found = true;
                    return ValueId {
                        container_id: container_id,
                        segment_id: None,
                        page_id: Some(page_index as PageId),
                        slot_id: Some(slot_id),
                    };
                }
            }
        }
        /* create a new page if the page was not found */
        let mut value_id = ValueId {
            container_id: container_id,
            segment_id: None,
            page_id: Some(num_pages),
            slot_id: None,
        };
        if !page_found {
            let mut new_page: Page = Page::new(num_pages);
            if let Some(slot_id) = new_page.add_value(&value) {
                self.write_page(container_id, new_page, tid);
                value_id.slot_id = Some(slot_id);
            }
        }
        return value_id;
    }

    /// Insert some bytes into a container for vector of values (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns a vector of value ids associated with the stored values.
    fn insert_values(
        &self,
        container_id: ContainerId,
        values: Vec<Vec<u8>>,
        tid: TransactionId,
    ) -> Vec<ValueId> {
        let mut insertion_result: Vec<ValueId> = Vec::new();
        let values_iterator = values.iter();
        for value in values_iterator {
            insertion_result.push(self.insert_value(container_id, value.to_vec(), tid));
        }
        return insertion_result;
    }

    /// Delete the data for a value. If the valueID is not found it returns Ok() still.
    fn delete_value(&self, id: ValueId, tid: TransactionId) -> Result<(), CrustyError> {
        let container_files = self.container_files.write().unwrap();
        let heapfile: &HeapFile = &*(*(*container_files).get(&id.container_id).unwrap());

        match (*heapfile).read_page_from_file(id.page_id.unwrap()) {
            Ok(mut page) => {
                if let Some(()) = page.delete_value(id.slot_id.unwrap()) {
                    (*heapfile).write_page_to_file(page);
                    return Ok(());
                } else {
                    return Err(CrustyError::CrustyError(format!(
                        "slot_id {} could not be found in page {} for deletion",
                        id.slot_id.unwrap(),
                        id.page_id.unwrap()
                    )));
                }
            }
            Err(_) => {
                return Err(CrustyError::CrustyError(format!(
                    "page with id {} does not exist for container {}",
                    id.container_id,
                    id.page_id.unwrap()
                )));
            }
        }
    }

    /// Updates a value. Returns valueID on update (which may have changed). Error on failure
    /// Any process that needs to determine if a value changed will need to compare the return valueId against
    /// the sent value.
    fn update_value(
        &self,
        value: Vec<u8>,
        id: ValueId,
        _tid: TransactionId,
    ) -> Result<ValueId, CrustyError> {
        if let Ok(()) = self.delete_value(id, _tid) {
            let value: ValueId = self.insert_value(id.container_id, value, _tid);
            return Ok(value);
        }
        return Err(CrustyError::CrustyError(format!(
            "Failed to Update Value with container_id {}, page_id {}, slot_id {}",
            id.container_id,
            id.page_id.unwrap(),
            id.slot_id.unwrap()
        )));
    }

    /// Create a new container to be stored.
    /// fn create_container(&self, name: String) -> ContainerId;
    /// Creates a new container object.
    /// For this milestone you will not need to utilize
    /// the container_config, name, container_type, or dependencies
    ///
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to add delta to.
    fn create_container(
        &self,
        container_id: ContainerId,
        _container_config: common::ContainerConfig,
        _name: Option<String>,
        _container_type: common::ids::StateType,
        _dependencies: Option<Vec<ContainerId>>,
    ) -> Result<(), CrustyError> {
        fs::create_dir_all(&format!("{}/containers/", &self.storage_path));

        let mut pathbuf: PathBuf = PathBuf::new();
        let mut c_id_file = "containers/heapfile_".to_string();
        c_id_file.push_str(&container_id.to_string());
        let mut file_name: String = format!("{}/{}", &self.storage_path, c_id_file);

        let mut pathbuf: PathBuf = PathBuf::new();
        pathbuf.push(file_name);

        match HeapFile::new(pathbuf) {
            Ok(heapfile) => {
                let mut container_files = self.container_files.write().unwrap();
                (*container_files).insert(container_id, Arc::new(heapfile));
                return Ok(());
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    /// A wrapper function to call create container
    fn create_table(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        self.create_container(
            container_id,
            StorageManager::get_simple_config(),
            None,
            common::ids::StateType::BaseTable,
            None,
        )
    }

    /// Remove the container and all stored values in the container.
    /// If the container is persisted remove the underlying files
    fn remove_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        let mut file_path = self.storage_path.clone();
        let c_id: &str = &container_id.to_string();
        file_path.push_str("containers/heapfile_");
        file_path.push_str(c_id);

        // remove the file from the file system
        fs::remove_file(&file_path)?;

        // remove the container_id-to-HeapFile mapping
        let mut container_files = (*self.container_files).write().unwrap();
        (*container_files).remove(&container_id);

        Ok(())
    }

    /// Get an iterator that returns all valid records
    fn get_iterator(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
    ) -> Self::ValIterator {
        let container_files = self.container_files.read().unwrap();
        if !(*container_files).contains_key(&container_id) {
            panic!("Container does not exist in the storage manager");
        }
        let arc_file: &Arc<HeapFile> = (*container_files).get(&container_id).unwrap();
        let mut cloned_file: Arc<HeapFile> = arc_file.clone();

        return HeapFileIterator::new(container_id, tid, cloned_file);
    }

    /// Get the data for a particular ValueId. Error if does not exists
    fn get_value(
        &self,
        id: ValueId,
        tid: TransactionId,
        perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        if let Some(page) = self.get_page(id.container_id, id.page_id.unwrap(), tid, perm, false) {
            match page.get_value(id.slot_id.unwrap()) {
                Some(bytes) => {
                    return Ok(bytes);
                }
                None => {
                    return Err(CrustyError::CrustyError(format!(
                        "Value for container_id {}, page_id {} and slot_id {} does not exist",
                        id.container_id,
                        id.page_id.unwrap(),
                        id.slot_id.unwrap()
                    )));
                }
            }
        }
        return Err(CrustyError::CrustyError(format!(
            "Value for container_id {}, page_id {} and slot_id {} does not exist",
            id.container_id,
            id.page_id.unwrap(),
            id.slot_id.unwrap()
        )));
    }

    /// Notify the storage manager that the transaction is finished so that any held resources can be released.
    fn transaction_finished(&self, tid: TransactionId) {
        panic!("TODO milestone tm");
    }

    /// Testing utility to reset all state associated the storage manager.
    fn reset(&self) -> Result<(), CrustyError> {
        fs::remove_dir_all(&self.storage_path)?;

        // clear all metadata
        let mut container_files = (*self.container_files).write().unwrap();
        (*container_files).clear();

        fs::create_dir_all(&self.storage_path)?;
        fs::create_dir_all(&format!("{}/containers/", &self.storage_path));
        Ok(())
    }

    /// If there is a buffer pool or cache it should be cleared/reset.
    fn clear_cache(&self) {
        panic!("TODO milestone hs");
    }

    /// Shutdown the storage manager. Can call drop. Should be safe to call multiple times.
    /// If temp, this should remove all stored files.
    /// If not a temp SM, this should serialize the mapping between containerID and Heapfile.
    /// HINT: Heapfile won't be serializable/deserializable. You'll want to serialize information
    /// that can be used to create a HeapFile object pointing to the same data. You don't need to
    /// worry about recreating read_count or write_count.
    fn shutdown(&self) {
        debug!("shutting down the storage manager ");
        drop(self);

        let cid_to_files = (*self.container_files).read().unwrap();
        let filepath = format!("{}/serde_containers", self.storage_path);
        fs::create_dir_all(&filepath);

        let mut serde_map: HashMap<String, String> = HashMap::new();

        for (id, heapfile) in cid_to_files.iter() {
            let mut name = "heapfile_".to_string();
            name.push_str(&id.to_string());
            let subpath = format!("{}/containers", self.storage_path);
            let filename = format!("{}/{}", subpath, name);
            serde_map.insert(id.to_string(), filename);
        }
        let serde_filename = format!("{}/{}", filepath, "all_containers".to_string());
        serde_json::to_writer(
            fs::File::create(&serde_filename).expect("error creating file"),
            &serde_map,
        )
        .expect("error deserializing storage manager");
    }

    fn import_csv(
        &self,
        table: &Table,
        path: String,
        _tid: TransactionId,
        container_id: ContainerId,
        _timestamp: LogicalTimeStamp,
    ) -> Result<(), CrustyError> {
        // Convert path into an absolute path.
        let path = fs::canonicalize(path)?;
        debug!("server::csv_utils trying to open file, path: {:?}", path);
        let file = fs::File::open(path)?;
        // Create csv reader.
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(file);

        // Iterate through csv records.
        let mut inserted_records = 0;
        for result in rdr.records() {
            #[allow(clippy::single_match)]
            match result {
                Ok(rec) => {
                    // Build tuple and infer types from schema.
                    let mut tuple = Tuple::new(Vec::new());
                    for (field, attr) in rec.iter().zip(table.schema.attributes()) {
                        // TODO: Type mismatch between attributes and record data>
                        match &attr.dtype() {
                            DataType::Int => {
                                let value: i32 = field.parse::<i32>().unwrap();
                                tuple.field_vals.push(Field::IntField(value));
                            }
                            DataType::String => {
                                let value: String = field.to_string().clone();
                                tuple.field_vals.push(Field::StringField(value));
                            }
                        }
                    }
                    //TODO: How should individual row insertion errors be handled?
                    debug!(
                        "server::csv_utils about to insert tuple into container_id: {:?}",
                        &container_id
                    );
                    self.insert_value(container_id, tuple.get_bytes(), _tid);
                    inserted_records += 1;
                }
                _ => {
                    // FIXME: get error from csv reader
                    error!("Could not read row from CSV");
                    return Err(CrustyError::IOError(
                        "Could not read row from CSV".to_string(),
                    ));
                }
            }
        }
        info!("Num records imported: {:?}", inserted_records);
        Ok(())
    }
}

/// Trait Impl for Drop
impl Drop for StorageManager {
    /// Shutdown the storage manager. Can be called by shutdown. Should be safe to call multiple times.
    /// If temp, this should remove all stored files.
    fn drop(&mut self) {
        if self.is_temp {
            debug!("Removing storage path on drop {}", self.storage_path);
            fs::remove_dir_all(self.storage_path.clone()).unwrap();
        }
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use crate::storage_manager::StorageManager;
    use common::storage_trait::StorageTrait;
    use common::testutil::*;

    #[test]
    fn hs_sm_a_insert() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);

        let bytes = get_random_byte_vec(40);
        let tid = TransactionId::new();

        let val1 = sm.insert_value(cid, bytes.clone(), tid);

        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val1.page_id.unwrap());
        assert_eq!(0, val1.slot_id.unwrap());

        let p1 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();

        let val2 = sm.insert_value(cid, bytes, tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val2.page_id.unwrap());
        assert_eq!(1, val2.slot_id.unwrap());

        let p2 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();
        assert_ne!(p1.get_bytes()[..], p2.get_bytes()[..]);
    }

    #[test]
    fn hs_sm_b_iter_small() {
        init();

        let sm = StorageManager::new_test_sm();

        let cid = 1;
        sm.create_table(cid);
        let tid = TransactionId::new();

        //Test one page
        let mut byte_vec: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec {
            sm.insert_value(cid, val.clone(), tid);
        }

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);

        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x);
        }

        // Should be on two pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }

        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x);
        }

        // Should be on 3 pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(300),
            get_random_byte_vec(500),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x);
        }
    }

    #[test]
    #[ignore]
    fn hs_sm_b_iter_large() {
        init();

        let sm = StorageManager::new_test_sm();
        let cid = 1;

        sm.create_table(cid).unwrap();
        let tid = TransactionId::new();

        let vals = get_random_vec_of_byte_vec(1000, 40, 400);
        sm.insert_values(cid, vals, tid);
        let mut count = 0;
        for _ in sm.get_iterator(cid, tid, Permissions::ReadOnly) {
            count += 1;
        }
        assert_eq!(1000, count);
    }
}
