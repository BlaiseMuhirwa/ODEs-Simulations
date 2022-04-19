use common::ids::{PageId, SlotId, ValueId};
use common::testutil::int_vec_to_tuple;
use common::PAGE_SIZE;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryInto;
use std::mem;

/// Header structure definition. The struct contains three fiels that holds
/// metadata for a page.
/// page_id: The unique identifier of a page
/// furthest_slot: This is the slot with the highest offset in the page
/// entries: This is a hashtable that maps slot ids to a vector of size 2. The
/// first entry is the offset and the second is byte size of the record in the page.

pub(crate) struct Header {
    pub page_id: PageId,
    pub number_of_slots: u16,
    pub furthest_slot: Option<SlotId>,
    pub max_slot_id: SlotId,
    pub entries: HashMap<SlotId, Vec<SlotId>>,
}

/// The struct for a page. Note this can hold more elements/meta data when created,
/// but it must be able to be packed/serialized/marshalled into the data array of size
/// PAGE_SIZE. In the header, you are allowed to allocate 8 bytes for general page metadata and
/// 6 bytes per value/entry/slot stored. For example a page that has stored 3 values, can use
/// up to 8+3*6=26 bytes, leaving the rest (PAGE_SIZE-26 for data) when serialized.
/// You do not need reclaim header information for a value inserted (eg 6 bytes per value ever inserted)
/// The rest must filled as much as possible to hold values.
pub(crate) struct Page {
    pub header: Header,
    /// The data for data
    pub data: [u8; PAGE_SIZE],
}

/// The functions required for page
impl Page {
    /// Create a new page
    /// Creates a new Header data structure containing page_id and
    /// the number of entries in the page
    pub fn new(page_id: PageId) -> Self {
        let header = Header {
            page_id: page_id,
            number_of_slots: 0,
            furthest_slot: None,
            max_slot_id: 0,
            entries: HashMap::new(),
        };
        let page = Page {
            header: header,
            data: [0; PAGE_SIZE],
        };
        return page;
    }

    /// Return the page id for a page
    pub fn get_page_id(&self) -> PageId {
        return self.header.page_id;
    }

    /// Computes the largest SlotId that the page currrently holds.
    /// Since slots are filled in an increasing order, this means that if
    /// the page is empty, then this should return slotid 0, and if the largest
    /// slot used is slot 5, then slot 5 should be returned.

    pub fn find_furthest_slot(&self) -> Option<SlotId> {
        return self.header.furthest_slot;
    }

    /// Attempts to add a new value to this page if there is space available.
    /// Returns Some(SlotId) if it was inserted or None if there was not enough space.
    /// Note that where the bytes are stored in the page does not matter (heap), but it
    /// should not change the slotId for any existing value. This means that
    /// bytes in the page may not follow the slot order.
    /// If a slot is deleted you should replace the slotId on the next insert.
    ///
    /// HINT: You can copy/clone bytes into a slice using the following function.
    /// They must have the same size.
    /// self.data[X..y].clone_from_slice(&bytes);
    ///

    pub fn add_value(&mut self, bytes: &[u8]) -> Option<SlotId> {
        let len = bytes.len();
        let first_free_slot: Option<SlotId> = self.get_first_free_space();
        let remaining_space: usize = self.get_largest_free_contiguous_space();
        if len + 6 >= remaining_space {
            return None;
        }
        if first_free_slot.is_some() {
            /* found a prior slot_id whose record has been deleted */
            let furthest_slot: SlotId = self.find_furthest_slot().unwrap();

            /* get offset */
            let id: SlotId = first_free_slot.unwrap();
            let mut record_ptr: &Vec<SlotId> = self.header.entries.get_mut(&furthest_slot).unwrap();
            let init_write_position: usize = ((*record_ptr)[0] + (*record_ptr)[1]).into();

            self.data[init_write_position..init_write_position + len].clone_from_slice(&bytes);
            /* update the metadata */
            let mut new_entry = Vec::new();
            new_entry.push(init_write_position as SlotId);
            new_entry.push(len as SlotId);
            self.header.entries.insert(id, new_entry);
            self.header.number_of_slots += 1;
            self.header.furthest_slot = Some(id);
            self.header.max_slot_id = self.header.max_slot_id.max(id);
            return Some(id);
        }
        /* no previous deleted slot is available */
        let furthest_slot: Option<SlotId> = self.find_furthest_slot();
        if furthest_slot.is_none() {
            self.data[0..len].clone_from_slice(&bytes);
            let mut new_entry: Vec<SlotId> = Vec::new();
            new_entry.push(0);
            new_entry.push(len as SlotId);
            self.header.entries.insert(0, new_entry);
            self.header.number_of_slots += 1;
            self.header.furthest_slot = Some(0);
            return Some(0);
        } else {
            /* page already has records */
            let current_slot_id: SlotId = self.header.number_of_slots;
            let mut record_ptr: &Vec<SlotId> = self
                .header
                .entries
                .get_mut(&(furthest_slot.unwrap()))
                .unwrap();
            let init_write_position: usize = ((*record_ptr)[0] + (*record_ptr)[1]).into();

            self.data[init_write_position..init_write_position + len].clone_from_slice(&bytes);

            let mut new_entry: Vec<SlotId> = Vec::new();
            new_entry.push(init_write_position as SlotId);
            new_entry.push(len as SlotId);
            self.header.entries.insert(current_slot_id, new_entry);
            self.header.number_of_slots += 1;
            self.header.furthest_slot = Some(current_slot_id);
            self.header.max_slot_id = self.header.max_slot_id.max(current_slot_id);
            return Some(current_slot_id);
        }
    }

    /// Return the bytes for the slotId. If the slotId is not valid then return None
    pub fn get_value(&self, slot_id: SlotId) -> Option<Vec<u8>> {
        if !self.header.entries.contains_key(&slot_id) {
            return None;
        }
        /* get byte offset*/
        let record_ptr = self.header.entries.get(&slot_id).unwrap();
        let offset: usize = (record_ptr)[0] as usize;
        /* get bytes needed */
        let bytes: usize = (record_ptr)[1] as usize;
        /* copy bytes from the page */
        let bytes_array: Vec<u8> = self.data[offset..offset + bytes].to_vec();

        return Some(bytes_array);
    }

    /// Delete the bytes/slot for the slotId. If the slotId is not valid then return None
    /// The slotId for a deleted slot should be assigned to the next added value
    /// The space for the value should be free to use for a later added value.
    /// HINT: Return Some(()) for a valid delete
    pub fn delete_value(&mut self, slot_id: SlotId) -> Option<()> {
        if !self.header.entries.contains_key(&slot_id) {
            return None;
        }
        let furthest_slot: SlotId = self.find_furthest_slot().unwrap();
        /* remove key from hashmap */
        let deleted_value: Vec<SlotId> = self.header.entries.remove(&slot_id).unwrap();

        /* set the byte stream for slot_it to zeros */
        let mut zeros_vec: Vec<u8> = Vec::new();
        for i in 0..deleted_value[1] {
            zeros_vec.push(0);
        }
        let offset: usize = deleted_value[0].into();
        let byte_size: usize = deleted_value[1].into();
        self.data[offset..offset + byte_size].clone_from_slice(&zeros_vec[..]);

        /* check if the deleted value's slot_id was the maximum
        if slot_id == self.header.max_slot_id {
            let mut prev_max_id: SlotId = 0;
            if self.header.entries.len() == 0 {
                self.header.max_slot_id = 0;
            } else {
                let temp_map = self.header.entries.clone();
                for (key, value) in temp_map.into_iter() {
                    prev_max_id = prev_max_id.max(key);
                }
            }
            self.header.max_slot_id = prev_max_id;
        }
        */
        /* check if the deleted value was the last value in the page */
        if slot_id == furthest_slot {
            self.header.number_of_slots -= 1;
            let mut previous_max_slot: SlotId = 0;
            let temp_map = self.header.entries.clone();
            for (index, (key, value)) in temp_map.into_iter().enumerate() {
                if index == 0 {
                    previous_max_slot = key;
                    continue;
                }
                let val = self.header.entries.get(&previous_max_slot).unwrap();
                if (*val)[0] < (*value)[0] {
                    previous_max_slot = key;
                }
            }
            self.header.furthest_slot = Some(previous_max_slot);
            return Some(());
        }

        /* otherwise, shift all values after deleted value */
        let rotation_start: usize = deleted_value[0].into();
        let furthest_slot_value = self.header.entries.get(&furthest_slot).unwrap();
        let rotation_end: usize = ((*furthest_slot_value)[0] + (*furthest_slot_value)[1]).into();
        self.data[rotation_start..rotation_end].rotate_left(deleted_value[1].into());

        for (key, value) in self.header.entries.iter_mut() {
            if (*value)[0] > deleted_value[0] {
                (*value)[0] -= deleted_value[1];
            }
        }
        /* udate page metadata */
        self.header.number_of_slots -= 1;

        Some(())
    }

    /// Gets first free space in the page. This is for determining prior delete
    /// positions in the page. Since slots are added as an increasing sequence,
    /// if the header's hashmap, for instance, contains SlotIds =[1,2,4,5], then
    /// this function should return position 3. This position can be used to map
    /// to new incoming records
    ///
    pub fn get_first_free_space(&self) -> Option<SlotId> {
        let len = self.header.entries.len() as u16;
        if len == 0 {
            return None;
        }
        let keys: Vec<&SlotId> = self.header.entries.keys().collect();
        let max = **keys.iter().max().unwrap();
        for i in 0..max {
            if !self.header.entries.contains_key(&i) {
                return Some(i as SlotId);
            }
        }
        return None;
    }

    /// Create a new page from the byte array.
    ///
    /// HINT to create a primitive data type from a slice you can use the following
    /// (the example is for a u16 type and the data store in little endian)
    /// u16::from_le_bytes(data[X..Y].try_into().unwrap());
    pub fn from_bytes(data: &[u8]) -> Self {
        let mut index = 2;
        let mut vec: Vec<u16> = Vec::new();
        /* get page_id, number of slots, furthest_slot, and max_slot_id */
        for i in 0..4 {
            let item: u16 = u16::from_le_bytes(
                data[PAGE_SIZE - index..PAGE_SIZE - index + 2]
                    .try_into()
                    .unwrap(),
            );
            vec.push(item);
            index += 2;
        }
        let mut num_entries: u16 = vec[1];
        /* get HashMap entries */
        let mut mappings: HashMap<SlotId, Vec<SlotId>> = HashMap::new();
        while num_entries > 0 {
            let mut temp: Vec<u16> = Vec::new();
            /* get slotId, offset and byte size */
            for i in 0..3 {
                let item: u16 = u16::from_le_bytes(
                    data[PAGE_SIZE - index..PAGE_SIZE - index + 2]
                        .try_into()
                        .unwrap(),
                );
                temp.push(item);
                index += 2;
            }
            let mut value: Vec<SlotId> = Vec::new();
            value.push(temp[1]); //append offset
            value.push(temp[2]); //append byte size
            mappings.insert(temp[0] as SlotId, value);
            num_entries -= 1;
        }
        let header = Header {
            page_id: vec[0] as PageId,
            number_of_slots: vec[1] as u16,
            furthest_slot: Some(vec[2] as SlotId),
            max_slot_id: vec[3],
            entries: mappings,
        };

        let mut data_array: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        let length = PAGE_SIZE;
        for i in 0..length {
            data_array[i] = u8::from_le_bytes(data[i..i + 1].try_into().unwrap());
        }
        let page = Page {
            header: header,
            data: data_array,
        };
        return page;
    }

    /// Convert a page into bytes. This must be same size as PAGE_SIZE.
    /// We use a Vec<u8> for simplicity here.
    ///
    /// HINT: To convert a vec of bytes using little endian, use
    /// to_le_bytes().to_vec()
    pub fn get_bytes(&self) -> Vec<u8> {
        let mut bytes_repr: Vec<u8> = Vec::new();
        let len = self.header.entries.len();
        for index in 0..PAGE_SIZE {
            let current_bytes = self.data[index].to_le_bytes().to_vec();
            bytes_repr.extend(&current_bytes);
        }
        let page_id: PageId = self.header.page_id;

        /* append page_id at the end of page */
        let page_id_bytes: [u8; 2] = page_id.to_le_bytes();
        bytes_repr[PAGE_SIZE - 1] = page_id_bytes[1];
        bytes_repr[PAGE_SIZE - 2] = page_id_bytes[0];

        /* append slot number at the end of the page */
        let entries_size: [u8; 2] = self.header.number_of_slots.to_le_bytes();
        bytes_repr[PAGE_SIZE - 3] = entries_size[1];
        bytes_repr[PAGE_SIZE - 4] = entries_size[0];

        /* append furthest_slot at the end of the page */
        let next_slot: [u8; 2] = self.header.furthest_slot.unwrap().to_le_bytes();
        bytes_repr[PAGE_SIZE - 5] = next_slot[1];
        bytes_repr[PAGE_SIZE - 6] = next_slot[0];

        /* append max_slot_id at the end of the page */
        let max_slot: [u8; 2] = self.header.max_slot_id.to_le_bytes();
        bytes_repr[PAGE_SIZE - 7] = max_slot[1];
        bytes_repr[PAGE_SIZE - 8] = max_slot[0];

        /* append entries with offset and byte size */
        let mut used_slots = self.header.entries.len();
        let mut current_idx = 9;
        let mapping = &self.header.entries;
        let mut temp_vec: Vec<&u16> = Vec::new();
        for key in self.header.entries.keys() {
            temp_vec.push(key);
        }
        //temp_vec.sort();

        for item in temp_vec.into_iter() {
            let key = item;
            let value = self.header.entries.get(key);

            /* append slotId bytes */
            let key_bytes: [u8; 2] = key.to_le_bytes();
            bytes_repr[PAGE_SIZE - current_idx] = key_bytes[1];
            current_idx += 1;
            bytes_repr[PAGE_SIZE - current_idx] = key_bytes[0];
            current_idx += 1;

            /* append offset bytes */
            let offset: [u8; 2] = self.header.entries.get(key).unwrap()[0].to_le_bytes();
            bytes_repr[PAGE_SIZE - current_idx] = offset[1];
            current_idx += 1;
            bytes_repr[PAGE_SIZE - current_idx] = offset[0];
            current_idx += 1;

            /* append byte size for current record */
            let byte_size: [u8; 2] = self.header.entries.get(key).unwrap()[1].to_le_bytes();
            bytes_repr[PAGE_SIZE - current_idx] = byte_size[1];
            current_idx += 1;
            bytes_repr[PAGE_SIZE - current_idx] = byte_size[0];
            current_idx += 1;
        }
        return bytes_repr;
    }

    /// A utility function to determine the size of the header in the page
    /// when serialized/to_bytes.
    /// Will be used by tests. Optional for you to use in your code
    #[allow(dead_code)]
    pub(crate) fn get_header_size(&self) -> usize {
        /* get length of the the HashMap in the header */
        let hashmap_size: usize = 6 * self.header.entries.len();
        let page_metadata_size: usize = 8;
        return hashmap_size + page_metadata_size;
    }

    /// A utility function to determine the largest block of free space in the page.
    /// Will be used by tests. Optional for you to use in your code
    #[allow(dead_code)]
    pub(crate) fn get_largest_free_contiguous_space(&self) -> usize {
        let mapping = &self.header.entries;
        let header_size = self.get_header_size();
        if self.header.entries.len() == 0 {
            return PAGE_SIZE - header_size;
        }
        let mut entries_size: usize = 0;
        let furthest_val = self
            .header
            .entries
            .get(&self.find_furthest_slot().unwrap())
            .unwrap();
        entries_size += (*furthest_val)[0] as usize + (*furthest_val)[1] as usize;

        return PAGE_SIZE - (header_size + entries_size);
    }
}

/// The (consuming) iterator struct for a page.
/// This should iterate through all valid values of the page.
/// See https://stackoverflow.com/questions/30218886/how-to-implement-iterator-and-intoiterator-for-a-simple-struct
pub struct PageIter {
    page: Page,
    sorted_vals: Vec<u16>,
    index: usize,
}

/// The implementation of the (consuming) page iterator.
impl Iterator for PageIter {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.sorted_vals.len() {
            let offset_and_bytes: &Vec<SlotId> = self
                .page
                .header
                .entries
                .get(&self.sorted_vals[self.index])
                .unwrap();
            let offset: usize = (*offset_and_bytes)[0] as usize;
            let bytes: usize = (*offset_and_bytes)[1] as usize;
            let value: Vec<u8> = self.page.data[offset..offset + bytes].to_vec();

            self.index += 1;
            return Some(value);
        }

        return None;
    }
}

/// The implementation of IntoIterator which allows an iterator to be created
/// for a page. This should create the PageIter struct with the appropriate state/metadata
/// on initialization.
impl IntoIterator for Page {
    type Item = Vec<u8>;
    type IntoIter = PageIter;

    fn into_iter(self) -> Self::IntoIter {
        let mut temp_vec: Vec<u16> = Vec::new();
        for key in self.header.entries.keys() {
            temp_vec.push(*key);
        }
        temp_vec.sort();
        PageIter {
            page: self,
            sorted_vals: temp_vec,
            index: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::testutil::init;
    use common::testutil::*;
    use common::Tuple;

    /// Limits how on how many bytes we can use for page metadata / header
    pub const FIXED_HEADER_SIZE: usize = 8;
    pub const HEADER_PER_VAL_SIZE: usize = 6;

    #[test]
    fn hs_page_get_value() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();

        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();

        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Recheck

        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));
    }

    #[test]
    fn hs_page_create() {
        init();
        let p = Page::new(0);

        assert_eq!(0, p.get_page_id());
        assert_eq!(
            PAGE_SIZE - p.get_header_size(),
            p.get_largest_free_contiguous_space()
        );
    }

    #[test]
    fn hs_page_simple_insert() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        let byte_len = tuple_bytes.len();

        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        assert_eq!(
            PAGE_SIZE - byte_len - p.get_header_size(),
            p.get_largest_free_contiguous_space()
        );

        let tuple_bytes2 = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        assert_eq!(
            PAGE_SIZE - p.get_header_size() - byte_len - byte_len,
            p.get_largest_free_contiguous_space()
        );
    }

    #[test]
    fn hs_page_space() {
        init();
        let mut p = Page::new(0);
        let size = 10;
        let bytes = get_random_byte_vec(size);
        assert_eq!(10, bytes.len());
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_largest_free_contiguous_space()
        );
    }

    #[test]
    fn hs_page_header_size_small() {
        init();
        // Testing that the header is no more than 8 bytes for the header,
        // and 6 bytes per value inserted
        let mut p = Page::new(0);
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);

        let bytes = get_random_byte_vec(10);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE);
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(Some(3), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE * 4);
    }

    #[test]
    fn hs_page_header_size_full() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let byte_size = 10;
        let bytes = get_random_byte_vec(byte_size);
        // how many vals can we hold with 8 bytes
        let num_vals: usize = (((PAGE_SIZE - FIXED_HEADER_SIZE) as f64
            / (byte_size + HEADER_PER_VAL_SIZE) as f64)
            .floor()) as usize;
        if PAGE_SIZE == 4096 && FIXED_HEADER_SIZE == 8 && HEADER_PER_VAL_SIZE == 6 {
            assert_eq!(255, num_vals);
        }
        for _ in 0..num_vals {
            p.add_value(&bytes);
        }
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + (num_vals * HEADER_PER_VAL_SIZE));
        assert!(
            p.get_largest_free_contiguous_space()
                >= PAGE_SIZE
                    - (byte_size * num_vals)
                    - FIXED_HEADER_SIZE
                    - (num_vals * HEADER_PER_VAL_SIZE)
        );
    }

    #[test]
    fn hs_page_no_space() {
        init();
        let mut p = Page::new(0);
        let size = PAGE_SIZE / 4;
        let bytes = get_random_byte_vec(size);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size,
            p.get_largest_free_contiguous_space()
        );

        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_largest_free_contiguous_space()
        );

        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_largest_free_contiguous_space()
        );

        //Should reject here
        assert_eq!(None, p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_largest_free_contiguous_space()
        );

        // Take small amount of data
        let small_bytes = get_random_byte_vec(size / 4);
        assert_eq!(Some(3), p.add_value(&small_bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3 - small_bytes.len(),
            p.get_largest_free_contiguous_space()
        );
    }

    #[test]
    fn hs_page_simple_delete() {
        init();
        let mut p = Page::new(0);

        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        //Delete slot 0
        assert_eq!(Some(()), p.delete_value(0));
        //Recheck slot 1

        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);

        assert_eq!(tuple2, check_tuple2);
        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(0));
        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));
        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));
        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(1));
    }

    #[test]
    fn hs_page_get_first_free_space() {
        init();
        let _p = Page::new(0);

        let _b1 = get_random_byte_vec(100);
        let _b2 = get_random_byte_vec(50);
    }

    #[test]
    fn hs_page_delete_insert() {
        init();
        let mut p = Page::new(0);
        let tuple_bytes = get_random_byte_vec(20);
        let tuple_bytes2 = get_random_byte_vec(20);
        let tuple_bytes3 = get_random_byte_vec(20);
        let tuple_bytes4 = get_random_byte_vec(20);
        let tuple_bytes_big = get_random_byte_vec(40);
        let tuple_bytes_small1 = get_random_byte_vec(5);
        let tuple_bytes_small2 = get_random_byte_vec(5);

        //Add 3 values

        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();

        assert_eq!(tuple_bytes, check_bytes);

        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes2, check_bytes);
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));
        //Verify slot 1 is gone
        assert_eq!(None, p.get_value(1));

        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        //Insert same bytes, should go to slot 1
        assert_eq!(Some(1), p.add_value(&tuple_bytes4));

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes4, check_bytes);
        //Delete 0
        assert_eq!(Some(()), p.delete_value(0));
        //Insert big, should go to slot 0 with space later in free block
        assert_eq!(Some(0), p.add_value(&tuple_bytes_big));

        //Insert small, should go to 3
        assert_eq!(Some(3), p.add_value(&tuple_bytes_small1));

        //Insert small, should go to new
        assert_eq!(Some(4), p.add_value(&tuple_bytes_small2));
    }

    #[test]
    fn hs_page_size() {
        init();
        let mut p = Page::new(2);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let page_bytes = p.get_bytes();
        assert_eq!(PAGE_SIZE, page_bytes.len());
    }

    #[test]
    fn hs_page_simple_byte_serialize() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        //Get bytes and create from bytes
        let bytes = p.get_bytes();
        let mut p2 = Page::from_bytes(&bytes);
        assert_eq!(0, p2.get_page_id());

        //Check reads
        let check_bytes2 = p2.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        let check_bytes = p2.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        //Add a new tuple to the new page
        let tuple3 = int_vec_to_tuple(vec![4, 3, 2]);
        let tuple_bytes3 = tuple3.get_bytes();
        assert_eq!(Some(2), p2.add_value(&tuple_bytes3));
        assert_eq!(tuple_bytes3, p2.get_value(2).unwrap());
        assert_eq!(tuple_bytes2, p2.get_value(1).unwrap());
        assert_eq!(tuple_bytes, p2.get_value(0).unwrap());
    }

    #[test]
    fn hs_page_iter() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 0, 1]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let tuple2 = int_vec_to_tuple(vec![0, 0, 2]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let tuple3 = int_vec_to_tuple(vec![0, 0, 3]);
        let tuple_bytes3 = serde_cbor::to_vec(&tuple3).unwrap();
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let tuple4 = int_vec_to_tuple(vec![0, 0, 4]);
        let tuple_bytes4 = serde_cbor::to_vec(&tuple4).unwrap();
        assert_eq!(Some(3), p.add_value(&tuple_bytes4));

        let tup_vec = vec![
            tuple_bytes.clone(),
            tuple_bytes2.clone(),
            tuple_bytes3.clone(),
            tuple_bytes4.clone(),
        ];
        let page_bytes = p.get_bytes();

        // Test iteration 1
        let mut iter = p.into_iter();
        assert_eq!(Some(tuple_bytes.clone()), iter.next());

        assert_eq!(Some(tuple_bytes2.clone()), iter.next());
        assert_eq!(Some(tuple_bytes3.clone()), iter.next());
        assert_eq!(Some(tuple_bytes4.clone()), iter.next());
        assert_eq!(None, iter.next());

        //Check another way
        let p = Page::from_bytes(&page_bytes);
        assert_eq!(Some(tuple_bytes.clone()), p.get_value(0));

        for (i, x) in p.into_iter().enumerate() {
            assert_eq!(tup_vec[i], x);
        }

        let p = Page::from_bytes(&page_bytes);
        let mut count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 4);

        //Add a value and check
        let mut p = Page::from_bytes(&page_bytes);
        assert_eq!(Some(4), p.add_value(&tuple_bytes));
        //get the updated bytes

        let page_bytes = p.get_bytes();
        count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 5);

        //Delete
        let mut p = Page::from_bytes(&page_bytes);
        p.delete_value(2);
        let mut iter = p.into_iter();
        assert_eq!(Some(tuple_bytes.clone()), iter.next());
        assert_eq!(Some(tuple_bytes2.clone()), iter.next());

        assert_eq!(Some(tuple_bytes4.clone()), iter.next());

        assert_eq!(Some(tuple_bytes.clone()), iter.next());

        assert_eq!(None, iter.next());
    }
}
