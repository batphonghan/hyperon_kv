use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
};

pub struct HyperonStore {
    file: File,
    index: HashMap<ByteString, u64>,
}

type ByteString = Vec<u8>;
type ByteStr = [u8];

struct KeyPair {
    key: ByteString,
    value: ByteString,
}

impl HyperonStore {
    pub fn open(path: &str) -> std::io::Result<Self> {
        let path = Path::new(path);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;
        let index = HashMap::new();
        let mut store = HyperonStore { file, index };
        store.load_indexes()?;

        Ok(store)
    }

    // Insert to file and update index
    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> std::io::Result<()> {
        let current_position = self.insert_ignore_index(key, value)?;
        self.index.insert(key.to_vec(), current_position);

        Ok(())
    }

    // Insert to file then return index
    fn insert_ignore_index(&mut self, key: &ByteStr, value: &ByteStr) -> std::io::Result<u64> {
        let key_len = key.len();
        let value_len = value.len();
        let mut tmp = ByteString::with_capacity(key_len + value_len);
        for b in key {
            tmp.push(*b);
        }
        for b in value {
            tmp.push(*b);
        }
        let mut hasher = Hasher::new();
        hasher.update(&tmp);
        let checksum = hasher.finalize();

        let mut f = BufWriter::new(&mut self.file);

        f.seek(SeekFrom::End(0))?;
        let current_position = f.seek(SeekFrom::Current(0))?;
        f.write_u32::<LittleEndian>(checksum)?;
        f.write_u32::<LittleEndian>(key_len as u32)?;
        f.write_u32::<LittleEndian>(value_len as u32)?;
        f.write_all(&tmp)?;

        Ok(current_position)
    }

    pub fn get(&mut self, key: &ByteStr) -> std::io::Result<Option<ByteString>> {
        let position = match self.index.get(key) {
            Some(position) => *position,
            None => return Ok(None),
        };

        let s = self.file.seek(SeekFrom::Start(position))?;
        println!("{:?}", s);
        let mut f = BufReader::new(&mut self.file);
        let kv = HyperonStore::process_record(&mut f)?;

        Ok(Some(kv.value))
    }

    fn load_indexes(&mut self) -> std::io::Result<()> {
        loop {
            let current_position = self.file.seek(SeekFrom::Current(0))?;
            let mut f = BufReader::new(&mut self.file);
            let maybe_kv = HyperonStore::process_record(&mut f);
            let kv = match maybe_kv {
                Ok(_kv) => _kv,
                Err(e) => match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => break,
                    _ => return Err(e),
                },
            };
            self.index.insert(kv.key, current_position);
        }
        Ok(())
    }

    pub fn update(&mut self, key: &ByteStr, value: &ByteStr) -> std::io::Result<()> {
        let position = self.insert_ignore_index(key, value)?;
        self.index.insert(key.to_vec(), position);
        Ok(())
    }

    pub fn delete(&mut self, key: &ByteStr) -> std::io::Result<()> {
        let _ = self.insert_ignore_index(key, &[])?;
        self.index.remove(key);
        Ok(())
    }

    fn process_record<T: Read>(r: &mut T) -> std::io::Result<KeyPair> {
        let saved_check_sum = r.read_u32::<LittleEndian>()?;
        let key_len = r.read_u32::<LittleEndian>()?;
        let value_len = r.read_u32::<LittleEndian>()?;

        let data_len = (key_len + value_len) as usize;
        let mut data: Vec<u8> = vec![0u8; data_len];

        r.read_exact(&mut data)?;
        let mut hasher = Hasher::new();
        hasher.update(&data);
        let validate_check = hasher.finalize();
        if saved_check_sum != validate_check {
            panic!(
                "data corruption encountered ({:08x} != {:08x} )",
                validate_check, saved_check_sum,
            );
        }

        let value = data.split_off(key_len as usize);
        let key = data;

        Ok(KeyPair { key, value })
    }
}
