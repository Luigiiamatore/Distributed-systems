use std::collections::HashMap;
use std::io::SeekFrom;
use std::mem::size_of;
use std::path::PathBuf;
use sha2::Digest;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

#[derive(Eq, Hash, PartialEq)]
struct Key {
    key:  [u8; 255]
}

impl Key {
    fn from_slice(key: &[u8]) -> Result<Key, String> {
        if key.len() > 255 {
            return Err("Key is longer than 255".to_string());
        }

        let mut k = [0u8; 255];
        k[..key.len()].copy_from_slice(key);
        Ok(Self { key: k })
    }

    async fn parse_file(file: &mut File) -> Result<Self, String> {
        let mut key_buf = [0u8; size_of::<Key>()];
        if file.read(&mut key_buf).await.unwrap() == 0 {
            return Err("EOF reached, couldn't read more bytes".to_string());
        }

        Self::from_slice(&key_buf)
    }

    fn buf(&self) -> Vec<u8> {
        self.key.to_vec()
    }
}

struct Value {
    value: Vec<u8>,
    size: u16
}

impl Value {
    fn byte_size(&self) -> usize {
        size_of::<u16>() + self.size as usize
    }

    fn from_slice(slice: &[u8]) -> Result<Self, String> {
        if slice.len() > u16::MAX as usize {
            return Err("Value has size greater than 65535 bytes".to_string());
        }

        Ok(Self { 
            value: slice.to_vec(), 
            size: slice.len() as u16 
        })
    }

    fn parse_buf(buf: &[u8]) -> Self {
        let size = u16::from_be_bytes([buf[0], buf[1]]);
        let value = buf[2..2 + size as usize].to_vec();
        Self { value, size }
    }

    async fn parse_file(file: &mut File) -> Result<Value, String> {
        let size = file.read_u16().await.map_err(|_| "Failed to read size")?;
        let mut value = vec![0; size as usize];
        file.read_exact(&mut value).await.map_err(|_| "Failed to read value")?;

        Ok(Self { value, size })
    }

    fn buf(&self) -> Vec<u8> {
        [self.size.to_be_bytes().as_slice(), &self.value].concat()
    }
}

struct Record {
    key: Key,
    value: Value,
    flag: bool
}

impl Record {
    fn size(&self) -> usize {
        size_of::<Key>() + self.value.byte_size() + size_of::<bool>()
    }

    fn from_key_value(key: &str, value: &[u8]) -> Result<Self, String> {
        Ok(Self {
            key:  Key::from_slice(key.as_bytes())?,
            value: Value::from_slice(value)?,
            flag: true
        })
    }

    pub fn parse_buf(buf: &[u8]) -> Self {
        let key = Key::from_slice(&buf[..size_of::<Key>()]).unwrap();
        let value_offset = size_of::<Key>();
        let value = Value::parse_buf(&buf[value_offset..]);
        let flag = buf[value_offset + value.byte_size()] > 0;
        Self { key, value, flag }
    }

    async fn parse_file(file: &mut File) -> Result<Record, String> {
        let key = Key::parse_file(file).await?;
        let value = Value::parse_file(file).await?;
        let flag = file.read_u8().await.map_err(|_| "Failed to read flag")? > 0;

        Ok(Self { key, value, flag })
    }

    pub fn buf(&self) -> Vec<u8> {
        [self.key.buf(), self.value.buf(), vec![self.flag as u8]].concat()
    }
}

struct TmpFile {
    dir_path: PathBuf,
    path: PathBuf
}

impl TmpFile {
    fn get_digest(buffer: &[u8]) -> Vec<u8> {
        sha2::Sha256::digest(buffer).to_vec()
    }

    fn exists(&self) -> bool {
        self.path.exists()
    }

    async fn write(&self, buffer: &[u8]) {
        let dir = File::open(&self.dir_path).await.unwrap();
        let mut tmp_file = File::create(&self.path).await.unwrap();

        let digest = Self::get_digest(buffer);
        let content = [&digest, buffer].concat();

        tmp_file.write_all(&content).await.unwrap();
        tmp_file.sync_data().await.unwrap();
        dir.sync_data().await.unwrap();
    }

    async fn read(&self) -> Result<Record, String> {
        let mut tmp_file = File::open(&self.path).await.unwrap();

        let mut checksum = vec![0u8; 32];
        tmp_file.read_exact(&mut checksum).await.unwrap();

        let mut buf = Vec::new();
        tmp_file.read_to_end(&mut buf).await.unwrap();

        if Self::get_digest(&buf) != checksum {
            return Err("Checksums don't match".to_string());
        }

        Ok(Record::parse_buf(&buf))
    }

    async fn delete(&self) {
        let dir = File::open(&self.dir_path).await.unwrap();
        tokio::fs::remove_file(&self.path).await.unwrap();
        dir.sync_data().await.unwrap();
    }
}

struct DstFile {
    dir_path: PathBuf,
    path: PathBuf
}

impl DstFile {
    async fn create(&self) -> bool {
        if self.path.exists() {
            return false;
        }

        let dir = File::open(&self.dir_path).await.unwrap();
        let file = File::create(&self.path).await.unwrap();
        file.sync_data().await.unwrap();
        dir.sync_data().await.unwrap();

        true
    }

    async fn write(&self, content: &[u8]) {
        let mut file = File::options().append(true).open(&self.path).await.unwrap();
        file.write_all(content).await.unwrap();
        file.sync_data().await.unwrap();
    }

    async fn read_record(&self, index: usize) -> Result<Record, String> {
        let mut file = File::open(&self.path).await.unwrap();
        file.seek(SeekFrom::Start(index as u64)).await.unwrap();
        Record::parse_file(&mut file).await
    }
}

struct CustomStorage {
    tmp_file: TmpFile,
    dst_file: DstFile,
    key_index: HashMap<Key, usize>,
    eof_index: usize
}

impl CustomStorage {
    async fn stable_store(&mut self, record: &Record) -> Result<(), String> {
        self.tmp_file.write(&record.buf()).await;
        let tmp_record = self.tmp_file.read().await?;
        self.dst_file.write(&tmp_record.buf()).await;
        self.tmp_file.delete().await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl StableStorage for CustomStorage {
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String> {
        let record = Record::from_key_value(key, value)?;

        self.stable_store(&record).await?;

        let index = self.eof_index;
        self.eof_index += record.size();
        self.key_index.insert(record.key, index);

        Ok(())
    }

    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        let key = Key::from_slice(key.as_bytes());
        if key.is_err() { return None; }

        let key = key.unwrap();
        let index = self.key_index.get(&key)?;

        let record = self.dst_file.read_record(*index).await.ok()?;
        Some(record.value.value)
    }

    async fn remove(&mut self, key: &str) -> bool {
        let key = Key::from_slice(key.as_bytes());
        if key.is_err() { return false; }

        let key = key.unwrap();
        if !self.key_index.contains_key(&key) { return false; }

        let record = Record {
            key,
            value: Value { value: vec![0], size: 1 },
            flag: false
        };

        if self.stable_store(&record).await.is_err() { return false; }

        self.key_index.remove(&record.key);
        self.eof_index += record.size();

        true
    }
}

#[async_trait::async_trait]
pub trait StableStorage: Send + Sync {
    /// Stores `value` under `key`.
    ///
    /// Detailed requirements are specified in the description of the assignment.
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String>;

    /// Retrieves value stored under `key`.
    ///
    /// Detailed requirements are specified in the description of the assignment.
    async fn get(&self, key: &str) -> Option<Vec<u8>>;

    /// Removes `key` and the value stored under it.
    ///
    /// Detailed requirements are specified in the description of the assignment.
    async fn remove(&mut self, key: &str) -> bool;
}

/// Creates a new instance of stable storage.
pub async fn build_stable_storage(root_storage_dir: PathBuf) -> Box<dyn StableStorage> {
    let dst_file = DstFile {
        dir_path: root_storage_dir.clone(),
        path: root_storage_dir.join("dstfile.txt")
    };

    let tmp_file = TmpFile {
        dir_path: root_storage_dir.clone(),
        path: root_storage_dir.join("tmpfile.txt")
    };

    let mut key_index = HashMap::new();
    let mut eof_index = 0;

    if !dst_file.create().await {
        while let Ok(record) = dst_file.read_record(eof_index).await {
            let size = record.size();

            if record.flag {
                key_index.insert(record.key, eof_index);
            } else {
                key_index.remove(&record.key);
            }

            eof_index += size;
        }
    }

    if tmp_file.exists() {
        if let Ok(record) = tmp_file.read().await {
            dst_file.write(&record.buf()).await;

            let index = eof_index;
            eof_index += record.size();
            key_index.insert(record.key, index);
        }

        tmp_file.delete().await;
    }

    Box::new(CustomStorage {
        dst_file,
        tmp_file,
        key_index,
        eof_index
    })
}