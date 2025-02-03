use crate::sectors_manager::SectorsManager;
use crate::{SectorIdx, SectorVec};
use sha2::Digest;
use std::io::SeekFrom;
use std::path::PathBuf;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Semaphore, SemaphorePermit};

const SEC_SIZE: usize = 4096;
const MAX_FILE: usize = 1024;

pub struct FileSectorsManager {
    base_dir: PathBuf,
    semaphore: Semaphore,
}

impl FileSectorsManager {
    pub async fn init(base_dir: PathBuf) -> Self {
        let manager = Self {
            base_dir,
            semaphore: Semaphore::new(MAX_FILE),
        };
        manager.recover_state().await;
        manager
    }

    fn sector_path(&self, sector_idx: u64) -> PathBuf {
        self.base_dir.join(sector_idx.to_string())
    }

    async fn acquire_permission(&self) -> SemaphorePermit {
        self.semaphore.acquire().await.unwrap()
    }

    async fn recover_state(&self) {
        if let Ok(mut entries) = fs::read_dir(&self.base_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Ok(file_type) = entry.file_type().await {
                    if file_type.is_file() {
                        let path = entry.path();
                        let file_name = path.file_name().unwrap().to_str().unwrap();

                        if file_name.starts_with("tmp_") {
                            let actual_file_name = &file_name[4..];
                            if let Ok(content) = read_and_verify_content(&path).await {
                                let dst_path = self.base_dir.join(actual_file_name);
                                let _permit = self.acquire_permission().await;
                                write_file(&dst_path, &content).await;
                            }

                            fs::remove_file(&path)
                                .await
                                .expect(&format!("Unable to remove {}", path.display()));
                        }
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl SectorsManager for FileSectorsManager {
    async fn read_data(&self, idx: SectorIdx) -> SectorVec {
        let path = self.sector_path(idx);

        if !fs::try_exists(&path).await.unwrap_or(false) {
            return SectorVec(vec![0; SEC_SIZE]);
        }

        let _permit = self.acquire_permission().await; // Acquisisce il permesso
        let mut file = fs::File::open(&path)
            .await
            .expect(&format!("Unable to open {}", path.display()));
        let mut buffer = vec![0; SEC_SIZE];
        file.read_exact(&mut buffer)
            .await
            .expect(&format!("Unable to read {}", path.display()));

        SectorVec(buffer)
    }

    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8) {
        let path = self.sector_path(idx);

        if !fs::try_exists(&path).await.unwrap_or(false) {
            return (0, 0);
        }

        let _permit = self.acquire_permission().await;
        let mut file = fs::File::open(&path)
            .await
            .expect(&format!("Unable to open {}", path.display()));
        file.seek(SeekFrom::Start(SEC_SIZE as u64))
            .await
            .expect(&format!("Unable to seek in {}", path.display()));

        let ts = file.read_u64().await.unwrap();
        let wr = file.read_u8().await.unwrap();

        (ts, wr)
    }

    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {
        let dst_path = self.sector_path(idx);
        let temp_path = self.base_dir.join(format!("tmp_{}", idx.to_string()));

        let content = [
            sector.0 .0.as_slice(),
            &sector.1.to_be_bytes(),
            &sector.2.to_be_bytes(),
        ]
            .concat();

        let tmp_content = [
            sha2::Sha256::digest(&content).as_slice(),
            content.as_slice(),
        ]
            .concat();

        let _permit = self.acquire_permission().await;
        write_file(&temp_path, &tmp_content).await;
        write_file(&dst_path, &content).await;

        fs::remove_file(&temp_path)
            .await
            .expect(&format!("Unable to remove {}", temp_path.display()));

        let _permit = self.acquire_permission().await;
        let dir = fs::File::open(self.base_dir.clone())
            .await
            .expect(&"Failed to open parent directory".to_string());
        dir.sync_data()
            .await
            .expect("Failed to sync parent directory");
    }
}

async fn read_and_verify_content(file_path: &PathBuf) -> Result<[u8; SEC_SIZE + 8 + 1], String> {
    let mut file = fs::File::open(file_path)
        .await
        .map_err(|e| format!("Failed to open file {:?}: {}", file_path, e))?;

    let mut hash = [0u8; 32];
    file.read_exact(&mut hash)
        .await
        .map_err(|e| format!("Failed to read hash from {:?}: {}", file_path, e))?;

    let mut content = [0u8; SEC_SIZE + 8 + 1];
    file.read_exact(&mut content)
        .await
        .map_err(|e| format!("Failed to read content from {:?}: {}", file_path, e))?;

    let computed_hash = sha2::Sha256::digest(&content);
    if hash != computed_hash.as_slice() {
        return Err(format!(
            "Hash mismatch for {:?}: expected {:?}, got {:?}",
            file_path, hash, computed_hash
        ));
    }

    Ok(content)
}

async fn write_file(path: &PathBuf, content: &[u8]) {
    let mut file = fs::File::create(path)
        .await
        .expect(&format!("Unable to open {}", path.display()));
    file.write_all(content)
        .await
        .expect(&format!("Unable to write to file {}", path.display()));
    file.flush()
        .await
        .expect(&format!("Unable to flush file {}", path.display()));
    file.sync_data()
        .await
        .expect(&format!("Unable to sync file {}", path.display()));
}
