// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::BytesMut;
use filetime::{set_file_atime, FileTime};
use futures::stream::StreamExt;
use rand::{thread_rng, Rng};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom, Take};
use tokio::task::spawn_blocking;
use tokio_stream::wrappers::ReadDirStream;

use buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use common::{log, DigestInfo};
use config;
use error::{make_err, make_input_err, Code, Error, ResultExt};
use evicting_map::{EvictingMap, LenEntry};
use traits::{StoreTrait, UploadSizeInfo};

// Default size to allocate memory of the buffer when reading files.
const DEFAULT_BUFF_SIZE: usize = 32 * 1024;

struct FileEntry {
    digest: DigestInfo,
    file_size: u64,
    temp_path: Arc<String>,
    content_path: Arc<String>,
    // Will be the name of the file in the temp_path if it is flagged for deletion.
    pending_delete_file_name: AtomicU64,
}

impl FileEntry {
    async fn read_file_part(&self, offset: u64, length: u64) -> Result<Take<File>, Error> {
        let full_content_path = to_full_path_from_digest(&self.content_path, &self.digest);
        let mut file = fs::File::open(&full_content_path)
            .await
            .err_tip(|| format!("Failed to open file in filesystem store {}", full_content_path))?;

        file.seek(SeekFrom::Start(offset))
            .await
            .err_tip(|| format!("Failed to seek file: {}", full_content_path))?;
        Ok(file.take(length))
    }
}

#[async_trait]
impl LenEntry for FileEntry {
    #[inline]
    fn len(&self) -> usize {
        self.file_size as usize
    }

    #[inline]
    async fn touch(&self) {
        let full_content_path = to_full_path_from_digest(&self.content_path, &self.digest);
        let res = match spawn_blocking(move || set_file_atime(&full_content_path, FileTime::now())).await {
            Ok(res) => res.err_tip(|| "Failed to touch file in filesystem store"),
            Err(_) => Err(make_err!(Code::Internal, "Failed to change atime of file")),
        };
        if let Err(err) = res {
            log::error!("{:?}", err);
        }
    }

    #[inline]
    async fn unref(&self) {
        self.pending_delete_file_name
            .store(thread_rng().gen::<u64>(), Ordering::Relaxed);
        let from_path = to_full_path_from_digest(&self.content_path, &self.digest);
        let to_path = to_full_path(&self.temp_path, &self.temp_path.to_string());
        // It is possible (although extremely unlikely) that another thread is reading
        // this file while we want to delete it here. To prevent errors in either case
        // we rename the file (since that other thread would have an open file handle)
        // to the temp folder then delete it when the Arc reference is dropped.
        if let Err(err) = fs::rename(&from_path, &to_path).await {
            log::warn!("Failed to rename file from {} to {} : {:?}", from_path, to_path, err);
        }
    }
}

impl Drop for FileEntry {
    fn drop(&mut self) {
        // If the file was flagged to be deleted (ie: only if unref() was called) delete
        // the file, but in another spawn. This will ensure we don't delete the files
        // on safe shutdown as well as not block this thread while we wait on an OS
        // blocking call.
        let pending_delete_file_name = self.pending_delete_file_name.load(Ordering::Relaxed);
        if pending_delete_file_name == 0 {
            return;
        }
        let full_temp_path = to_full_path(&self.temp_path, &pending_delete_file_name.to_string());
        tokio::spawn(async move {
            if let Err(err) = fs::remove_file(&full_temp_path).await {
                log::info!(
                    "\x1b[0;31mFilesystem Store\x1b[0m: Failed to remove file {} {:?}",
                    full_temp_path,
                    err
                );
            }
        });
    }
}

#[inline]
fn to_full_path(folder: &str, name: &str) -> String {
    format!("{}/{}", folder, name)
}

#[inline]
fn to_full_path_from_digest(folder: &str, digest: &DigestInfo) -> String {
    format!("{}/{}-{}", folder, digest.str(), digest.size_bytes)
}

async fn add_files_to_cache(
    evicting_map: &EvictingMap<Arc<FileEntry>, SystemTime>,
    anchor_time: &SystemTime,
    temp_path: &Arc<String>,
    content_path: &Arc<String>,
) -> Result<(), Error> {
    fn make_digest(file_name: &str) -> Result<DigestInfo, Error> {
        let (hash, size) = file_name.split_once('-').err_tip(|| "")?;
        let size = i64::from_str_radix(size, 10)?;
        DigestInfo::try_new(hash, size)
    }

    async fn process_entry(
        evicting_map: &EvictingMap<Arc<FileEntry>, SystemTime>,
        file_name: &str,
        dir_entry: &fs::DirEntry,
        anchor_time: &SystemTime,
        temp_path: &Arc<String>,
        content_path: &Arc<String>,
    ) -> Result<(), Error> {
        let digest = make_digest(&file_name)?;

        let metadata = dir_entry.metadata().await.err_tip(|| "Failed to get metadata")?;
        let atime = match metadata.accessed() {
            Ok(atime) => atime,
            Err(err) => {
                panic!(
                    "{}{}{} : {} {:?}",
                    "It appears this filesystem does not support access time. ",
                    "Please configure this program to run on a drive that supports ",
                    "atime",
                    file_name,
                    err
                );
            }
        };

        let file_entry = FileEntry {
            digest: digest.clone(),
            file_size: metadata.len(),
            temp_path: temp_path.clone(),
            content_path: content_path.clone(),
            pending_delete_file_name: AtomicU64::new(0),
        };
        let time_since_anchor = anchor_time
            .duration_since(atime.clone())
            .map_err(|_| make_input_err!("File access time newer than now"))?;
        evicting_map
            .insert_with_time(digest, Arc::new(file_entry), time_since_anchor.as_secs() as i32)
            .await;
        Ok(())
    }

    let dir_handle = fs::read_dir(format!("{}/", content_path))
        .await
        .err_tip(|| "Failed opening content directory for iterating in filesystem store")?;

    let mut read_dir_stream = ReadDirStream::new(dir_handle);
    while let Some(dir_entry) = read_dir_stream.next().await {
        let dir_entry = dir_entry.unwrap();
        let file_name = dir_entry.file_name().into_string().unwrap();
        let result = process_entry(
            &evicting_map,
            &file_name,
            &dir_entry,
            &anchor_time,
            &temp_path,
            &content_path,
        )
        .await;
        if let Err(err) = result {
            log::warn!(
                "Could not add file to eviction cache, so deleting: {} - {:?}",
                file_name,
                err
            );
            let _ = fs::remove_file(dir_entry.path()).await; // Ignore result.
        }
    }
    Ok(())
}

async fn prune_temp_path(temp_path: &str) -> Result<(), Error> {
    let dir_handle = fs::read_dir(temp_path)
        .await
        .err_tip(|| "Failed opening temp directory to prune partial downloads in filesystem store")?;

    let mut read_dir_stream = ReadDirStream::new(dir_handle);
    while let Some(dir_entry) = read_dir_stream.next().await {
        let path = dir_entry?.path();
        if let Err(err) = fs::remove_file(&path).await {
            log::warn!("Failed to delete file in filesystem store {:?} : {:?}", &path, err);
        }
    }
    Ok(())
}

pub struct FilesystemStore {
    temp_path: Arc<String>,
    content_path: Arc<String>,
    evicting_map: EvictingMap<Arc<FileEntry>, SystemTime>,
}

impl FilesystemStore {
    pub async fn new(config: &config::backends::FilesystemStore) -> Result<Self, Error> {
        let now = SystemTime::now();

        let empty_policy = config::backends::EvictionPolicy::default();
        let eviction_policy = config.eviction_policy.as_ref().unwrap_or(&empty_policy);
        let evicting_map = EvictingMap::new(eviction_policy, now);

        fs::create_dir_all(&config.temp_path)
            .await
            .err_tip(|| format!("Failed to temp directory {:?}", &config.temp_path))?;
        fs::create_dir_all(&config.content_path)
            .await
            .err_tip(|| format!("Failed to content directory {:?}", &config.content_path))?;

        let temp_path = Arc::new(config.temp_path.clone());
        let content_path = Arc::new(config.content_path.clone());
        add_files_to_cache(&evicting_map, &now, &temp_path, &content_path).await?;
        prune_temp_path(&temp_path.as_ref()).await?;

        let store = Self {
            temp_path: Arc::new(config.temp_path.clone()),
            content_path: Arc::new(config.content_path.clone()),
            evicting_map,
        };
        Ok(store)
    }

    pub fn get_file_for_digest(&self, digest: &DigestInfo) -> String {
        to_full_path_from_digest(self.content_path.as_ref(), &digest)
    }

    async fn update_file(
        self: Pin<&Self>,
        file_path: &str,
        file: &mut File,
        digest: DigestInfo,
        mut reader: DropCloserReadHalf,
    ) -> Result<(), Error> {
        let mut file_size = 0;
        loop {
            let mut data = reader
                .recv()
                .await
                .err_tip(|| "Failed to receive data in filesystem store")?;
            let data_len = data.len();
            if data_len == 0 {
                break; // EOF.
            }
            file.write_all_buf(&mut data)
                .await
                .err_tip(|| format!("Failed to write data into filesystem store {}", file_path))?;
            file_size += data_len as u64;
        }

        let entry = Arc::new(FileEntry {
            digest: digest.clone(),
            file_size,
            temp_path: self.temp_path.clone(),
            content_path: self.content_path.clone(),
            pending_delete_file_name: AtomicU64::new(0),
        });

        let final_path = to_full_path_from_digest(&self.content_path, &digest);
        fs::rename(&file_path, &final_path).await.err_tip(|| {
            format!(
                "Failed to move file from temp directory to content directory {}",
                file_path
            )
        })?;

        self.evicting_map.insert(digest, entry).await;
        Ok(())
    }
}

#[async_trait]
impl StoreTrait for FilesystemStore {
    async fn has(self: Pin<&Self>, digest: DigestInfo) -> Result<Option<usize>, Error> {
        Ok(self.evicting_map.size_for_key(&digest).await)
    }

    async fn update(
        self: Pin<&Self>,
        digest: DigestInfo,
        reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        let file_name = thread_rng().gen::<u64>().to_string();
        let temp_full_path = to_full_path(&self.temp_path, &file_name);

        let mut file = fs::File::create(&temp_full_path)
            .await
            .err_tip(|| "Failed to create temp file in filesystem store")?;

        if let Err(err) = self.update_file(&temp_full_path, &mut file, digest, reader).await {
            let result = fs::remove_file(temp_full_path)
                .await
                .err_tip(|| "Failed to delete temp file in filesystem store");
            if result.is_err() {
                return Result::<(), Error>::Err(err).merge(result);
            }
            return Err(err);
        }

        Ok(())
    }

    async fn get_part(
        self: Pin<&Self>,
        digest: DigestInfo,
        mut writer: DropCloserWriteHalf,
        offset: usize,
        length: Option<usize>,
    ) -> Result<(), Error> {
        let entry = self
            .evicting_map
            .get(&digest)
            .await
            .ok_or_else(|| make_err!(Code::NotFound, "not found in filesystem store"))?;
        let mut file = entry
            .read_file_part(offset as u64, length.unwrap_or(usize::MAX) as u64)
            .await?;

        let mut buf = BytesMut::with_capacity(length.unwrap_or(DEFAULT_BUFF_SIZE));
        loop {
            file.read_buf(&mut buf)
                .await
                .err_tip(|| "Failed to read data in filesystem store")?;
            if buf.len() == 0 {
                break; // EOF.
            }
            writer
                .send(buf.split().freeze())
                .await
                .err_tip(|| "Failed to send chunk in filesystem store get_part")?;
        }
        writer
            .send_eof()
            .await
            .err_tip(|| "Filed to send EOF in filesystem store get_part")?;

        Ok(())
    }
}
