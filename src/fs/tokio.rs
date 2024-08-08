use std::{fs, fs::DirEntry, io, path::Path};

use async_stream::stream;
use futures_core::Stream;
use regex::Regex;
use tokio::fs::{create_dir_all, remove_file, File, OpenOptions};

use super::{FileId, FileProvider, FileType};
use crate::executor::tokio::TokioExecutor;

impl FileProvider for TokioExecutor {
    type File = File;

    async fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
        create_dir_all(path).await
    }

    async fn open(path: impl AsRef<Path> + Send) -> io::Result<Self::File> {
        OpenOptions::new()
            .truncate(false)
            .create(true)
            .write(true)
            .read(true)
            .open(path)
            .await
    }

    async fn remove(path: impl AsRef<Path> + Send) -> io::Result<()> {
        remove_file(path).await
    }

    fn list(
        dir_path: impl AsRef<Path> + Send,
        file_type: FileType,
    ) -> io::Result<impl Stream<Item = io::Result<(Self::File, FileId)>>> {
        let dir_path = dir_path.as_ref().to_path_buf();
        let mut entries: Vec<DirEntry> =
            fs::read_dir(&dir_path)?.collect::<Result<Vec<_>, io::Error>>()?;
        entries.sort_by_key(|entry| entry.file_name());

        Ok(stream! {
            for entry in entries {
                let path = entry.path();
                if path.is_file() {
                    if let Some(filename) = path.file_name().and_then(|s| s.to_str()) {
                        if Regex::new(format!("^[0123456789ABCDEFGHJKMNPQRSTVWXYZ]{{26}}.{}$", file_type).as_str()).unwrap().is_match(filename) {
                            // SAFETY: Checked on WAL_REGEX
                            let file_id = FileId::from_string(filename
                                .split('.')
                                .next()
                                .unwrap()).unwrap();
                            yield Ok((Self::open(dir_path.join(filename)).await?, file_id))
                        }
                    }
                }
            }
        })
    }
}
