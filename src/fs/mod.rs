pub mod tokio;

use std::{
    fmt::{Display, Formatter},
    future::Future,
    io,
    path::Path,
};
use ::tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};
use futures_core::Stream;
use ulid::Ulid;

pub(crate) type FileId = Ulid;

pub enum FileType {
    Wal,
    Parquet,
    Log,
}

pub trait AsyncFile: AsyncRead + AsyncWrite + AsyncSeek + Send + Sync + Unpin + 'static {}

impl<T> AsyncFile for T where T: AsyncRead + AsyncWrite + AsyncSeek + Send + Sync + Unpin + 'static {}

pub trait FileProvider {
    type File: AsyncFile;

    fn create_dir_all(path: impl AsRef<Path>) -> impl Future<Output = io::Result<()>>;

    fn open(path: impl AsRef<Path> + Send) -> impl Future<Output = io::Result<Self::File>> + Send;

    fn remove(path: impl AsRef<Path> + Send) -> impl Future<Output = io::Result<()>> + Send;

    fn list(
        dir_path: impl AsRef<Path> + Send,
        file_type: FileType,
    ) -> io::Result<impl Stream<Item = io::Result<(Self::File, FileId)>>>;
}

impl Display for FileType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FileType::Wal => write!(f, "wal"),
            FileType::Parquet => write!(f, "parquet"),
            FileType::Log => write!(f, "log"),
        }
    }
}
