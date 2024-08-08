mod checksum;
pub(crate) mod log;
pub(crate) mod record_entry;

use std::{io, marker::PhantomData};

use async_stream::stream;
use checksum::{HashReader, HashWriter};
use futures_core::Stream;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use log::Log;
use thiserror::Error;

use crate::{
    fs::FileId,
    record::{Key, Record},
    serdes::{Decode, Encode},
    timestamp::Timestamped,
    wal::{log::LogType, record_entry::RecordEntry},
};

#[derive(Debug)]
pub(crate) struct WalFile<F, R> {
    file: F,
    file_id: FileId,
    _marker: PhantomData<R>,
}

impl<F, R> WalFile<F, R> {
    pub(crate) fn new(file: F, file_id: FileId) -> Self {
        Self {
            file,
            file_id,
            _marker: PhantomData,
        }
    }

    pub(crate) fn file_id(&self) -> FileId {
        self.file_id
    }
}

impl<F, R> WalFile<F, R>
where
    F: AsyncWrite + Unpin + Send,
    R: Record,
{
    pub(crate) async fn write<'r>(
        &mut self,
        log_ty: LogType,
        key: Timestamped<<R::Key as Key>::Ref<'r>>,
        value: Option<R::Ref<'r>>,
    ) -> Result<(), <R::Ref<'r> as Encode>::Error> {
        let mut writer = HashWriter::new(&mut self.file);
        Log::new(log_ty, RecordEntry::<R>::Encode((key, value)))
            .encode(&mut writer)
            .await?;
        writer.eol().await?;
        Ok(())
    }

    pub(crate) async fn flush(&mut self) -> io::Result<()> {
        self.file.flush().await
    }
}

impl<F, R> WalFile<F, R>
where
    F: AsyncRead + Unpin,
    R: Record,
{
    pub(crate) fn recover(
        &mut self,
    ) -> impl Stream<
        Item = Result<
            (LogType, Timestamped<R::Key>, Option<R>),
            RecoverError<<R as Decode>::Error>,
        >,
    > + '_ {
        stream! {
            let mut file = BufReader::new(&mut self.file);

            loop {
                if file.buffer().is_empty() && file.fill_buf().await?.is_empty() {
                    return;
                }

                let mut reader = HashReader::new(&mut file);

                let record = Log::<RecordEntry<'static, R>>::decode(&mut reader).await.map_err(RecoverError::Io)?;

                if !reader.checksum().await? {
                    yield Err(RecoverError::Checksum);
                    return;
                }
                if let RecordEntry::Decode((key, value)) = record.record {
                    yield Ok((record.log_type, key, value));
                } else {
                    unreachable!()
                }
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum RecoverError<E: std::error::Error> {
    #[error("wal recover decode error: {0}")]
    Decode(E),
    #[error("wal recover checksum error")]
    Checksum,
    #[error("wal recover io error")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, pin::pin};

    use futures_util::StreamExt;

    use super::{log::LogType, FileId, WalFile};
    use crate::timestamp::Timestamped;

    #[tokio::test]
    async fn write_and_recover() {
        let mut file = Vec::new();
        {
            let mut wal = WalFile::<_, String>::new(Cursor::new(&mut file), FileId::new());
            wal.write(
                LogType::Full,
                Timestamped::new("hello", 0.into()),
                Some("hello"),
            )
            .await
            .unwrap();
            wal.flush().await.unwrap();
        }
        {
            let mut wal = WalFile::<_, String>::new(Cursor::new(&mut file), FileId::new());

            {
                let mut stream = pin!(wal.recover());
                let (_, key, value) = stream.next().await.unwrap().unwrap();
                assert_eq!(key.ts, 0.into());
                assert_eq!(value, Some("hello".to_string()));
            }

            wal.write(
                LogType::Full,
                Timestamped::new("world", 1.into()),
                Some("world"),
            )
            .await
            .unwrap();
        }

        {
            let mut wal = WalFile::<_, String>::new(Cursor::new(&mut file), FileId::new());

            {
                let mut stream = pin!(wal.recover());
                let (_, key, value) = stream.next().await.unwrap().unwrap();
                assert_eq!(key.ts, 0.into());
                assert_eq!(value, Some("hello".to_string()));
                let (_, key, value) = stream.next().await.unwrap().unwrap();
                assert_eq!(key.ts, 1.into());
                assert_eq!(value, Some("world".to_string()));
            }
        }
    }
}
