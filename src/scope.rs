use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{
    fs::FileId,
    serdes::{Decode, Encode},
};

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Scope<K> {
    pub(crate) min: K,
    pub(crate) max: K,
    pub(crate) gen: FileId,
    pub(crate) wal_ids: Option<Vec<FileId>>,
}

impl<K> Clone for Scope<K>
where
    K: Clone,
{
    fn clone(&self) -> Self {
        Scope {
            min: self.min.clone(),
            max: self.max.clone(),
            gen: self.gen,
            wal_ids: self.wal_ids.clone(),
        }
    }
}

impl<K> Scope<K>
where
    K: Ord,
{
    pub(crate) fn contains(&self, key: &K) -> bool {
        &self.min <= key && key <= &self.max
    }

    #[allow(unused)]
    pub(crate) fn meets(&self, target: &Self) -> bool {
        self.contains(&target.min) || self.contains(&target.max)
    }

    pub(crate) fn gen(&self) -> FileId {
        self.gen
    }
}

impl<K> Encode for Scope<K>
where
    K: Encode,
{
    type Error = <K as Encode>::Error;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: AsyncWrite + Unpin + Send,
    {
        self.min.encode(writer).await?;
        self.max.encode(writer).await?;

        writer.write_all(&self.gen.to_bytes()).await?;

        match &self.wal_ids {
            None => {
                0u8.encode(writer).await?;
            }
            Some(ids) => {
                1u8.encode(writer).await?;
                (ids.len() as u32).encode(writer).await?;
                for id in ids {
                    writer.write_all(&id.to_bytes()).await?;
                }
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        // ProcessUniqueId: usize + u64
        self.min.size() + self.max.size() + 16
    }
}

impl<K> Decode for Scope<K>
where
    K: Decode,
{
    type Error = <K as Decode>::Error;

    async fn decode<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self, Self::Error> {
        let min = K::decode(reader).await?;
        let max = K::decode(reader).await?;

        let gen = {
            let mut slice = [0; 16];
            reader.read_exact(&mut slice).await?;
            FileId::from_bytes(slice)
        };
        let wal_ids = match u8::decode(reader).await? {
            0 => None,
            1 => {
                let len = u32::decode(reader).await? as usize;
                let mut ids = Vec::with_capacity(len);

                for _ in 0..len {
                    let mut slice = [0; 16];
                    reader.read_exact(&mut slice).await?;
                    ids.push(FileId::from_bytes(slice));
                }
                Some(ids)
            }
            _ => unreachable!(),
        };

        Ok(Scope {
            min,
            max,
            gen,
            wal_ids,
        })
    }
}
