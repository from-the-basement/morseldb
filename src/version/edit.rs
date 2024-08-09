use std::mem::size_of;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{
    fs::FileId,
    scope::Scope,
    serdes::{Decode, Encode},
    timestamp::Timestamp,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum VersionEdit<K> {
    Add { level: u8, scope: Scope<K> },
    Remove { level: u8, gen: FileId },
    LatestTimeStamp { ts: Timestamp },
}

impl<K> VersionEdit<K>
where
    K: Decode,
{
    pub(crate) async fn recover<R: AsyncRead + Unpin>(reader: &mut R) -> Vec<VersionEdit<K>> {
        let mut edits = Vec::new();

        while let Ok(edit) = VersionEdit::decode(reader).await {
            edits.push(edit)
        }
        edits
    }
}

impl<K> Encode for VersionEdit<K>
where
    K: Encode,
{
    type Error = <K as Encode>::Error;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: AsyncWrite + Unpin + Send,
    {
        match self {
            VersionEdit::Add { scope, level } => {
                writer.write_all(&0u8.to_le_bytes()).await?;
                writer.write_all(&level.to_le_bytes()).await?;
                scope.encode(writer).await?;
            }
            VersionEdit::Remove { gen, level } => {
                writer.write_all(&1u8.to_le_bytes()).await?;
                writer.write_all(&level.to_le_bytes()).await?;
                writer.write_all(&gen.to_bytes()).await?;
            }
            VersionEdit::LatestTimeStamp { ts } => {
                writer.write_all(&2u8.to_le_bytes()).await?;
                ts.encode(writer).await?;
            }
        }

        Ok(())
    }

    fn size(&self) -> usize {
        size_of::<u8>()
            + size_of::<u8>()
            + match self {
                VersionEdit::Add { scope, .. } => scope.size(),
                VersionEdit::Remove { .. } => 16,
                VersionEdit::LatestTimeStamp { ts } => ts.size(),
            }
    }
}

impl<K> Decode for VersionEdit<K>
where
    K: Decode,
{
    type Error = <K as Decode>::Error;

    async fn decode<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self, Self::Error> {
        let edit_type = {
            let mut len = [0; size_of::<u8>()];
            reader.read_exact(&mut len).await?;
            u8::from_le_bytes(len) as usize
        };

        Ok(match edit_type {
            0 => {
                let level = {
                    let mut level = [0; size_of::<u8>()];
                    reader.read_exact(&mut level).await?;
                    u8::from_le_bytes(level)
                };
                let scope = Scope::<K>::decode(reader).await?;

                VersionEdit::Add { level, scope }
            }
            1 => {
                let level = {
                    let mut level = [0; size_of::<u8>()];
                    reader.read_exact(&mut level).await?;
                    u8::from_le_bytes(level)
                };
                let gen = {
                    let mut slice = [0; 16];
                    reader.read_exact(&mut slice).await?;
                    FileId::from_bytes(slice)
                };
                VersionEdit::Remove { level, gen }
            }
            2 => {
                let ts = Timestamp::decode(reader).await?;
                VersionEdit::LatestTimeStamp { ts }
            }
            _ => todo!(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::{fs::FileId, scope::Scope, serdes::Encode, version::edit::VersionEdit};

    #[tokio::test]
    async fn encode_and_decode() {
        let edits = vec![
            VersionEdit::Add {
                level: 0,
                scope: Scope {
                    min: "Min".to_string(),
                    max: "Max".to_string(),
                    gen: Default::default(),
                    wal_ids: Some(vec![FileId::new(), FileId::new()]),
                },
            },
            VersionEdit::Remove {
                level: 1,
                gen: Default::default(),
            },
        ];

        let bytes = {
            let mut cursor = Cursor::new(vec![]);

            for edit in edits.clone() {
                edit.encode(&mut cursor).await.unwrap();
            }
            cursor.into_inner()
        };

        let decode_edits = {
            let mut cursor = Cursor::new(bytes);

            VersionEdit::<String>::recover(&mut cursor).await
        };

        assert_eq!(edits, decode_edits);
    }
}
