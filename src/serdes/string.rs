use std::{io, mem::size_of};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::{Decode, Encode};

impl<'r> Encode for &'r str {
    type Error = io::Error;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: AsyncWrite + Unpin,
    {
        writer.write_all(&(self.len() as u16).to_le_bytes()).await?;
        writer.write_all(self.as_bytes()).await
    }

    fn size(&self) -> usize {
        size_of::<u16>() + self.len()
    }
}

impl Encode for String {
    type Error = io::Error;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: AsyncWrite + Unpin + Send,
    {
        self.as_str().encode(writer).await
    }

    fn size(&self) -> usize {
        self.as_str().size()
    }
}

impl Decode for String {
    type Error = io::Error;

    async fn decode<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self, Self::Error> {
        let len = {
            let mut len = [0; size_of::<u16>()];
            reader.read_exact(&mut len).await?;
            u16::from_le_bytes(len) as usize
        };

        let vec = {
            let mut vec = vec![0; len];
            reader.read_exact(&mut vec).await?;
            vec
        };

        Ok(unsafe { String::from_utf8_unchecked(vec) })
    }
}
