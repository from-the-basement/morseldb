use std::{io, mem::size_of};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::serdes::{Decode, Encode};

impl Encode for bool {
    type Error = io::Error;

    async fn encode<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> Result<(), Self::Error> {
        writer
            .write_all(&if *self { 1u8 } else { 0u8 }.to_le_bytes())
            .await
    }

    fn size(&self) -> usize {
        size_of::<u8>()
    }
}

impl Decode for bool {
    type Error = io::Error;

    async fn decode<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self, Self::Error> {
        let buf = {
            let mut buf = [0; size_of::<u8>()];
            reader.read_exact(&mut buf).await?;
            buf
        };

        Ok(u8::from_le_bytes(buf) == 1u8)
    }
}
