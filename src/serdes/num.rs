use std::{io, mem::size_of};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::{Decode, Encode};

#[macro_export]
macro_rules! implement_encode_decode {
    ($struct_name:ident) => {
        impl Encode for $struct_name {
            type Error = io::Error;

            async fn encode<W: AsyncWrite + Unpin>(
                &self,
                writer: &mut W,
            ) -> Result<(), Self::Error> {
                writer.write_all(&self.to_le_bytes()).await
            }

            fn size(&self) -> usize {
                size_of::<Self>()
            }
        }

        impl Decode for $struct_name {
            type Error = io::Error;

            async fn decode<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self, Self::Error> {
                let buf = {
                    let mut buf = [0; size_of::<Self>()];
                    reader.read_exact(&mut buf).await?;
                    buf
                };

                Ok(Self::from_le_bytes(buf))
            }
        }
    };
}

implement_encode_decode!(i8);
implement_encode_decode!(i16);
implement_encode_decode!(i32);
implement_encode_decode!(i64);
implement_encode_decode!(u8);
implement_encode_decode!(u16);
implement_encode_decode!(u32);
implement_encode_decode!(u64);
