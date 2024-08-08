mod arc;
mod boolean;
mod num;
pub(crate) mod option;
mod string;

use std::{future::Future, io};

use tokio::io::{AsyncRead, AsyncWrite};

pub trait Encode: Send + Sync {
    type Error: From<io::Error> + std::error::Error + Send + Sync + 'static;

    fn encode<W>(&self, writer: &mut W) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        W: AsyncWrite + Unpin + Send;

    fn size(&self) -> usize;
}

impl<T: Encode> Encode for &T {
    type Error = T::Error;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: AsyncWrite + Unpin + Send,
    {
        Encode::encode(*self, writer).await
    }

    fn size(&self) -> usize {
        Encode::size(*self)
    }
}

pub trait Decode: Sized {
    type Error: From<io::Error> + std::error::Error + Send + Sync + 'static;

    fn decode<R>(reader: &mut R) -> impl Future<Output = Result<Self, Self::Error>>
    where
        R: AsyncRead + Unpin;
}
