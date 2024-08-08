pub mod timestamped;

use std::io;

use arrow::{
    array::{PrimitiveArray, Scalar},
    datatypes::UInt32Type,
};
use tokio::io::{AsyncRead, AsyncWrite};

pub(crate) use self::timestamped::*;
use crate::serdes::{Decode, Encode};

#[repr(transparent)]
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Hash)]
pub struct Timestamp(u32);

pub(crate) const EPOCH: Timestamp = Timestamp(0);

impl From<u32> for Timestamp {
    fn from(ts: u32) -> Self {
        Self(ts)
    }
}

impl From<Timestamp> for u32 {
    fn from(value: Timestamp) -> Self {
        value.0
    }
}

impl Timestamp {
    pub(crate) fn to_arrow_scalar(self) -> Scalar<PrimitiveArray<UInt32Type>> {
        PrimitiveArray::<UInt32Type>::new_scalar(self.0)
    }
}

impl Encode for Timestamp {
    type Error = io::Error;
    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: AsyncWrite + Unpin + Send,
    {
        self.0.encode(writer).await
    }
    fn size(&self) -> usize {
        self.0.size()
    }
}
impl Decode for Timestamp {
    type Error = io::Error;
    async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
    where
        R: AsyncRead + Unpin,
    {
        u32::decode(reader).await.map(Timestamp)
    }
}
