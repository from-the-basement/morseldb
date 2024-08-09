use std::{borrow::Borrow, cmp::Ordering, marker::PhantomData, mem::size_of, ptr};

use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    serdes::{Decode, Encode},
    timestamp::Timestamp,
};

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Timestamped<V> {
    pub ts: Timestamp,
    pub value: V,
}

impl<V> Copy for Timestamped<V> where V: Copy {}

impl<V> Timestamped<V> {
    pub(crate) fn new(value: V, ts: Timestamp) -> Self {
        Self { value, ts }
    }

    pub(crate) fn map<G>(&self, mut f: impl FnMut(&V) -> G) -> Timestamped<G> {
        Timestamped {
            value: f(&self.value),
            ts: self.ts,
        }
    }

    pub(crate) fn value(&self) -> &V {
        &self.value
    }

    #[allow(unused)]
    pub(crate) fn ts(&self) -> Timestamp {
        self.ts
    }

    #[allow(unused)]
    pub(crate) fn into_parts(self) -> (V, Timestamp) {
        (self.value, self.ts)
    }
}

impl<V> Timestamped<V>
where
    V: Encode,
{
    pub(crate) fn size(&self) -> usize {
        self.value.size() + size_of::<u32>()
    }
}

impl<V> PartialOrd<Self> for Timestamped<V>
where
    V: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        TimestampedRef::new(&self.value, self.ts)
            .partial_cmp(TimestampedRef::new(&other.value, other.ts))
    }
}

impl<V> Ord for Timestamped<V>
where
    V: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        TimestampedRef::new(&self.value, self.ts).cmp(TimestampedRef::new(&other.value, other.ts))
    }
}

#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct TimestampedRef<V> {
    _marker: PhantomData<V>,
    _mem: [()],
}

impl<V> TimestampedRef<V> {
    pub(crate) fn new(value: &V, ts: Timestamp) -> &Self {
        let value = value as *const _ as usize;
        let ts: u32 = ts.into();

        let mem = ptr::slice_from_raw_parts(value as *const V, ts as usize) as *const Self;
        unsafe { &*mem }
    }

    fn to_timestamped(&self) -> (&V, Timestamp) {
        let i = self as *const TimestampedRef<V> as *const [()];
        unsafe { (&*(i as *const ()).cast::<V>(), (i.len() as u32).into()) }
    }

    pub(crate) fn value(&self) -> &V {
        self.to_timestamped().0
    }

    pub(crate) fn ts(&self) -> Timestamp {
        self.to_timestamped().1
    }
}

impl<Q, V> Borrow<TimestampedRef<Q>> for Timestamped<V>
where
    V: Borrow<Q>,
{
    fn borrow(&self) -> &TimestampedRef<Q> {
        TimestampedRef::new(self.value.borrow(), self.ts)
    }
}

impl<V> PartialEq for TimestampedRef<V>
where
    V: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.value() == other.value() && self.ts() == other.ts()
    }
}

impl<V> Eq for TimestampedRef<V> where V: Eq {}

impl<V> PartialOrd<Self> for TimestampedRef<V>
where
    V: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.value()
            .partial_cmp(other.value())
            .map(|ordering| ordering.then_with(|| other.ts().cmp(&self.ts())))
    }
}

impl<K> Ord for TimestampedRef<K>
where
    K: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.value()
            .cmp(other.value())
            .then_with(|| other.ts().cmp(&self.ts()))
    }
}

impl<V> Encode for Timestamped<V>
where
    V: Encode,
{
    type Error = V::Error;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: AsyncWrite + Unpin + Send,
    {
        self.ts.encode(writer).await?;
        self.value.encode(writer).await
    }

    fn size(&self) -> usize {
        self.ts.size() + self.value.size()
    }
}

impl<V> Decode for Timestamped<V>
where
    V: Decode,
{
    type Error = V::Error;

    async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
    where
        R: AsyncRead + Unpin,
    {
        let ts = Timestamp::decode(reader).await?;
        let value = V::decode(reader).await?;
        Ok(Timestamped::new(value, ts))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_cmp() {
        let value1 = Timestamped::new(&1, 1_u32.into());
        let value2 = Timestamped::new(&2, 2_u32.into());
        assert!(value1 < value2);

        let value1 = Timestamped::new(&1, 1_u32.into());
        let value2 = Timestamped::new(&1, 2_u32.into());
        assert!(value1 > value2);
    }

    #[test]
    fn test_value_eq() {
        let value1 = Timestamped::new(&1, 1_u32.into());
        let value2 = Timestamped::new(&1, 1_u32.into());
        assert_eq!(value1, value2);

        let value1 = Timestamped::new(&1, 1_u32.into());
        let value2 = Timestamped::new(&2, 1_u32.into());
        assert_ne!(value1, value2);

        let value1 = Timestamped::new(&1, 1_u32.into());
        let value2 = Timestamped::new(&1, 2_u32.into());
        assert_ne!(value1, value2);
    }
}
