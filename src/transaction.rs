use std::{
    collections::{
        btree_map::{Entry, Range},
        BTreeMap, Bound,
    },
    io,
    mem::transmute,
};

use async_lock::RwLockReadGuard;
use lockable::AsyncLimit;
use parquet::errors::ParquetError;
use thiserror::Error;

use crate::{
    compaction::CompactTask,
    fs::FileProvider,
    record::{Key, KeyRef},
    stream,
    timestamp::{Timestamp, Timestamped},
    version::VersionRef,
    wal::log::LogType,
    DbError, LockMap, Projection, Record, Scan, Schema,
};

pub(crate) struct TransactionScan<'scan, R: Record> {
    inner: Range<'scan, R::Key, Option<R>>,
    ts: Timestamp,
}

impl<'scan, R> Iterator for TransactionScan<'scan, R>
where
    R: Record,
{
    type Item = (Timestamped<<R::Key as Key>::Ref<'scan>>, &'scan Option<R>);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(key, value)| (Timestamped::new(key.as_key_ref(), self.ts), value))
    }
}
/// optimistic ACID transaction, open with
/// [`DB::transaction`](../struct.DB.html#method.transaction) method
pub struct Transaction<'txn, R, FP>
where
    R: Record,
    FP: FileProvider,
{
    ts: Timestamp,
    local: BTreeMap<R::Key, Option<R>>,
    share: RwLockReadGuard<'txn, Schema<R, FP>>,
    version: VersionRef<R, FP>,
    lock_map: LockMap<R::Key>,
}

impl<'txn, R, FP> Transaction<'txn, R, FP>
where
    R: Record + Send,
    FP: FileProvider,
{
    pub(crate) fn new(
        version: VersionRef<R, FP>,
        share: RwLockReadGuard<'txn, Schema<R, FP>>,
        lock_map: LockMap<R::Key>,
    ) -> Self {
        Self {
            ts: version.transaction_ts(),
            local: BTreeMap::new(),
            share,
            version,
            lock_map,
        }
    }

    /// get the record with `key` as the primary key and get only the data specified in
    /// [`Projection`](../enum.Projection.html)
    pub async fn get<'get>(
        &'get self,
        key: &'get R::Key,
        projection: Projection,
    ) -> Result<Option<TransactionEntry<'get, R>>, DbError<R>> {
        Ok(match self.local.get(key).and_then(|v| v.as_ref()) {
            Some(v) => Some(TransactionEntry::Local(v.as_record_ref())),
            None => self
                .share
                .get(&self.version, key, self.ts, projection)
                .await?
                .and_then(|entry| {
                    if entry.value().is_none() {
                        None
                    } else {
                        Some(TransactionEntry::Stream(entry))
                    }
                }),
        })
    }

    /// scan records with primary keys in the `range`
    pub async fn scan<'scan>(
        &'scan self,
        range: (Bound<&'scan R::Key>, Bound<&'scan R::Key>),
    ) -> Scan<'scan, R, FP> {
        let streams = vec![TransactionScan {
            inner: self.local.range(range),
            ts: self.ts,
        }
        .into()];
        Scan::new(&self.share, range, self.ts, &self.version, streams)
    }

    /// insert a sequence of data as a single batch on this transaction
    pub fn insert(&mut self, value: R) {
        self.entry(value.key().to_key(), Some(value))
    }

    /// delete the record with the primary key as the `key` on this transaction
    pub fn remove(&mut self, key: R::Key) {
        self.entry(key, None)
    }

    fn entry(&mut self, key: R::Key, value: Option<R>) {
        match self.local.entry(key) {
            Entry::Vacant(v) => {
                v.insert(value);
            }
            Entry::Occupied(mut o) => *o.get_mut() = value,
        }
    }

    /// commit the data in the [`Transaction`](struct.Transaction.html) to the corresponding
    /// [`DB`](../struct.DB.html)
    pub async fn commit(mut self) -> Result<(), CommitError<R>> {
        let mut _key_guards = Vec::new();

        for (key, _) in self.local.iter() {
            // SAFETY: Error is Never
            _key_guards.push(
                self.lock_map
                    .async_lock(key.clone(), AsyncLimit::no_limit())
                    .await
                    .unwrap(),
            );
        }
        for (key, _) in self.local.iter() {
            if self.share.check_conflict(key, self.ts) {
                return Err(CommitError::WriteConflict(key.clone()));
            }
        }

        let len = self.local.len();
        let is_excess = match len {
            0 => false,
            1 => {
                let new_ts = self.version.transaction_ts();
                let (key, record) = self.local.pop_first().unwrap();
                Self::append(&self.share, LogType::Full, key, record, new_ts).await?
            }
            _ => {
                let new_ts = self.version.transaction_ts();
                let mut iter = self.local.into_iter();

                let (key, record) = iter.next().unwrap();
                Self::append(&self.share, LogType::First, key, record, new_ts).await?;

                for (key, record) in (&mut iter).take(len - 2) {
                    Self::append(&self.share, LogType::Middle, key, record, new_ts).await?;
                }

                let (key, record) = iter.next().unwrap();
                Self::append(&self.share, LogType::Last, key, record, new_ts).await?
            }
        };
        if is_excess {
            let _ = self.share.compaction_tx.try_send(CompactTask::Freeze);
        }
        Ok(())
    }

    async fn append(
        schema: &Schema<R, FP>,
        log_ty: LogType,
        key: <R as Record>::Key,
        record: Option<R>,
        new_ts: Timestamp,
    ) -> Result<bool, CommitError<R>> {
        Ok(match record {
            Some(record) => schema.write(log_ty, record, new_ts).await?,
            None => schema.remove(log_ty, key, new_ts).await?,
        })
    }
}

pub enum TransactionEntry<'entry, R>
where
    R: Record,
{
    Stream(stream::Entry<'entry, R>),
    Local(R::Ref<'entry>),
}

impl<'entry, R> TransactionEntry<'entry, R>
where
    R: Record,
{
    pub fn get(&self) -> R::Ref<'_> {
        match self {
            TransactionEntry::Stream(entry) => entry.value().unwrap(),
            TransactionEntry::Local(value) => {
                // Safety: shorter lifetime must be safe
                unsafe { transmute::<R::Ref<'entry>, R::Ref<'_>>(value.clone()) }
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum CommitError<R>
where
    R: Record,
{
    #[error("transaction io error {:?}", .0)]
    Io(#[from] io::Error),
    #[error("transaction parquet error {:?}", .0)]
    Parquet(#[from] ParquetError),
    #[error("transaction database error {:?}", .0)]
    Database(#[from] DbError<R>),
    #[error("transaction write conflict: {:?}", .0)]
    WriteConflict(R::Key),
}

#[cfg(test)]
mod tests {
    use std::{collections::Bound, sync::Arc};

    use futures_util::StreamExt;
    use tempfile::TempDir;

    use crate::{
        compaction::tests::build_version,
        executor::tokio::TokioExecutor,
        tests::{build_db, build_schema, Test},
        transaction::CommitError,
        DbOption, Projection, DB,
    };

    #[tokio::test]
    async fn transaction_read_write() {
        let temp_dir = TempDir::new().unwrap();

        let db =
            DB::<String, TokioExecutor>::new(DbOption::from(temp_dir.path()), TokioExecutor::new())
                .await
                .unwrap();
        {
            let mut txn1 = db.transaction().await;
            txn1.insert("foo".to_string());

            let txn2 = db.transaction().await;
            assert!(txn2
                .get(&"foo".to_string(), Projection::All)
                .await
                .unwrap()
                .is_none());

            txn1.commit().await.unwrap();
            txn2.commit().await.unwrap();
        }

        {
            let txn3 = db.transaction().await;
            assert!(txn3
                .get(&"foo".to_string(), Projection::All)
                .await
                .unwrap()
                .is_some());
            txn3.commit().await.unwrap();
        }
    }

    #[tokio::test]
    async fn write_conflicts() {
        let temp_dir = TempDir::new().unwrap();

        let db =
            DB::<String, TokioExecutor>::new(DbOption::from(temp_dir.path()), TokioExecutor::new())
                .await
                .unwrap();

        let mut txn = db.transaction().await;
        txn.insert(0.to_string());
        txn.insert(1.to_string());
        txn.commit().await.unwrap();

        let mut txn_0 = db.transaction().await;
        let mut txn_1 = db.transaction().await;
        let mut txn_2 = db.transaction().await;

        txn_0.insert(1.to_string());
        txn_1.insert(1.to_string());
        txn_1.insert(2.to_string());
        txn_2.insert(2.to_string());

        txn_0.commit().await.unwrap();

        if let Err(CommitError::WriteConflict(conflict_key)) = txn_1.commit().await {
            assert_eq!(conflict_key, 1.to_string());
            txn_2.commit().await.unwrap();
            return;
        }
        unreachable!();
    }

    #[tokio::test]
    async fn transaction_projection() {
        let temp_dir = TempDir::new().unwrap();

        let db =
            DB::<Test, TokioExecutor>::new(DbOption::from(temp_dir.path()), TokioExecutor::new())
                .await
                .unwrap();

        let mut txn1 = db.transaction().await;
        txn1.insert(Test {
            vstring: 0.to_string(),
            vu32: 0,
            vbool: Some(true),
        });

        let key = 0.to_string();
        let entry = txn1.get(&key, Projection::All).await.unwrap().unwrap();

        assert_eq!(entry.get().vstring, 0.to_string());
        assert_eq!(entry.get().vu32, Some(0));
        assert_eq!(entry.get().vbool, Some(true));
        drop(entry);

        txn1.commit().await.unwrap();
    }

    #[tokio::test]
    async fn transaction_scan() {
        let temp_dir = TempDir::new().unwrap();
        let option = Arc::new(DbOption::from(temp_dir.path()));

        let (_, version) = build_version(&option).await;
        let (schema, compaction_rx) = build_schema(option.clone()).await.unwrap();
        let db = build_db(option, compaction_rx, TokioExecutor::new(), schema, version)
            .await
            .unwrap();

        let mut txn = db.transaction().await;
        txn.insert(Test {
            vstring: "king".to_string(),
            vu32: 8,
            vbool: Some(true),
        });

        let mut stream = txn
            .scan((Bound::Unbounded, Bound::Unbounded))
            .await
            .projection(vec![1])
            .take()
            .await
            .unwrap();

        let entry_0 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_0.key().value, "1");
        assert!(entry_0.value().unwrap().vbool.is_none());
        let entry_1 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_1.key().value, "2");
        assert!(entry_1.value().unwrap().vbool.is_none());
        let entry_2 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_2.key().value, "3");
        assert!(entry_2.value().unwrap().vbool.is_none());
        let entry_3 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_3.key().value, "4");
        assert!(entry_3.value().unwrap().vbool.is_none());
        let entry_4 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_4.key().value, "5");
        assert!(entry_4.value().unwrap().vbool.is_none());
        let entry_5 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_5.key().value, "6");
        assert!(entry_5.value().unwrap().vbool.is_none());
        let entry_6 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_6.key().value, "7");
        assert!(entry_6.value().unwrap().vbool.is_none());
        let entry_7 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_7.key().value, "8");
        assert!(entry_7.value().unwrap().vbool.is_none());
        let entry_8 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_8.key().value, "9");
        assert!(entry_8.value().unwrap().vbool.is_none());
        let entry_9 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_9.key().value, "alice");
        let entry_10 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_10.key().value, "ben");
        let entry_11 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_11.key().value, "carl");
        let entry_12 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_12.key().value, "dice");
        let entry_13 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_13.key().value, "erika");
        let entry_14 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_14.key().value, "funk");
        let entry_15 = stream.next().await.unwrap().unwrap();
        assert_eq!(entry_15.key().value, "king");
    }
}
