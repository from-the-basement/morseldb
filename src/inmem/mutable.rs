use std::{intrinsics::transmute, ops::Bound};

use async_lock::Mutex;
use crossbeam_skiplist::{
    map::{Entry, Range},
    SkipMap,
};
use futures_util::io;
use ulid::Ulid;

use crate::{
    fs::{FileId, FileProvider},
    inmem::immutable::Immutable,
    record::{Key, KeyRef, Record},
    timestamp::{
        timestamped::{Timestamped, TimestampedRef},
        Timestamp, EPOCH,
    },
    wal::{log::LogType, WalFile},
    DbError, DbOption,
};

pub(crate) type MutableScan<'scan, R> = Range<
    'scan,
    TimestampedRef<<R as Record>::Key>,
    (
        Bound<&'scan TimestampedRef<<R as Record>::Key>>,
        Bound<&'scan TimestampedRef<<R as Record>::Key>>,
    ),
    Timestamped<<R as Record>::Key>,
    Option<R>,
>;

#[derive(Debug)]
pub struct Mutable<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    pub(crate) data: SkipMap<Timestamped<R::Key>, Option<R>>,
    wal: Option<Mutex<WalFile<FP::File, R>>>,
}

impl<R, FP> Mutable<R, FP>
where
    FP: FileProvider,
    R: Record,
{
    pub async fn new(option: &DbOption<R>) -> io::Result<Self> {
        let mut wal = None;
        if option.use_wal {
            let file_id = Ulid::new();
            let file = FP::open(option.wal_path(&file_id)).await?;

            wal = Some(Mutex::new(WalFile::new(file, file_id)));
        };

        Ok(Self {
            data: Default::default(),
            wal,
        })
    }
}

impl<R, FP> Mutable<R, FP>
where
    R: Record + Send,
    FP: FileProvider,
{
    pub(crate) async fn insert(
        &self,
        log_ty: LogType,
        record: R,
        ts: Timestamp,
    ) -> Result<usize, DbError<R>> {
        self.append(Some(log_ty), record.key().to_key(), ts, Some(record))
            .await
    }

    pub(crate) async fn remove(
        &self,
        log_ty: LogType,
        key: R::Key,
        ts: Timestamp,
    ) -> Result<usize, DbError<R>> {
        self.append(Some(log_ty), key, ts, None).await
    }

    pub(crate) async fn append(
        &self,
        log_ty: Option<LogType>,
        key: R::Key,
        ts: Timestamp,
        value: Option<R>,
    ) -> Result<usize, DbError<R>> {
        let timestamped_key = Timestamped::new(key, ts);

        if let (Some(log_ty), Some(wal)) = (log_ty, &self.wal) {
            let mut wal_guard = wal.lock().await;

            wal_guard
                .write(
                    log_ty,
                    timestamped_key.map(|key| unsafe { transmute(key.as_key_ref()) }),
                    value.as_ref().map(R::as_record_ref),
                )
                .await
                .unwrap();
            // .map_err(WriteError::Encode)?;
        }
        self.data.insert(timestamped_key, value);

        Ok(self.data.len())
    }

    #[allow(unused)]
    fn get(
        &self,
        key: &R::Key,
        ts: Timestamp,
    ) -> Option<Entry<'_, Timestamped<R::Key>, Option<R>>> {
        self.data
            .range::<TimestampedRef<R::Key>, _>((
                Bound::Included(TimestampedRef::new(key, ts)),
                Bound::Unbounded,
            ))
            .next()
            .and_then(|entry| {
                if &entry.key().value == key {
                    Some(entry)
                } else {
                    None
                }
            })
    }

    pub(crate) fn scan<'scan>(
        &'scan self,
        range: (Bound<&'scan R::Key>, Bound<&'scan R::Key>),
        ts: Timestamp,
    ) -> MutableScan<'scan, R> {
        let lower = range.0.map(|key| TimestampedRef::new(key, ts));
        let upper = range.1.map(|key| TimestampedRef::new(key, EPOCH));

        self.data.range((lower, upper))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub(crate) fn check_conflict(&self, key: &R::Key, ts: Timestamp) -> bool {
        self.data
            .range::<TimestampedRef<<R as Record>::Key>, _>((
                Bound::Excluded(TimestampedRef::new(key, u32::MAX.into())),
                Bound::Excluded(TimestampedRef::new(key, ts)),
            ))
            .next()
            .is_some()
    }

    pub(crate) async fn into_immutable(self) -> io::Result<(Option<FileId>, Immutable<R::Columns>)> {
        let mut file_id = None;

        if let Some(wal) = self.wal {
            let mut wal_guard = wal.lock().await;
            wal_guard.flush().await?;
            file_id = Some(wal_guard.file_id());
        }

        Ok((file_id, Immutable::from(self.data)))
    }
}

impl<R, FP> Mutable<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    #[allow(unused)]
    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use super::Mutable;
    use crate::{
        executor::tokio::TokioExecutor,
        fs::FileProvider,
        record::Record,
        tests::{Test, TestRef},
        timestamp::Timestamped,
        wal::log::LogType,
        DbOption,
    };

    #[tokio::test]
    async fn insert_and_get() {
        let key_1 = "key_1".to_owned();
        let key_2 = "key_2".to_owned();

        let temp_dir = tempfile::tempdir().unwrap();
        let option = DbOption::from(temp_dir.path());
        TokioExecutor::create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let mem_table = Mutable::<Test, TokioExecutor>::new(&option).await.unwrap();

        mem_table
            .insert(
                LogType::Full,
                Test {
                    vstring: key_1.clone(),
                    vu32: 1,
                    vbool: Some(true),
                },
                0_u32.into(),
            )
            .await
            .unwrap();
        mem_table
            .insert(
                LogType::Full,
                Test {
                    vstring: key_2.clone(),
                    vu32: 2,
                    vbool: None,
                },
                1_u32.into(),
            )
            .await
            .unwrap();

        let entry = mem_table.get(&key_1, 0_u32.into()).unwrap();
        assert_eq!(
            entry.value().as_ref().unwrap().as_record_ref(),
            TestRef {
                vstring: &key_1,
                vu32: Some(1),
                vbool: Some(true)
            }
        )
    }

    #[tokio::test]
    async fn range() {
        let temp_dir = tempfile::tempdir().unwrap();
        let option = DbOption::from(temp_dir.path());
        TokioExecutor::create_dir_all(&option.wal_dir_path())
            .await
            .unwrap();

        let mutable = Mutable::<String, TokioExecutor>::new(&option)
            .await
            .unwrap();

        mutable
            .insert(LogType::Full, "1".into(), 0_u32.into())
            .await
            .unwrap();
        mutable
            .insert(LogType::Full, "2".into(), 0_u32.into())
            .await
            .unwrap();
        mutable
            .insert(LogType::Full, "2".into(), 1_u32.into())
            .await
            .unwrap();
        mutable
            .insert(LogType::Full, "3".into(), 1_u32.into())
            .await
            .unwrap();
        mutable
            .insert(LogType::Full, "4".into(), 0_u32.into())
            .await
            .unwrap();

        let mut scan = mutable.scan((Bound::Unbounded, Bound::Unbounded), 0_u32.into());

        assert_eq!(
            scan.next().unwrap().key(),
            &Timestamped::new("1".into(), 0_u32.into())
        );
        assert_eq!(
            scan.next().unwrap().key(),
            &Timestamped::new("2".into(), 1_u32.into())
        );
        assert_eq!(
            scan.next().unwrap().key(),
            &Timestamped::new("2".into(), 0_u32.into())
        );
        assert_eq!(
            scan.next().unwrap().key(),
            &Timestamped::new("3".into(), 1_u32.into())
        );
        assert_eq!(
            scan.next().unwrap().key(),
            &Timestamped::new("4".into(), 0_u32.into())
        );

        let lower = "1".to_string();
        let upper = "4".to_string();
        let mut scan = mutable.scan(
            (Bound::Included(&lower), Bound::Included(&upper)),
            1_u32.into(),
        );

        assert_eq!(
            scan.next().unwrap().key(),
            &Timestamped::new("1".into(), 0_u32.into())
        );
        assert_eq!(
            scan.next().unwrap().key(),
            &Timestamped::new("2".into(), 1_u32.into())
        );
        assert_eq!(
            scan.next().unwrap().key(),
            &Timestamped::new("2".into(), 0_u32.into())
        );
        assert_eq!(
            scan.next().unwrap().key(),
            &Timestamped::new("3".into(), 1_u32.into())
        );
        assert_eq!(
            scan.next().unwrap().key(),
            &Timestamped::new("4".into(), 0_u32.into())
        );
    }
}
