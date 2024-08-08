use std::{
    collections::{Bound, VecDeque},
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures_core::Stream;
use parquet::{arrow::ProjectionMask, errors::ParquetError};

use crate::{
    fs::{FileId, FileProvider},
    ondisk::{scan::SsTableScan, sstable::SsTable},
    record::Record,
    scope::Scope,
    stream::record_batch::RecordBatchEntry,
    timestamp::Timestamp,
    version::Version,
    DbOption,
};

enum FutureStatus<'level, R, FP>
where
    R: Record,
    FP: FileProvider,
{
    Init(FileId),
    Ready(SsTableScan<'level, R, FP>),
    OpenFile(Pin<Box<dyn Future<Output = io::Result<FP::File>> + Send + 'level>>),
    LoadStream(
        Pin<
            Box<
                dyn Future<Output = Result<SsTableScan<'level, R, FP>, ParquetError>>
                    + Send
                    + 'level,
            >,
        >,
    ),
}

pub(crate) struct LevelStream<'level, R, FP>
where
    R: Record,
    FP: FileProvider,
{
    lower: Bound<&'level R::Key>,
    upper: Bound<&'level R::Key>,
    ts: Timestamp,
    option: Arc<DbOption<R>>,
    gens: VecDeque<FileId>,
    limit: Option<usize>,
    projection_mask: ProjectionMask,
    status: FutureStatus<'level, R, FP>,
}

impl<'level, R, FP> LevelStream<'level, R, FP>
where
    R: Record,
    FP: FileProvider,
{
    // Kould: only used by Compaction now, and the start and end of the sstables range are known
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        version: &Version<R, FP>,
        level: usize,
        start: usize,
        end: usize,
        range: (Bound<&'level R::Key>, Bound<&'level R::Key>),
        ts: Timestamp,
        limit: Option<usize>,
        projection_mask: ProjectionMask,
    ) -> Option<Self> {
        let (lower, upper) = range;
        let mut gens: VecDeque<FileId> = version.level_slice[level][start..end + 1]
            .iter()
            .map(Scope::gen)
            .collect();
        let first_gen = gens.pop_front()?;
        let status = FutureStatus::Init(first_gen);

        Some(LevelStream {
            lower,
            upper,
            ts,
            option: version.option().clone(),
            gens,
            limit,
            projection_mask,
            status,
        })
    }
}

impl<'level, R, FP> Stream for LevelStream<'level, R, FP>
where
    R: Record,
    FP: FileProvider + 'level,
{
    type Item = Result<RecordBatchEntry<R>, ParquetError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            return match &mut self.status {
                FutureStatus::Init(gen) => {
                    let gen = *gen;
                    self.status =
                        FutureStatus::OpenFile(Box::pin(FP::open(self.option.table_path(&gen))));
                    continue;
                }
                FutureStatus::Ready(stream) => match Pin::new(stream).poll_next(cx) {
                    Poll::Ready(None) => match self.gens.pop_front() {
                        None => Poll::Ready(None),
                        Some(gen) => {
                            self.status = FutureStatus::OpenFile(Box::pin(FP::open(
                                self.option.table_path(&gen),
                            )));
                            continue;
                        }
                    },
                    Poll::Ready(Some(result)) => {
                        if let Some(limit) = &mut self.limit {
                            *limit -= 1;
                        }
                        Poll::Ready(Some(result))
                    }
                    Poll::Pending => Poll::Pending,
                },
                FutureStatus::OpenFile(file_future) => match Pin::new(file_future).poll(cx) {
                    Poll::Ready(Ok(file)) => {
                        self.status = FutureStatus::LoadStream(Box::pin(SsTable::open(file).scan(
                            (self.lower, self.upper),
                            self.ts,
                            self.limit,
                            self.projection_mask.clone(),
                        )));
                        continue;
                    }
                    Poll::Ready(Err(err)) => Poll::Ready(Some(Err(ParquetError::from(err)))),
                    Poll::Pending => Poll::Pending,
                },
                FutureStatus::LoadStream(stream_future) => match Pin::new(stream_future).poll(cx) {
                    Poll::Ready(Ok(scan)) => {
                        self.status = FutureStatus::Ready(scan);
                        continue;
                    }
                    Poll::Ready(Err(err)) => Poll::Ready(Some(Err(err))),
                    Poll::Pending => Poll::Pending,
                },
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::Bound, sync::Arc};

    use futures_util::StreamExt;
    use parquet::arrow::{arrow_to_parquet_schema, ProjectionMask};
    use tempfile::TempDir;

    use crate::{
        compaction::tests::build_version, record::Record, stream::level::LevelStream, tests::Test,
        DbOption,
    };

    #[tokio::test]
    async fn projection_scan() {
        let temp_dir = TempDir::new().unwrap();
        let option = Arc::new(DbOption::from(temp_dir.path()));

        let (_, version) = build_version(&option).await;

        {
            let mut level_stream_1 = LevelStream::new(
                &version,
                0,
                0,
                1,
                (Bound::Unbounded, Bound::Unbounded),
                1_u32.into(),
                None,
                ProjectionMask::roots(
                    &arrow_to_parquet_schema(Test::arrow_schema()).unwrap(),
                    [0, 1, 2, 3],
                ),
            )
            .unwrap();

            let entry_0 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_0.get().unwrap().vu32.is_some());
            assert!(entry_0.get().unwrap().vbool.is_none());
            let entry_1 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_1.get().unwrap().vu32.is_some());
            assert!(entry_1.get().unwrap().vbool.is_none());
            let entry_2 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_2.get().unwrap().vu32.is_some());
            assert!(entry_2.get().unwrap().vbool.is_none());
            let entry_3 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_3.get().unwrap().vu32.is_some());
            assert!(entry_3.get().unwrap().vbool.is_none());
            let entry_4 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_4.get().unwrap().vu32.is_some());
            assert!(entry_4.get().unwrap().vbool.is_none());
            let entry_5 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_5.get().unwrap().vu32.is_some());
            assert!(entry_5.get().unwrap().vbool.is_none());
        }
        {
            let mut level_stream_1 = LevelStream::new(
                &version,
                0,
                0,
                1,
                (Bound::Unbounded, Bound::Unbounded),
                1_u32.into(),
                None,
                ProjectionMask::roots(
                    &arrow_to_parquet_schema(Test::arrow_schema()).unwrap(),
                    [0, 1, 2, 4],
                ),
            )
            .unwrap();

            let entry_0 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_0.get().unwrap().vu32.is_none());
            assert!(entry_0.get().unwrap().vbool.is_some());
            let entry_1 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_1.get().unwrap().vu32.is_none());
            assert!(entry_1.get().unwrap().vbool.is_some());
            let entry_2 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_2.get().unwrap().vu32.is_none());
            assert!(entry_2.get().unwrap().vbool.is_some());
            let entry_3 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_3.get().unwrap().vu32.is_none());
            assert!(entry_3.get().unwrap().vbool.is_some());
            let entry_4 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_4.get().unwrap().vu32.is_none());
            assert!(entry_4.get().unwrap().vbool.is_some());
            let entry_5 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_5.get().unwrap().vu32.is_none());
            assert!(entry_5.get().unwrap().vbool.is_some());
        }
        {
            let mut level_stream_1 = LevelStream::new(
                &version,
                0,
                0,
                1,
                (Bound::Unbounded, Bound::Unbounded),
                1_u32.into(),
                None,
                ProjectionMask::roots(
                    &arrow_to_parquet_schema(Test::arrow_schema()).unwrap(),
                    [0, 1, 2],
                ),
            )
            .unwrap();

            let entry_0 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_0.get().unwrap().vu32.is_none());
            assert!(entry_0.get().unwrap().vbool.is_none());
            let entry_1 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_1.get().unwrap().vu32.is_none());
            assert!(entry_1.get().unwrap().vbool.is_none());
            let entry_2 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_2.get().unwrap().vu32.is_none());
            assert!(entry_2.get().unwrap().vbool.is_none());
            let entry_3 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_3.get().unwrap().vu32.is_none());
            assert!(entry_3.get().unwrap().vbool.is_none());
            let entry_4 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_4.get().unwrap().vu32.is_none());
            assert!(entry_4.get().unwrap().vbool.is_none());
            let entry_5 = level_stream_1.next().await.unwrap().unwrap();
            assert!(entry_5.get().unwrap().vu32.is_none());
            assert!(entry_5.get().unwrap().vbool.is_none());
        }
    }
}
