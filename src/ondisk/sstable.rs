use std::{marker::PhantomData, ops::Bound};

use futures_util::StreamExt;
use parquet::{
    arrow::{
        arrow_reader::{ArrowReaderBuilder, ArrowReaderOptions},
        arrow_writer::ArrowWriterOptions,
        async_reader::AsyncReader,
        AsyncArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask,
    },
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use tokio::io::BufReader;

use super::{arrows::get_range_filter, scan::SsTableScan};
use crate::{
    fs::{AsyncFile, FileProvider},
    record::Record,
    stream::record_batch::RecordBatchEntry,
    timestamp::{Timestamp, TimestampedRef},
};

pub(crate) struct SsTable<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    reader: BufReader<FP::File>,
    _marker: PhantomData<R>,
}

impl<R, FP> SsTable<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    pub(crate) fn open(file: FP::File) -> Self {
        SsTable {
            reader: BufReader::new(file),
            _marker: PhantomData,
        }
    }

    #[allow(unused)]
    fn create_writer(&mut self) -> AsyncArrowWriter<&mut dyn AsyncFile> {
        // TODO: expose writer options
        let options = ArrowWriterOptions::new().with_properties(
            WriterProperties::builder()
                .set_created_by(concat!("tonbo version ", env!("CARGO_PKG_VERSION")).to_owned())
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
                .build(),
        );
        AsyncArrowWriter::try_new_with_options(
            (&mut self.reader as &mut dyn AsyncFile),
            R::arrow_schema().clone(),
            options,
        )
        .expect("Failed to create writer")
    }

    #[cfg(test)]
    async fn write(
        &mut self,
        record_batch: arrow::array::RecordBatch,
    ) -> parquet::errors::Result<()> {
        let mut writer = self.create_writer();
        writer.write(&record_batch).await?;

        if writer.in_progress_size() > (1 << 20) - 1 {
            writer.flush().await?;
        }

        writer.close().await?;
        Ok(())
    }

    async fn into_parquet_builder(
        self,
        limit: Option<usize>,
        projection_mask: ProjectionMask,
    ) -> parquet::errors::Result<ArrowReaderBuilder<AsyncReader<BufReader<FP::File>>>> {
        let mut builder = ParquetRecordBatchStreamBuilder::new_with_options(
            self.reader,
            ArrowReaderOptions::default().with_page_index(true),
        )
        .await?;
        if let Some(limit) = limit {
            builder = builder.with_limit(limit);
        }
        Ok(builder.with_projection(projection_mask))
    }

    pub(crate) async fn get(
        self,
        key: &TimestampedRef<R::Key>,
        projection_mask: ProjectionMask,
    ) -> parquet::errors::Result<Option<RecordBatchEntry<R>>> {
        self.scan(
            (Bound::Included(key.value()), Bound::Unbounded),
            key.ts(),
            Some(1),
            projection_mask,
        )
        .await?
        .next()
        .await
        .transpose()
    }

    pub(crate) async fn scan<'scan>(
        self,
        range: (Bound<&'scan R::Key>, Bound<&'scan R::Key>),
        ts: Timestamp,
        limit: Option<usize>,
        projection_mask: ProjectionMask,
    ) -> Result<SsTableScan<R, FP>, parquet::errors::ParquetError> {
        let builder = self
            .into_parquet_builder(limit, projection_mask.clone())
            .await?;

        let schema_descriptor = builder.metadata().file_metadata().schema_descr();

        // Safety: filter's lifetime relies on range's lifetime, sstable must not live longer than
        // it
        let filter = unsafe { get_range_filter::<R>(schema_descriptor, range, ts) };

        Ok(SsTableScan::new(
            builder.with_row_filter(filter).build()?,
            projection_mask,
        ))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{borrow::Borrow, ops::Bound, path::PathBuf};

    use futures_util::StreamExt;
    use parquet::arrow::{arrow_to_parquet_schema, ProjectionMask};

    use super::SsTable;
    use crate::{
        executor::tokio::TokioExecutor,
        fs::FileProvider,
        record::Record,
        tests::{get_test_record_batch, Test, TestRef},
        timestamp::Timestamped,
        DbOption,
    };

    pub(crate) async fn open_sstable<R, FP>(path: &PathBuf) -> SsTable<R, FP>
    where
        R: Record,
        FP: FileProvider,
    {
        SsTable::open(FP::open(path).await.unwrap())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_sstable() {
        let temp_dir = tempfile::tempdir().unwrap();
        let record_batch = get_test_record_batch::<TokioExecutor>(
            DbOption::from(temp_dir.path()),
            TokioExecutor::new(),
        )
        .await;
        let table_path = temp_dir.path().join("write_test.parquet");

        open_sstable::<Test, TokioExecutor>(&table_path)
            .await
            .write(record_batch)
            .await
            .unwrap();

        let key = Timestamped::new("hello".to_owned(), 1.into());

        assert_eq!(
            open_sstable::<Test, TokioExecutor>(&table_path)
                .await
                .get(key.borrow(), ProjectionMask::all())
                .await
                .unwrap()
                .unwrap()
                .get(),
            Some(TestRef {
                vstring: "hello",
                vu32: Some(12),
                vbool: Some(true),
            })
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn projection_query() {
        let temp_dir = tempfile::tempdir().unwrap();
        let record_batch = get_test_record_batch::<TokioExecutor>(
            DbOption::from(temp_dir.path()),
            TokioExecutor::new(),
        )
        .await;
        let table_path = temp_dir.path().join("projection_query_test.parquet");

        open_sstable::<Test, TokioExecutor>(&table_path)
            .await
            .write(record_batch)
            .await
            .unwrap();

        let key = Timestamped::new("hello".to_owned(), 1.into());

        {
            let test_ref_1 = open_sstable::<Test, TokioExecutor>(&table_path)
                .await
                .get(
                    key.borrow(),
                    ProjectionMask::roots(
                        &arrow_to_parquet_schema(Test::arrow_schema()).unwrap(),
                        [0, 1, 2, 3],
                    ),
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(test_ref_1.get().unwrap().vstring, "hello");
            assert_eq!(test_ref_1.get().unwrap().vu32, Some(12));
            assert_eq!(test_ref_1.get().unwrap().vbool, None);
        }
        {
            let test_ref_2 = open_sstable::<Test, TokioExecutor>(&table_path)
                .await
                .get(
                    key.borrow(),
                    ProjectionMask::roots(
                        &arrow_to_parquet_schema(Test::arrow_schema()).unwrap(),
                        [0, 1, 2, 4],
                    ),
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(test_ref_2.get().unwrap().vstring, "hello");
            assert_eq!(test_ref_2.get().unwrap().vu32, None);
            assert_eq!(test_ref_2.get().unwrap().vbool, Some(true));
        }
        {
            let test_ref_3 = open_sstable::<Test, TokioExecutor>(&table_path)
                .await
                .get(
                    key.borrow(),
                    ProjectionMask::roots(
                        &arrow_to_parquet_schema(Test::arrow_schema()).unwrap(),
                        [0, 1, 2],
                    ),
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(test_ref_3.get().unwrap().vstring, "hello");
            assert_eq!(test_ref_3.get().unwrap().vu32, None);
            assert_eq!(test_ref_3.get().unwrap().vbool, None);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn projection_scan() {
        let temp_dir = tempfile::tempdir().unwrap();
        let record_batch = get_test_record_batch::<TokioExecutor>(
            DbOption::from(temp_dir.path()),
            TokioExecutor::new(),
        )
        .await;
        let table_path = temp_dir.path().join("projection_scan_test.parquet");

        open_sstable::<Test, TokioExecutor>(&table_path)
            .await
            .write(record_batch)
            .await
            .unwrap();

        {
            let mut test_ref_1 = open_sstable::<Test, TokioExecutor>(&table_path)
                .await
                .scan(
                    (Bound::Unbounded, Bound::Unbounded),
                    1_u32.into(),
                    None,
                    ProjectionMask::roots(
                        &arrow_to_parquet_schema(Test::arrow_schema()).unwrap(),
                        [0, 1, 2, 3],
                    ),
                )
                .await
                .unwrap();

            let entry_0 = test_ref_1.next().await.unwrap().unwrap();
            assert_eq!(entry_0.get().unwrap().vstring, "hello");
            assert_eq!(entry_0.get().unwrap().vu32, Some(12));
            assert_eq!(entry_0.get().unwrap().vbool, None);

            let entry_1 = test_ref_1.next().await.unwrap().unwrap();
            assert_eq!(entry_1.get().unwrap().vstring, "world");
            assert_eq!(entry_1.get().unwrap().vu32, Some(12));
            assert_eq!(entry_1.get().unwrap().vbool, None);
        }
        {
            let mut test_ref_2 = open_sstable::<Test, TokioExecutor>(&table_path)
                .await
                .scan(
                    (Bound::Unbounded, Bound::Unbounded),
                    1_u32.into(),
                    None,
                    ProjectionMask::roots(
                        &arrow_to_parquet_schema(Test::arrow_schema()).unwrap(),
                        [0, 1, 2, 4],
                    ),
                )
                .await
                .unwrap();

            let entry_0 = test_ref_2.next().await.unwrap().unwrap();
            assert_eq!(entry_0.get().unwrap().vstring, "hello");
            assert_eq!(entry_0.get().unwrap().vu32, None);
            assert_eq!(entry_0.get().unwrap().vbool, Some(true));

            let entry_1 = test_ref_2.next().await.unwrap().unwrap();
            assert_eq!(entry_1.get().unwrap().vstring, "world");
            assert_eq!(entry_1.get().unwrap().vu32, None);
            assert_eq!(entry_1.get().unwrap().vbool, None);
        }
        {
            let mut test_ref_3 = open_sstable::<Test, TokioExecutor>(&table_path)
                .await
                .scan(
                    (Bound::Unbounded, Bound::Unbounded),
                    1_u32.into(),
                    None,
                    ProjectionMask::roots(
                        &arrow_to_parquet_schema(Test::arrow_schema()).unwrap(),
                        [0, 1, 2],
                    ),
                )
                .await
                .unwrap();

            let entry_0 = test_ref_3.next().await.unwrap().unwrap();
            assert_eq!(entry_0.get().unwrap().vstring, "hello");
            assert_eq!(entry_0.get().unwrap().vu32, None);
            assert_eq!(entry_0.get().unwrap().vbool, None);

            let entry_1 = test_ref_3.next().await.unwrap().unwrap();
            assert_eq!(entry_1.get().unwrap().vstring, "world");
            assert_eq!(entry_1.get().unwrap().vu32, None);
            assert_eq!(entry_1.get().unwrap().vbool, None);
        }
    }
}
