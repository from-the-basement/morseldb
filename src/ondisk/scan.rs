use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::{ready, Stream};
use parquet::arrow::{async_reader::ParquetRecordBatchStream, ProjectionMask};
use pin_project_lite::pin_project;
use tokio::io::BufReader;

use crate::{
    fs::FileProvider,
    record::Record,
    stream::record_batch::{RecordBatchEntry, RecordBatchIterator},
};

pin_project! {
    #[derive(Debug)]
    pub struct SsTableScan<'scan, R, FP>
    where
        FP: FileProvider,
    {
        #[pin]
        stream: ParquetRecordBatchStream<BufReader<FP::File>>,
        iter: Option<RecordBatchIterator<R>>,
        projection_mask: ProjectionMask,
        _marker: PhantomData<&'scan ()>
    }
}

impl<R, FP> SsTableScan<'_, R, FP>
where
    FP: FileProvider,
{
    pub fn new(
        stream: ParquetRecordBatchStream<BufReader<FP::File>>,
        projection_mask: ProjectionMask,
    ) -> Self {
        SsTableScan {
            stream,
            iter: None,
            projection_mask,
            _marker: PhantomData,
        }
    }
}

impl<'scan, R, FP> Stream for SsTableScan<'scan, R, FP>
where
    R: Record,
    FP: FileProvider,
{
    type Item = Result<RecordBatchEntry<R>, parquet::errors::ParquetError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match this.iter {
                Some(iter) => {
                    if let Some(entry) = iter.next() {
                        return Poll::Ready(Some(Ok(entry)));
                    }
                    *this.iter = None;
                }
                None => {
                    let record_batch = ready!(this.stream.as_mut().poll_next(cx)).transpose()?;
                    let record_batch = match record_batch {
                        Some(record_batch) => record_batch,
                        None => return Poll::Ready(None),
                    };
                    *this.iter = Some(RecordBatchIterator::new(
                        record_batch,
                        this.projection_mask.clone(),
                    ));
                }
            }
        }
    }
}
