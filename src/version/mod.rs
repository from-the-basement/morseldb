pub(crate) mod cleaner;
pub(crate) mod edit;
pub(crate) mod set;

use std::{
    marker::PhantomData,
    ops::Bound,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use flume::{SendError, Sender};
use parquet::arrow::ProjectionMask;
use thiserror::Error;
use tracing::error;

use crate::{
    fs::{FileId, FileProvider},
    ondisk::sstable::SsTable,
    record::Record,
    scope::Scope,
    serdes::Encode,
    stream::{level::LevelStream, record_batch::RecordBatchEntry, ScanStream},
    timestamp::{Timestamp, TimestampedRef},
    version::cleaner::CleanTag,
    DbOption,
};

pub(crate) const MAX_LEVEL: usize = 7;

pub(crate) type VersionRef<R, FP> = Arc<Version<R, FP>>;

#[derive(Debug)]
pub(crate) struct Version<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    ts: Timestamp,
    pub(crate) level_slice: [Vec<Scope<R::Key>>; MAX_LEVEL],
    clean_sender: Sender<CleanTag>,
    option: Arc<DbOption<R>>,
    timestamp: Arc<AtomicU32>,
    _p: PhantomData<FP>,
}

impl<R, FP> Version<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    #[cfg(test)]
    pub(crate) fn new(
        option: Arc<DbOption<R>>,
        clean_sender: Sender<CleanTag>,
        timestamp: Arc<AtomicU32>,
    ) -> Self {
        Version {
            ts: Timestamp::from(0),
            level_slice: [const { Vec::new() }; MAX_LEVEL],
            clean_sender,
            option: option.clone(),
            timestamp,
            _p: Default::default(),
        }
    }

    pub(crate) fn option(&self) -> &Arc<DbOption<R>> {
        &self.option
    }

    pub(crate) fn transaction_ts(&self) -> Timestamp {
        self.timestamp.fetch_add(1, Ordering::Release).into()
    }
}

impl<R, FP> Clone for Version<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    fn clone(&self) -> Self {
        let mut level_slice = [const { Vec::new() }; MAX_LEVEL];

        for (level, scopes) in self.level_slice.iter().enumerate() {
            level_slice[level].clone_from(scopes);
        }

        Self {
            ts: self.ts,
            level_slice,
            clean_sender: self.clean_sender.clone(),
            option: self.option.clone(),
            timestamp: self.timestamp.clone(),
            _p: Default::default(),
        }
    }
}

impl<R, FP> Version<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    #[allow(unused)]
    pub(crate) async fn query(
        &self,
        key: &TimestampedRef<R::Key>,
        projection_mask: ProjectionMask,
    ) -> Result<Option<RecordBatchEntry<R>>, VersionError<R>> {
        for scope in self.level_slice[0].iter().rev() {
            if !scope.contains(key.value()) {
                continue;
            }
            if let Some(entry) = self
                .table_query(key, &scope.gen, projection_mask.clone())
                .await?
            {
                return Ok(Some(entry));
            }
        }
        for level in self.level_slice[1..6].iter() {
            if level.is_empty() {
                continue;
            }
            let index = Self::scope_search(key.value(), level);
            if !level[index].contains(key.value()) {
                continue;
            }
            if let Some(entry) = self
                .table_query(key, &level[index].gen, projection_mask.clone())
                .await?
            {
                return Ok(Some(entry));
            }
        }

        Ok(None)
    }

    #[allow(unused)]
    async fn table_query(
        &self,
        key: &TimestampedRef<<R as Record>::Key>,
        gen: &FileId,
        projection_mask: ProjectionMask,
    ) -> Result<Option<RecordBatchEntry<R>>, VersionError<R>> {
        let file = FP::open(self.option.table_path(gen))
            .await
            .map_err(VersionError::Io)?;
        SsTable::<R, FP>::open(file)
            .get(key, projection_mask)
            .await
            .map_err(VersionError::Parquet)
    }

    pub(crate) fn scope_search(key: &R::Key, level: &[Scope<R::Key>]) -> usize {
        level
            .binary_search_by(|scope| scope.min.cmp(key))
            .unwrap_or_else(|index| index.saturating_sub(1))
    }

    pub(crate) fn tables_len(&self, level: usize) -> usize {
        self.level_slice[level].len()
    }

    pub(crate) async fn streams<'streams>(
        &self,
        streams: &mut Vec<ScanStream<'streams, R, FP>>,
        range: (Bound<&'streams R::Key>, Bound<&'streams R::Key>),
        ts: Timestamp,
        limit: Option<usize>,
        projection_mask: ProjectionMask,
    ) -> Result<(), VersionError<R>> {
        for scope in self.level_slice[0].iter() {
            let file = FP::open(self.option.table_path(&scope.gen))
                .await
                .map_err(VersionError::Io)?;
            let table = SsTable::open(file);

            streams.push(ScanStream::SsTable {
                inner: table
                    .scan(range, ts, limit, projection_mask.clone())
                    .await
                    .map_err(VersionError::Parquet)?,
            })
        }
        for (i, scopes) in self.level_slice[1..].iter().enumerate() {
            if scopes.is_empty() {
                continue;
            }
            streams.push(ScanStream::Level {
                // SAFETY: checked scopes no empty
                inner: LevelStream::new(
                    self,
                    i + 1,
                    0,
                    scopes.len() - 1,
                    range,
                    ts,
                    limit,
                    projection_mask.clone(),
                )
                .unwrap(),
            });
        }
        Ok(())
    }
}

impl<R, FP> Drop for Version<R, FP>
where
    R: Record,
    FP: FileProvider,
{
    fn drop(&mut self) {
        if let Err(err) = self.clean_sender.send(CleanTag::Clean { ts: self.ts }) {
            error!("[Version Drop Error]: {}", err)
        }
    }
}

#[derive(Debug, Error)]
pub enum VersionError<R>
where
    R: Record,
{
    #[error("version encode error: {0}")]
    Encode(#[source] <R::Key as Encode>::Error),
    #[error("version io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("version parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("version send error: {0}")]
    Send(#[from] SendError<CleanTag>),
}
