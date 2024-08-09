use std::{marker::PhantomData, path::PathBuf};

use parquet::{
    basic::Compression,
    file::properties::{EnabledStatistics, WriterProperties},
};

use crate::{
    fs::{FileId, FileProvider, FileType},
    record::Record,
    version::Version,
};

/// configure the operating parameters of each component in the [`DB`](../struct.DB.html)
#[derive(Debug, Clone)]
pub struct DbOption<R> {
    pub(crate) path: PathBuf,
    pub(crate) max_mutable_len: usize,
    pub(crate) immutable_chunk_num: usize,
    pub(crate) major_threshold_with_sst_size: usize,
    pub(crate) level_sst_magnification: usize,
    pub(crate) max_sst_file_size: usize,
    pub(crate) clean_channel_buffer: usize,
    pub(crate) write_parquet_properties: WriterProperties,
    pub(crate) use_wal: bool,
    _p: PhantomData<R>,
}

impl<R, P> From<P> for DbOption<R>
where
    P: Into<PathBuf>,
    R: Record,
{
    /// build the default configured [`DbOption`](struct.DbOption.html) based on the passed path
    fn from(path: P) -> Self {
        let (column_paths, sorting_columns) = R::primary_key_path();

        DbOption {
            path: path.into(),
            max_mutable_len: 3000,
            immutable_chunk_num: 3,
            major_threshold_with_sst_size: 10,
            level_sst_magnification: 10,
            max_sst_file_size: 24 * 1024 * 1024,
            clean_channel_buffer: 10,
            write_parquet_properties: WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .set_column_statistics_enabled(column_paths.clone(), EnabledStatistics::Page)
                .set_column_bloom_filter_enabled(column_paths.clone(), true)
                .set_max_row_group_size(256)
                .set_sorting_columns(Some(sorting_columns))
                .build(),

            use_wal: true,
            _p: Default::default(),
        }
    }
}

impl<R> DbOption<R>
where
    R: Record,
{
    /// build the [`DB`](../struct.DB.html) storage directory based on the passed path
    pub fn path(self, path: impl Into<PathBuf>) -> Self {
        DbOption {
            path: path.into(),
            ..self
        }
    }

    /// len threshold of `mutable` when freeze is triggered
    pub fn max_mutable_len(self, max_mutable_len: usize) -> Self {
        DbOption {
            max_mutable_len,
            ..self
        }
    }

    /// len threshold of `immutables` when minor compaction is triggered
    pub fn immutable_chunk_num(self, immutable_chunk_num: usize) -> Self {
        DbOption {
            immutable_chunk_num,
            ..self
        }
    }

    /// threshold for the number of `parquet` when major compaction is triggered
    pub fn major_threshold_with_sst_size(self, major_threshold_with_sst_size: usize) -> Self {
        DbOption {
            major_threshold_with_sst_size,
            ..self
        }
    }

    /// magnification that triggers major compaction between different levels
    pub fn level_sst_magnification(self, level_sst_magnification: usize) -> Self {
        DbOption {
            level_sst_magnification,
            ..self
        }
    }

    /// Maximum size of each parquet
    pub fn max_sst_file_size(self, max_sst_file_size: usize) -> Self {
        DbOption {
            max_sst_file_size,
            ..self
        }
    }

    /// cached message size in parquet cleaner
    pub fn clean_channel_buffer(self, clean_channel_buffer: usize) -> Self {
        DbOption {
            clean_channel_buffer,
            ..self
        }
    }

    /// specific settings for Parquet
    pub fn write_parquet_option(self, write_parquet_properties: WriterProperties) -> Self {
        DbOption {
            write_parquet_properties,
            ..self
        }
    }

    pub fn use_wal(self, use_wal: bool) -> Self {
        DbOption { use_wal, ..self }
    }
}

impl<R> DbOption<R>
where
    R: Record,
{
    pub(crate) fn table_path(&self, gen: &FileId) -> PathBuf {
        self.path.join(format!("{}.{}", gen, FileType::Parquet))
    }

    pub(crate) fn wal_dir_path(&self) -> PathBuf {
        self.path.join("wal")
    }

    pub(crate) fn wal_path(&self, gen: &FileId) -> PathBuf {
        self.wal_dir_path()
            .join(format!("{}.{}", gen, FileType::Wal))
    }

    pub(crate) fn version_path(&self) -> PathBuf {
        self.path.join(format!("version.{}", FileType::Log))
    }

    pub(crate) fn is_threshold_exceeded_major<E>(
        &self,
        version: &Version<R, E>,
        level: usize,
    ) -> bool
    where
        E: FileProvider,
    {
        Version::<R, E>::tables_len(version, level)
            >= (self.major_threshold_with_sst_size * self.level_sst_magnification.pow(level as u32))
    }
}
