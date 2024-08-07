use std::{collections::Bound, fs, fs::File, path::Path};

use async_stream::stream;
use futures_core::Stream;
use futures_util::StreamExt;
use parquet::data_type::AsBytes;
use redb::TableDefinition;
use rocksdb::{Direction, IteratorMode, TransactionDB};
use tonbo::{executor::tokio::TokioExecutor, record::KeyRef, Projection};
use tonbo_marco::tonbo_record;

#[allow(dead_code)]
const X: TableDefinition<&[u8], &[u8]> = TableDefinition::new("x");

type ItemKey = String;
type ProjectionField = String;

#[tonbo_record(::serde::Serialize, ::serde::Deserialize)]
pub struct BenchItem {
    #[primary_key]
    pub primary_key: String,
    pub string: String,
    pub u32: u32,
    pub boolean: bool,
}

pub trait BenchDatabase {
    type W<'db>: BenchWriteTransaction
    where
        Self: 'db;
    type R<'db>: BenchReadTransaction
    where
        Self: 'db;

    fn db_type_name() -> &'static str;

    async fn write_transaction(&self) -> Self::W<'_>;

    async fn read_transaction(&self) -> Self::R<'_>;
}

pub trait BenchWriteTransaction {
    type W<'txn>: BenchInserter
    where
        Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_>;

    #[allow(clippy::result_unit_err)]
    async fn commit(self) -> Result<(), ()>;
}

pub trait BenchInserter {
    #[allow(clippy::result_unit_err)]
    fn insert(&mut self, record: BenchItem) -> Result<(), ()>;

    #[allow(clippy::result_unit_err)]
    fn remove(&mut self, key: ItemKey) -> Result<(), ()>;
}

pub trait BenchReadTransaction {
    type T<'txn>: BenchReader
    where
        Self: 'txn;

    fn get_reader(&self) -> Self::T<'_>;
}

#[allow(clippy::len_without_is_empty)]
pub trait BenchReader {
    async fn get<'a>(&'a self, key: &'a ItemKey) -> Option<BenchItem>;

    fn range_from<'a>(
        &'a self,
        range: (Bound<&'a ItemKey>, Bound<&'a ItemKey>),
    ) -> impl Stream<Item = BenchItem> + 'a;

    fn projection_range_from<'a>(
        &'a self,
        range: (Bound<&'a ItemKey>, Bound<&'a ItemKey>),
    ) -> impl Stream<Item = String> + 'a;
}

pub struct TonboBenchDataBase<'a> {
    db: &'a tonbo::DB<BenchItem, TokioExecutor>,
}

impl<'a> TonboBenchDataBase<'a> {
    #[allow(dead_code)]
    pub fn new(db: &'a tonbo::DB<BenchItem, TokioExecutor>) -> Self {
        TonboBenchDataBase { db }
    }
}

impl<'a> BenchDatabase for TonboBenchDataBase<'a> {
    type W<'db> = TonboBenchWriteTransaction<'db> where Self: 'db;
    type R<'db> = TonboBenchReadTransaction<'db> where Self: 'db;

    fn db_type_name() -> &'static str {
        "tonbo"
    }

    async fn write_transaction(&self) -> Self::W<'_> {
        TonboBenchWriteTransaction {
            txn: self.db.transaction().await,
        }
    }

    async fn read_transaction(&self) -> Self::R<'_> {
        TonboBenchReadTransaction {
            txn: self.db.transaction().await,
        }
    }
}

pub struct TonboBenchReadTransaction<'a> {
    txn: tonbo::transaction::Transaction<'a, BenchItem, TokioExecutor>,
}

impl<'db> BenchReadTransaction for TonboBenchReadTransaction<'db> {
    type T<'txn> = TonboBenchReader<'db, 'txn> where Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        TonboBenchReader { txn: &self.txn }
    }
}

pub struct TonboBenchReader<'db, 'txn> {
    txn: &'txn tonbo::transaction::Transaction<'db, BenchItem, TokioExecutor>,
}

impl BenchReader for TonboBenchReader<'_, '_> {
    async fn get<'a>(&'a self, key: &'a ItemKey) -> Option<BenchItem> {
        self.txn
            .get(key, Projection::All)
            .await
            .unwrap()
            .map(|entry| BenchItem {
                primary_key: entry.get().primary_key.to_key(),
                string: entry.get().string.unwrap().to_string(),
                u32: entry.get().u32.unwrap(),
                boolean: entry.get().boolean.unwrap(),
            })
    }

    fn range_from<'a>(
        &'a self,
        range: (Bound<&'a ItemKey>, Bound<&'a ItemKey>),
    ) -> impl Stream<Item = BenchItem> + 'a {
        stream! {
            let mut stream = self.txn.scan(range).await.take().await.unwrap();

            while let Some(result) = stream.next().await {
                if let Some(item_ref) = result.unwrap().value() {
                    yield BenchItem {
                        primary_key: item_ref.primary_key.to_key(),
                        string: item_ref.string.unwrap().to_string(),
                        u32: item_ref.u32.unwrap(),
                        boolean: item_ref.boolean.unwrap(),
                    };
                }
            }
        }
    }

    fn projection_range_from<'a>(
        &'a self,
        range: (Bound<&'a ItemKey>, Bound<&'a ItemKey>),
    ) -> impl Stream<Item = ProjectionField> + 'a {
        stream! {
            let mut stream = self.txn.scan(range).await.projection(vec![1]).take().await.unwrap();

            while let Some(result) = stream.next().await {
                if let Some(item_ref) = result.unwrap().value() {
                    yield item_ref.string.unwrap().to_string();
                }
            }
        }
    }
}

pub struct TonboBenchWriteTransaction<'a> {
    txn: tonbo::transaction::Transaction<'a, BenchItem, TokioExecutor>,
}

impl<'db> BenchWriteTransaction for TonboBenchWriteTransaction<'db> {
    type W<'txn> = TonboBenchInserter<'db, 'txn> where Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        TonboBenchInserter { txn: &mut self.txn }
    }

    async fn commit(self) -> Result<(), ()> {
        self.txn.commit().await.unwrap();
        Ok(())
    }
}

pub struct TonboBenchInserter<'db, 'txn> {
    txn: &'txn mut tonbo::transaction::Transaction<'db, BenchItem, TokioExecutor>,
}

impl BenchInserter for TonboBenchInserter<'_, '_> {
    fn insert(&mut self, record: BenchItem) -> Result<(), ()> {
        self.txn.insert(record);
        Ok(())
    }

    fn remove(&mut self, key: ItemKey) -> Result<(), ()> {
        self.txn.remove(key);
        Ok(())
    }
}

pub struct RedbBenchDatabase<'a> {
    db: &'a redb::Database,
}

impl<'a> RedbBenchDatabase<'a> {
    #[allow(dead_code)]
    pub fn new(db: &'a redb::Database) -> Self {
        RedbBenchDatabase { db }
    }
}

impl<'a> BenchDatabase for RedbBenchDatabase<'a> {
    type W<'db> = RedbBenchWriteTransaction where Self: 'db;
    type R<'db> = RedbBenchReadTransaction where Self: 'db;

    fn db_type_name() -> &'static str {
        "redb"
    }

    async fn write_transaction(&self) -> Self::W<'_> {
        let txn = self.db.begin_write().unwrap();
        RedbBenchWriteTransaction { txn }
    }

    async fn read_transaction(&self) -> Self::R<'_> {
        let txn = self.db.begin_read().unwrap();
        RedbBenchReadTransaction { txn }
    }
}

pub struct RedbBenchReadTransaction {
    txn: redb::ReadTransaction,
}

impl BenchReadTransaction for RedbBenchReadTransaction {
    type T<'txn> = RedbBenchReader where Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        let table = self.txn.open_table(X).unwrap();
        RedbBenchReader { table }
    }
}

pub struct RedbBenchReader {
    table: redb::ReadOnlyTable<&'static [u8], &'static [u8]>,
}

impl BenchReader for RedbBenchReader {
    async fn get<'a>(&'a self, key: &'a ItemKey) -> Option<BenchItem> {
        self.table
            .get(key.as_bytes())
            .unwrap()
            .map(|guard| bincode::deserialize::<BenchItem>(guard.value()).unwrap())
    }

    fn range_from<'a>(
        &'a self,
        range: (Bound<&'a ItemKey>, Bound<&'a ItemKey>),
    ) -> impl Stream<Item = BenchItem> + 'a {
        let (lower, upper) = range;
        let mut iter = self
            .table
            .range::<&[u8]>((lower.map(ItemKey::as_bytes), upper.map(ItemKey::as_bytes)))
            .unwrap();

        stream! {
            while let Some(item) = iter.next() {
                let (_, v) = item.unwrap();
                yield bincode::deserialize(v.value()).unwrap()
            }
        }
    }

    fn projection_range_from<'a>(
        &'a self,
        range: (Bound<&'a ItemKey>, Bound<&'a ItemKey>),
    ) -> impl Stream<Item = ProjectionField> + 'a {
        let (lower, upper) = range;
        let mut iter = self
            .table
            .range::<&[u8]>((lower.map(ItemKey::as_bytes), upper.map(ItemKey::as_bytes)))
            .unwrap();

        stream! {
            while let Some(item) = iter.next() {
                let (_, v) = item.unwrap();
                yield bincode::deserialize::<BenchItem>(v.value()).unwrap().string
            }
        }
    }
}

pub struct RedbBenchWriteTransaction {
    txn: redb::WriteTransaction,
}

impl BenchWriteTransaction for RedbBenchWriteTransaction {
    type W<'txn> = RedbBenchInserter<'txn> where Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        let table = self.txn.open_table(X).unwrap();
        RedbBenchInserter { table }
    }

    async fn commit(self) -> Result<(), ()> {
        self.txn.commit().map_err(|_| ())
    }
}

pub struct RedbBenchInserter<'txn> {
    table: redb::Table<'txn, &'static [u8], &'static [u8]>,
}

impl BenchInserter for RedbBenchInserter<'_> {
    fn insert(&mut self, record: BenchItem) -> Result<(), ()> {
        self.table
            .insert(
                record.primary_key.as_bytes(),
                bincode::serialize(&record).unwrap().as_bytes(),
            )
            .map(|_| ())
            .map_err(|_| ())
    }

    fn remove(&mut self, key: ItemKey) -> Result<(), ()> {
        self.table
            .remove(key.as_bytes())
            .map(|_| ())
            .map_err(|_| ())
    }
}

pub struct SledBenchDatabase<'a> {
    db: &'a sled::Db,
    db_dir: &'a Path,
}

impl<'a> SledBenchDatabase<'a> {
    pub fn new(db: &'a sled::Db, path: &'a Path) -> Self {
        SledBenchDatabase { db, db_dir: path }
    }
}

impl<'a> BenchDatabase for SledBenchDatabase<'a> {
    type W<'db>
    = SledBenchWriteTransaction<'db>
    where
        Self: 'db;
    type R<'db>
    = SledBenchReadTransaction<'db>
    where
        Self: 'db;

    fn db_type_name() -> &'static str {
        "sled"
    }

    async fn write_transaction(&self) -> Self::W<'_> {
        SledBenchWriteTransaction {
            db: &self.db,
            db_dir: &self.db_dir,
        }
    }

    async fn read_transaction(&self) -> Self::R<'_> {
        SledBenchReadTransaction { db: &self.db }
    }
}

pub struct SledBenchReadTransaction<'db> {
    db: &'db sled::Db,
}

impl BenchReadTransaction for SledBenchReadTransaction<'_> {
    type T<'txn>
    = SledBenchReader<'txn>
    where
        Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        SledBenchReader { db: &self.db }
    }
}

pub struct SledBenchReader<'db> {
    db: &'db sled::Db,
}

impl<'db> BenchReader for SledBenchReader<'db> {
    async fn get<'a>(&'a self, key: &'a ItemKey) -> Option<BenchItem> {
        self.db
            .get(key.as_bytes())
            .unwrap()
            .map(|guard| bincode::deserialize::<BenchItem>(guard.as_bytes()).unwrap())
    }

    fn range_from<'a>(
        &'a self,
        range: (Bound<&'a ItemKey>, Bound<&'a ItemKey>),
    ) -> impl Stream<Item = BenchItem> + 'a {
        let (lower, upper) = range;
        let mut iter = self.db.range::<&[u8], (Bound<&[u8]>, Bound<&[u8]>)>((
            lower.map(ItemKey::as_bytes),
            upper.map(ItemKey::as_bytes),
        ));

        stream! {
            while let Some(item) = iter.next() {
                let (_, v) = item.unwrap();
                yield bincode::deserialize(v.as_bytes()).unwrap()
            }
        }
    }

    fn projection_range_from<'a>(
        &'a self,
        range: (Bound<&'a ItemKey>, Bound<&'a ItemKey>),
    ) -> impl Stream<Item = ProjectionField> + 'a {
        let (lower, upper) = range;
        let mut iter = self.db.range::<&[u8], (Bound<&[u8]>, Bound<&[u8]>)>((
            lower.map(ItemKey::as_bytes),
            upper.map(ItemKey::as_bytes),
        ));

        stream! {
            while let Some(item) = iter.next() {
                let (_, v) = item.unwrap();
                yield bincode::deserialize::<BenchItem>(v.as_bytes()).unwrap().string
            }
        }
    }
}

pub struct SledBenchWriteTransaction<'a> {
    db: &'a sled::Db,
    db_dir: &'a Path,
}

impl BenchWriteTransaction for SledBenchWriteTransaction<'_> {
    type W<'txn>
    = SledBenchInserter<'txn>
    where
        Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        SledBenchInserter { db: &self.db }
    }

    async fn commit(self) -> Result<(), ()> {
        self.db.flush().unwrap();
        // Workaround for sled durability
        // Fsync all the files, because sled doesn't guarantee durability (it uses
        // sync_file_range()) See: https://github.com/spacejam/sled/issues/1351
        for entry in fs::read_dir(self.db_dir).unwrap() {
            let entry = entry.unwrap();
            if entry.path().is_file() {
                let file = File::open(entry.path()).unwrap();
                let _ = file.sync_all();
            }
        }
        Ok(())
    }
}

pub struct SledBenchInserter<'a> {
    db: &'a sled::Db,
}

impl<'a> BenchInserter for SledBenchInserter<'a> {
    fn insert(&mut self, record: BenchItem) -> Result<(), ()> {
        self.db
            .insert(
                record.primary_key.as_bytes(),
                bincode::serialize(&record).unwrap().as_bytes(),
            )
            .map(|_| ())
            .map_err(|_| ())
    }

    fn remove(&mut self, key: ItemKey) -> Result<(), ()> {
        self.db.remove(key.as_bytes()).map(|_| ()).map_err(|_| ())
    }
}

pub struct RocksdbBenchDatabase<'a> {
    db: &'a TransactionDB,
}

impl<'a> RocksdbBenchDatabase<'a> {
    pub fn new(db: &'a TransactionDB) -> Self {
        Self { db }
    }
}

impl<'a> BenchDatabase for RocksdbBenchDatabase<'a> {
    type W<'db>
    = RocksdbBenchWriteTransaction<'db>
    where
        Self: 'db;
    type R<'db>
    = RocksdbBenchReadTransaction<'db>
    where
        Self: 'db;

    fn db_type_name() -> &'static str {
        "rocksdb"
    }

    async fn write_transaction(&self) -> Self::W<'_> {
        RocksdbBenchWriteTransaction {
            txn: self.db.transaction(),
        }
    }

    async fn read_transaction(&self) -> Self::R<'_> {
        RocksdbBenchReadTransaction {
            snapshot: self.db.snapshot(),
        }
    }
}

pub struct RocksdbBenchWriteTransaction<'a> {
    txn: rocksdb::Transaction<'a, TransactionDB>,
}

impl<'a> BenchWriteTransaction for RocksdbBenchWriteTransaction<'a> {
    type W<'txn>
    = RocksdbBenchInserter<'txn>
    where
        Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        RocksdbBenchInserter { txn: &self.txn }
    }

    async fn commit(self) -> Result<(), ()> {
        self.txn.commit().map_err(|_| ())
    }
}

pub struct RocksdbBenchInserter<'a> {
    txn: &'a rocksdb::Transaction<'a, TransactionDB>,
}

impl BenchInserter for RocksdbBenchInserter<'_> {
    fn insert(&mut self, record: BenchItem) -> Result<(), ()> {
        self.txn
            .put(
                record.primary_key.as_bytes(),
                bincode::serialize(&record).unwrap().as_bytes(),
            )
            .map(|_| ())
            .map_err(|_| ())
    }

    fn remove(&mut self, key: ItemKey) -> Result<(), ()> {
        self.txn.delete(key.as_bytes()).map(|_| ()).map_err(|_| ())
    }
}

pub struct RocksdbBenchReadTransaction<'db> {
    snapshot: rocksdb::SnapshotWithThreadMode<'db, TransactionDB>,
}

impl<'db> BenchReadTransaction for RocksdbBenchReadTransaction<'db> {
    type T<'txn>
    = RocksdbBenchReader<'db, 'txn>
    where
        Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        RocksdbBenchReader {
            snapshot: &self.snapshot,
        }
    }
}

pub struct RocksdbBenchReader<'db, 'txn> {
    snapshot: &'txn rocksdb::SnapshotWithThreadMode<'db, TransactionDB>,
}

impl<'db, 'txn> BenchReader for RocksdbBenchReader<'db, 'txn> {
    async fn get<'a>(&'a self, key: &'a ItemKey) -> Option<BenchItem> {
        self.snapshot
            .get(key.as_bytes())
            .unwrap()
            .map(|bytes| bincode::deserialize::<BenchItem>(&bytes).unwrap())
    }

    fn range_from<'a>(
        &'a self,
        range: (Bound<&'a ItemKey>, Bound<&'a ItemKey>),
    ) -> impl Stream<Item = BenchItem> + 'a {
        fn bound_to_include(bound: Bound<&[u8]>) -> Option<&[u8]> {
            match bound {
                Bound::Included(bytes) | Bound::Excluded(bytes) => Some(bytes),
                Bound::Unbounded => None,
            }
        }

        let (lower, upper) = range;
        let lower = bound_to_include(lower.map(String::as_bytes))
            .map(|bytes| IteratorMode::From(bytes, Direction::Forward))
            .unwrap_or(IteratorMode::Start);
        let upper = bound_to_include(upper.map(String::as_bytes));

        let mut iter = self.snapshot.iterator(lower);

        stream! {
            while let Some(item) = iter.next() {
                let (key, v) = item.unwrap();
                if let Some(upper) = upper {
                    if upper.cmp(&key).is_lt() {
                        return;
                    }
                }
                yield bincode::deserialize(v.as_bytes()).unwrap()
            }
        }
    }

    fn projection_range_from<'a>(
        &'a self,
        range: (Bound<&'a ItemKey>, Bound<&'a ItemKey>),
    ) -> impl Stream<Item = ProjectionField> + 'a {
        fn bound_to_include(bound: Bound<&[u8]>) -> Option<&[u8]> {
            match bound {
                Bound::Included(bytes) | Bound::Excluded(bytes) => Some(bytes),
                Bound::Unbounded => None,
            }
        }

        let (lower, upper) = range;
        let lower = bound_to_include(lower.map(String::as_bytes))
            .map(|bytes| IteratorMode::From(bytes, Direction::Forward))
            .unwrap_or(IteratorMode::Start);
        let upper = bound_to_include(upper.map(String::as_bytes));

        let mut iter = self.snapshot.iterator(lower);

        stream! {
            while let Some(item) = iter.next() {
                let (key, v) = item.unwrap();
                if let Some(upper) = upper {
                    if upper.cmp(&key).is_lt() {
                        return;
                    }
                }
                yield bincode::deserialize::<BenchItem>(v.as_bytes()).unwrap().string
            }
        }
    }
}
