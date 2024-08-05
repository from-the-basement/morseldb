mod common;

use std::{
    collections::Bound,
    env::current_dir,
    fs,
    sync::Arc,
    time::{Duration, Instant},
};

use common::*;
use futures_util::{future::join_all, StreamExt};
use tempfile::NamedTempFile;
use tonbo::{executor::tokio::TokioExecutor, DbOption, DB};

const ITERATIONS: usize = 2;
const ELEMENTS: usize = 1_000_000;
const KEY_SIZE: usize = 24;
const STRING_SIZE: usize = 150;
const RNG_SEED: u64 = 3;

fn make_rng() -> fastrand::Rng {
    fastrand::Rng::with_seed(RNG_SEED)
}

fn gen_string(rng: &mut fastrand::Rng, len: usize) -> String {
    let charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    let random_string: String = (0..len)
        .map(|_| {
            let idx = rng.usize(0..charset.len());
            charset.chars().nth(idx).unwrap()
        })
        .collect();
    random_string
}

fn gen_record(rng: &mut fastrand::Rng) -> BenchItem {
    BenchItem {
        primary_key: gen_string(rng, KEY_SIZE),
        string: gen_string(rng, STRING_SIZE),
        u32: rng.u32(..),
        boolean: rng.bool(),
    }
}

fn make_rng_shards(shards: usize, elements: usize) -> Vec<fastrand::Rng> {
    let mut rngs = vec![];
    let elements_per_shard = elements / shards;
    for i in 0..shards {
        let mut rng = make_rng();
        for _ in 0..(i * elements_per_shard) {
            gen_record(&mut rng);
        }
        rngs.push(rng);
    }

    rngs
}

async fn benchmark<T: BenchDatabase + Send + Sync>(db: T) -> Vec<(String, Duration)> {
    let mut rng = make_rng();
    let mut results = Vec::new();
    let db = Arc::new(db);

    let start = Instant::now();
    let mut txn = db.write_transaction().await;
    let mut inserter = txn.get_inserter();
    {
        for _ in 0..ELEMENTS {
            inserter.insert(gen_record(&mut rng)).unwrap();
        }
    }
    drop(inserter);
    txn.commit().await.unwrap();

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Bulk loaded {} items in {}ms",
        T::db_type_name(),
        ELEMENTS,
        duration.as_millis()
    );
    results.push(("bulk load".to_string(), duration));

    let start = Instant::now();
    let writes = 100;
    {
        for _ in 0..writes {
            let mut txn = db.write_transaction().await;
            let mut inserter = txn.get_inserter();
            inserter.insert(gen_record(&mut rng)).unwrap();
            drop(inserter);
            txn.commit().await.unwrap();
        }
    }

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Wrote {} individual items in {}ms",
        T::db_type_name(),
        writes,
        duration.as_millis()
    );
    results.push(("individual writes".to_string(), duration));

    let start = Instant::now();
    let batch_size = 1000;
    {
        for _ in 0..writes {
            let mut txn = db.write_transaction().await;
            let mut inserter = txn.get_inserter();
            for _ in 0..batch_size {
                inserter.insert(gen_record(&mut rng)).unwrap();
            }
            drop(inserter);
            txn.commit().await.unwrap();
        }
    }

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Wrote {} x {} items in {}ms",
        T::db_type_name(),
        writes,
        batch_size,
        duration.as_millis()
    );
    results.push(("batch writes".to_string(), duration));

    let txn = db.read_transaction().await;
    {
        // {
        //     let start = Instant::now();
        //     let len = txn.get_reader().len();
        //     assert_eq!(len, ELEMENTS as u64 + 100_000 + 100);
        //     let end = Instant::now();
        //     let duration = end - start;
        //     println!("{}: len() in {}ms", T::db_type_name(), duration.as_millis());
        //     results.push(("len()".to_string(), duration));
        // }

        for _ in 0..ITERATIONS {
            let mut rng = make_rng();
            let start = Instant::now();
            let reader = txn.get_reader();
            for _ in 0..ELEMENTS {
                let record = gen_record(&mut rng);
                let _ = reader.get(&record.primary_key).await;
            }
            let end = Instant::now();
            let duration = end - start;
            println!(
                "{}: Random read {} items in {}ms",
                T::db_type_name(),
                ELEMENTS,
                duration.as_millis()
            );
            results.push(("random reads".to_string(), duration));
        }

        for _ in 0..ITERATIONS {
            let mut rng = make_rng();
            let start = Instant::now();
            let reader = txn.get_reader();
            let mut value_sum = 0;
            let num_scan = 10;
            for _ in 0..ELEMENTS {
                let record = gen_record(&mut rng);
                let mut iter = Box::pin(
                    reader.range_from((Bound::Included(&record.primary_key), Bound::Unbounded)),
                );
                for _ in 0..num_scan {
                    if let Some(record) = iter.next().await {
                        value_sum += record.u32;
                    } else {
                        break;
                    }
                }
            }
            assert!(value_sum > 0);
            let end = Instant::now();
            let duration = end - start;
            println!(
                "{}: Random range read {} elements in {}ms",
                T::db_type_name(),
                ELEMENTS * num_scan,
                duration.as_millis()
            );
            results.push(("random range reads".to_string(), duration));
        }
    }
    drop(txn);

    for num_threads in [4, 8, 16, 32] {
        let mut rngs = make_rng_shards(num_threads, ELEMENTS);
        let start = Instant::now();

        let futures = (0..num_threads).map(|_| {
            let db2 = db.clone();
            let mut rng = rngs.pop().unwrap();

            async move {
                let txn = db2.read_transaction().await;
                let reader = txn.get_reader();
                for _ in 0..(ELEMENTS / num_threads) {
                    let record = gen_record(&mut rng);
                    let _ = reader.get(&record.primary_key);
                }
            }
        });
        join_all(futures).await;

        let end = Instant::now();
        let duration = end - start;
        println!(
            "{}: Random read ({} threads) {} items in {}ms",
            T::db_type_name(),
            num_threads,
            ELEMENTS,
            duration.as_millis()
        );
        results.push((format!("random reads ({num_threads} threads)"), duration));
    }

    let start = Instant::now();
    let deletes = ELEMENTS / 2;
    {
        let mut rng = make_rng();
        let mut txn = db.write_transaction().await;
        let mut inserter = txn.get_inserter();
        for _ in 0..deletes {
            let record = gen_record(&mut rng);
            inserter.remove(record.primary_key).unwrap();
        }
        drop(inserter);
        txn.commit().await.unwrap();
    }

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Removed {} items in {}ms",
        T::db_type_name(),
        deletes,
        duration.as_millis()
    );
    results.push(("removals".to_string(), duration));

    results
}

#[tokio::main]
async fn main() {
    let tmpdir = current_dir().unwrap().join(".benchmark");
    fs::create_dir(&tmpdir).unwrap();

    let redb_latency_results = {
        let tmpfile: NamedTempFile = NamedTempFile::new_in(&tmpdir).unwrap();
        let db = redb::Database::builder()
            .set_cache_size(4 * 1024 * 1024 * 1024)
            .create(tmpfile.path())
            .unwrap();
        let table = RedbBenchDatabase::new(&db);
        benchmark(table).await
    };

    let tonbo_latency_results = {
        let db = DB::new(
            DbOption::from(tmpdir.to_path_buf().join("tonbo")),
            TokioExecutor::new(),
        )
        .await
        .unwrap();
        let table = TonboBenchDataBase::new(&db);
        benchmark(table).await
    };

    fs::remove_dir_all(&tmpdir).unwrap();

    let mut rows: Vec<Vec<String>> = Vec::new();

    for (benchmark, _duration) in &redb_latency_results {
        rows.push(vec![benchmark.to_string()]);
    }

    for results in [redb_latency_results, tonbo_latency_results] {
        for (i, (_benchmark, duration)) in results.iter().enumerate() {
            rows[i].push(format!("{}ms", duration.as_millis()));
        }
    }

    let mut table = comfy_table::Table::new();
    table.set_width(100);
    table.set_header(["", "redb", "tonbo"]);
    for row in rows {
        table.add_row(row);
    }

    println!();
    println!("{table}");
}
