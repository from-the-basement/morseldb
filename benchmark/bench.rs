mod common;

use std::{
    collections::Bound,
    env::current_dir,
    fs,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use common::*;
use futures_util::{future::join_all, StreamExt};
use tempfile::{NamedTempFile, TempDir};

const ITERATIONS: usize = 2;
const WRITE_TIMES: usize = 10_000;
const READ_TIMES: usize = 200;
const KEY_SIZE: usize = 24;
const STRING_SIZE: usize = 2 * 1024;
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
        string_1: gen_string(rng, STRING_SIZE),
        string_2: gen_string(rng, STRING_SIZE),
        string_3: gen_string(rng, STRING_SIZE),
        string_4: gen_string(rng, STRING_SIZE),
        string_5: gen_string(rng, STRING_SIZE),
        string_6: gen_string(rng, STRING_SIZE),
        string_7: gen_string(rng, STRING_SIZE),
        string_8: gen_string(rng, STRING_SIZE),
        string_9: gen_string(rng, STRING_SIZE),
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

async fn benchmark<T: BenchDatabase + Send + Sync>(
    path: impl AsRef<Path> + Clone,
) -> Vec<(String, Duration)> {
    let mut rng = make_rng();
    let mut results = Vec::new();
    let db = Arc::new(T::build(path.clone()).await);

    let start = Instant::now();
    let mut txn = db.write_transaction().await;
    let mut inserter = txn.get_inserter();
    {
        for _ in 0..WRITE_TIMES {
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
        WRITE_TIMES,
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

    for num_threads in [4, 16] {
        let mut rngs = make_rng_shards(num_threads, writes);
        let start = Instant::now();

        let futures = (0..num_threads).map(|_| {
            let db2 = db.clone();
            let mut rng = rngs.pop().unwrap();

            async move {
                for _ in 0..(writes / num_threads) {
                    let mut txn = db2.write_transaction().await;
                    let mut inserter = txn.get_inserter();
                    inserter.insert(gen_record(&mut rng)).unwrap();
                    drop(inserter);
                    txn.commit().await.unwrap();
                }
            }
        });
        join_all(futures).await;

        let end = Instant::now();
        let duration = end - start;
        println!(
            "{}: Wrote {} individual items ({} threads) in {}ms",
            T::db_type_name(),
            writes,
            num_threads,
            duration.as_millis()
        );
        results.push((
            format!("individual writes ({num_threads} threads)"),
            duration,
        ));
    }

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

    for num_threads in [4, 16] {
        let mut rngs = make_rng_shards(num_threads, batch_size);
        let start = Instant::now();

        let futures = (0..num_threads).map(|_| {
            let db2 = db.clone();
            let mut rng = rngs.pop().unwrap();

            async move {
                for _ in 0..writes {
                    let mut txn = db2.write_transaction().await;
                    let mut inserter = txn.get_inserter();
                    for _ in 0..(batch_size / num_threads) {
                        inserter.insert(gen_record(&mut rng)).unwrap();
                    }
                    drop(inserter);
                    txn.commit().await.unwrap();
                }
            }
        });
        join_all(futures).await;

        let end = Instant::now();
        let duration = end - start;
        println!(
            "{}: Wrote {} x {} items ({} threads) in {}ms",
            T::db_type_name(),
            writes,
            batch_size,
            num_threads,
            duration.as_millis()
        );
        results.push((format!("batch writes ({num_threads} threads)"), duration));
    }

    drop(db);
    let db = Arc::new(T::build(path).await);
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
            for _ in 0..READ_TIMES {
                let record = gen_record(&mut rng);
                let _ = reader.get(&record.primary_key).await;
            }
            let end = Instant::now();
            let duration = end - start;
            println!(
                "{}: Random read {} items in {}ms",
                T::db_type_name(),
                READ_TIMES,
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
            for _ in 0..READ_TIMES {
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
                READ_TIMES * num_scan,
                duration.as_millis()
            );
            results.push(("random range reads".to_string(), duration));
        }

        for _ in 0..ITERATIONS {
            let mut rng = make_rng();
            let start = Instant::now();
            let reader = txn.get_reader();
            let mut value_sum = 0;
            let num_scan = 10;
            for _ in 0..READ_TIMES {
                let record = gen_record(&mut rng);
                let mut iter = Box::pin(reader.projection_range_from((
                    Bound::Included(&record.primary_key),
                    Bound::Unbounded,
                )));
                for _ in 0..num_scan {
                    if let Some(_projection_field) = iter.next().await {
                        value_sum += 1;
                    } else {
                        break;
                    }
                }
            }
            assert!(value_sum > 0);
            let end = Instant::now();
            let duration = end - start;
            println!(
                "{}: Random range projection read {} elements in {}ms",
                T::db_type_name(),
                READ_TIMES * num_scan,
                duration.as_millis()
            );
            results.push(("random range projection reads".to_string(), duration));
        }
    }
    drop(txn);

    for num_threads in [4, 8, 16, 32] {
        let mut rngs = make_rng_shards(num_threads, WRITE_TIMES);
        let start = Instant::now();

        let futures = (0..num_threads).map(|_| {
            let db2 = db.clone();
            let mut rng = rngs.pop().unwrap();

            async move {
                let txn = db2.read_transaction().await;
                let reader = txn.get_reader();
                for _ in 0..(WRITE_TIMES / num_threads) {
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
            WRITE_TIMES,
            duration.as_millis()
        );
        results.push((format!("random reads ({num_threads} threads)"), duration));
    }

    let start = Instant::now();
    let deletes = WRITE_TIMES / 2;
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

    let tonbo_latency_results = {
        let tmp_dir: TempDir = tempfile::tempdir_in(&tmpdir).unwrap();
        benchmark::<TonboBenchDataBase>(tmp_dir.path()).await
    };
    let redb_latency_results = {
        let tmp_file: NamedTempFile = NamedTempFile::new_in(&tmpdir).unwrap();
        benchmark::<RedbBenchDatabase>(tmp_file.path()).await
    };
    let rocksdb_results = {
        let tmp_file: TempDir = tempfile::tempdir_in(&tmpdir).unwrap();
        benchmark::<RocksdbBenchDatabase>(tmp_file.path()).await
    };
    let sled_results = {
        let tmp_file: TempDir = tempfile::tempdir_in(&tmpdir).unwrap();
        benchmark::<SledBenchDatabase>(tmp_file.path()).await
    };

    fs::remove_dir_all(&tmpdir).unwrap();

    let mut rows: Vec<Vec<String>> = Vec::new();

    for (benchmark, _duration) in &tonbo_latency_results {
        rows.push(vec![benchmark.to_string()]);
    }

    for results in [
        tonbo_latency_results,
        redb_latency_results,
        rocksdb_results,
        sled_results,
    ] {
        for (i, (_benchmark, duration)) in results.iter().enumerate() {
            rows[i].push(format!("{}ms", duration.as_millis()));
        }
    }

    let mut table = comfy_table::Table::new();
    table.set_width(100);
    table.set_header(["", "tonbo", "redb", "rocksdb", "sled"]);
    for row in rows {
        table.add_row(row);
    }

    println!();
    println!("{table}");
}
