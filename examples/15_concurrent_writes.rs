//! Example: concurrent inserts from separate sessions
//!
//! Each writer thread opens its own session on the same database path and inserts
//! a disjoint batch of rows concurrently.
//!
//! Run with: cargo run --example 15_concurrent_writes

mod concurrency_common;

use std::thread;
use std::time::Instant;

use concurrency_common::{
    cleanup_data_path, count_rows, example_data_path, insert_batch, join_write_outcomes,
    open_session, prepare_num_table, print_write_outcomes, WriteOutcome, CONCURRENT_WRITERS,
    ROW_COUNT, WRITE_BATCH_SIZE,
};

fn main() -> Result<(), chdb_rust::error::Error> {
    let path = example_data_path("chdb-concurrent-writes");
    {
        let setup = open_session(path.clone(), false)?;
        prepare_num_table(&setup)?;
    }

    println!("=== Concurrent writes from separate sessions ===\n");
    println!("Baseline row count: {ROW_COUNT}");
    println!("Spawning {CONCURRENT_WRITERS} writer threads...\n");

    let wall_start = Instant::now();
    let handles: Vec<_> = (0..CONCURRENT_WRITERS)
        .map(|i| {
            let path = path.clone();
            let label = format!("writer-{}", i + 1);
            let start = ROW_COUNT + 1 + (i as u64 * WRITE_BATCH_SIZE);
            thread::spawn(move || {
                let started = Instant::now();
                let write = (|| {
                    let session = open_session(path, false)?;
                    insert_batch(&session, start, WRITE_BATCH_SIZE)
                })();
                WriteOutcome {
                    label,
                    rows_written: write.as_ref().copied().unwrap_or(0),
                    elapsed: started.elapsed(),
                    error: write.err().map(|e| e.to_string()),
                }
            })
        })
        .collect();

    let outcomes = join_write_outcomes(handles);
    let wall_elapsed = wall_start.elapsed();

    println!();
    print_write_outcomes(&outcomes);
    for outcome in &outcomes {
        if let Some(err) = &outcome.error {
            cleanup_data_path(&path);
            return Err(chdb_rust::error::Error::QueryError(err.clone()));
        }
    }

    let verify = open_session(path.clone(), false)?;
    let final_count = count_rows(&verify)?;
    let expected = ROW_COUNT + (CONCURRENT_WRITERS as u64 * WRITE_BATCH_SIZE);

    println!("\nWall time: {wall_elapsed:.2?}");
    println!("final row count: {final_count} (expected {expected})");

    cleanup_data_path(&path);

    if final_count != expected {
        return Err(chdb_rust::error::Error::QueryError(format!(
            "expected {expected} rows after concurrent inserts, got {final_count}"
        )));
    }

    Ok(())
}
