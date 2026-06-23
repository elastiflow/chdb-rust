//! Example: concurrent reads and writes at the same time
//!
//! Combines multiple streaming reader sessions with multiple writer sessions
//! targeting the same on-disk database.
//!
//! Run with: cargo run --example 16_concurrent_writes_during_concurrent_reads

mod concurrency_common;

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use concurrency_common::{
    cleanup_data_path, count_rows, example_data_path, insert_batch, join_read_outcomes,
    join_write_outcomes, open_session, prepare_num_table, print_outcomes, print_write_outcomes,
    WriteOutcome, CONCURRENT_READERS, CONCURRENT_WRITERS, ROW_COUNT, WRITE_BATCH_SIZE,
};

fn main() -> Result<(), chdb_rust::error::Error> {
    let path = example_data_path("chdb-concurrent-reads-and-writes");
    {
        let setup = open_session(path.clone(), false)?;
        prepare_num_table(&setup)?;
    }

    println!("=== Concurrent reads and writes ===\n");
    println!("Baseline row count: {ROW_COUNT}");
    println!(
        "Starting {CONCURRENT_READERS} readers and {CONCURRENT_WRITERS} writers...\n"
    );

    let start_gate = Arc::new(Barrier::new(CONCURRENT_READERS + CONCURRENT_WRITERS + 1));
    let wall_start = Instant::now();

    let read_handles: Vec<_> = (0..CONCURRENT_READERS)
        .map(|i| {
            let path = path.clone();
            let start_gate = Arc::clone(&start_gate);
            let label = reader_label(i);
            thread::spawn(move || {
                start_gate.wait();
                let started = Instant::now();
                let read = (|| {
                    let session = open_session(path, false)?;
                    concurrency_common::streaming_read(&session, label)
                })();
                concurrency_common::ReadOutcome {
                    label: label.to_string(),
                    rows: read.as_ref().copied().unwrap_or(0),
                    elapsed: started.elapsed(),
                    error: read.err().map(|e| e.to_string()),
                }
            })
        })
        .collect();

    let write_handles: Vec<_> = (0..CONCURRENT_WRITERS)
        .map(|i| {
            let path = path.clone();
            let start_gate = Arc::clone(&start_gate);
            let label = format!("writer-{}", i + 1);
            let start = ROW_COUNT + 1 + (i as u64 * WRITE_BATCH_SIZE);
            thread::spawn(move || {
                start_gate.wait();
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

    start_gate.wait();

    let read_outcomes = join_read_outcomes(read_handles);
    let write_outcomes = join_write_outcomes(write_handles);
    let wall_elapsed = wall_start.elapsed();

    println!();
    print_outcomes(&read_outcomes);
    print_write_outcomes(&write_outcomes);

    for outcome in &read_outcomes {
        if let Some(err) = &outcome.error {
            cleanup_data_path(&path);
            return Err(chdb_rust::error::Error::QueryError(err.clone()));
        }
    }
    for outcome in &write_outcomes {
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
            "expected {expected} rows, got {final_count}"
        )));
    }

    Ok(())
}

fn reader_label(index: usize) -> &'static str {
    match index {
        0 => "reader-a",
        1 => "reader-b",
        2 => "reader-c",
        _ => "reader",
    }
}
