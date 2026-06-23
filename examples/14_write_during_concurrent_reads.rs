//! Example: insert while multiple streaming reads are in progress
//!
//! Spawns several reader threads (each with its own session) that signal after
//! their first chunk, then a writer thread inserts rows through another session.
//!
//! Run with: cargo run --example 14_write_during_concurrent_reads

mod concurrency_common;

use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use concurrency_common::{
    cleanup_data_path, count_rows, example_data_path, insert_batch, open_session, prepare_num_table,
    READ_QUERY, ROW_COUNT, STREAM_ARGS, WRITE_BATCH_SIZE, CONCURRENT_READERS,
};

fn main() -> Result<(), chdb_rust::error::Error> {
    let path = example_data_path("chdb-write-during-concurrent-reads");
    {
        let setup = open_session(path.clone(), false)?;
        prepare_num_table(&setup)?;
    }

    println!("=== Write during concurrent streaming reads ===\n");
    println!("Baseline row count: {ROW_COUNT}");
    println!("Starting {CONCURRENT_READERS} reader threads plus one writer...\n");

    let (ready_tx, ready_rx) = mpsc::channel();
    let path_for_writer = path.clone();

    let reader_handles: Vec<_> = (0..CONCURRENT_READERS)
        .map(|i| {
            let path = path.clone();
            let ready_tx = ready_tx.clone();
            thread::spawn(move || -> Result<(String, u64), chdb_rust::error::Error> {
                let label = format!("reader-{}", i + 1);
                let session = open_session(path, false)?;
                let mut stream = session.execute_stream(READ_QUERY, Some(STREAM_ARGS))?;

                let mut rows = 0u64;
                let mut chunks = 0usize;
                while let Some(chunk) = stream.next_chunk()? {
                    rows += chunk.rows_read();
                    chunks += 1;
                    if chunks == 1 {
                        let _ = ready_tx.send(());
                    }
                }

                println!("{label}: streamed {rows} rows in {chunks} chunks");
                Ok((label, rows))
            })
        })
        .collect();

    drop(ready_tx);

    let writer = thread::spawn(move || -> Result<u64, chdb_rust::error::Error> {
        let mut signals = 0usize;
        while signals < CONCURRENT_READERS {
            if ready_rx.recv().is_err() {
                break;
            }
            signals += 1;
        }

        let started = Instant::now();
        let session = open_session(path_for_writer, false)?;
        let inserted = insert_batch(&session, ROW_COUNT + 1, WRITE_BATCH_SIZE)?;
        println!(
            "writer: inserted {inserted} rows after {signals} readers started ({:.2?})",
            started.elapsed()
        );
        Ok(inserted)
    });

    let inserted = writer.join().expect("writer panicked")?;
    let read_results: Result<Vec<_>, _> = reader_handles
        .into_iter()
        .map(|handle| handle.join().expect("reader panicked"))
        .collect();
    let read_results = read_results?;

    let verify = open_session(path.clone(), false)?;
    let final_count = count_rows(&verify)?;

    println!();
    for (label, rows) in &read_results {
        println!("{label}: observed {rows} rows during stream");
    }
    println!("writer inserted: {inserted} rows");
    println!("final row count: {final_count}");

    cleanup_data_path(&path);
    Ok(())
}
