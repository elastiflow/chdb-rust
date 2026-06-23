//! Example: insert while a streaming read is in progress
//!
//! One thread opens a session and begins draining a large streaming `SELECT`.
//! After the first chunk arrives, a second thread opens a separate session on the
//! same database path and runs an `INSERT`. This demonstrates cross-session
//! read/write overlap; a single session cannot interleave a stream with a write
//! because [`QueryStream`] holds the connection until it is dropped.
//!
//! Run with: cargo run --example 13_write_during_single_read

mod concurrency_common;

use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use concurrency_common::{
    cleanup_data_path, count_rows, example_data_path, insert_batch, open_session, prepare_num_table,
    READ_QUERY, ROW_COUNT, STREAM_ARGS, WRITE_BATCH_SIZE,
};

fn main() -> Result<(), chdb_rust::error::Error> {
    let path = example_data_path("chdb-write-during-read");
    {
        let setup = open_session(path.clone(), false)?;
        prepare_num_table(&setup)?;
    }

    println!("=== Write during a single streaming read ===\n");
    println!("Baseline row count: {ROW_COUNT}");

    let (read_started_tx, read_started_rx) = mpsc::channel();
    let path_for_reader = path.clone();
    let path_for_writer = path.clone();

    let reader = thread::spawn(move || -> Result<u64, chdb_rust::error::Error> {
        let started = Instant::now();
        let session = open_session(path_for_reader, false)?;
        let mut stream = session.execute_stream(READ_QUERY, Some(STREAM_ARGS))?;

        let mut streamed_rows = 0u64;
        let mut chunks = 0usize;
        while let Some(chunk) = stream.next_chunk()? {
            streamed_rows += chunk.rows_read();
            chunks += 1;
            if chunks == 1 {
                let _ = read_started_tx.send(());
            }
        }

        println!(
            "reader: streamed {streamed_rows} rows in {chunks} chunks over {:.2?}",
            started.elapsed()
        );
        Ok(streamed_rows)
    });

    let writer = thread::spawn(move || -> WriteResult {
        let started = Instant::now();
        if read_started_rx.recv().is_err() {
            return WriteResult {
                rows_written: 0,
                error: Some("reader exited before first chunk".into()),
            };
        }

        let insert_start = ROW_COUNT + 1;
        let write = (|| {
            let session = open_session(path_for_writer, false)?;
            insert_batch(&session, insert_start, WRITE_BATCH_SIZE)
        })();

        WriteResult {
            rows_written: write.as_ref().copied().unwrap_or(0),
            error: write.err().map(|e| e.to_string()),
        }
    });

    let streamed_rows = reader.join().expect("reader panicked")?;
    let write_result = writer.join().expect("writer panicked");

    println!();
    match &write_result.error {
        Some(err) => println!("writer: FAILED — {err}"),
        None => println!("writer: inserted {} rows", write_result.rows_written),
    }

    let verify = open_session(path.clone(), false)?;
    let final_count = count_rows(&verify)?;
    println!("final row count: {final_count}");
    println!("streamed rows observed by reader: {streamed_rows}");
    if final_count > ROW_COUNT {
        println!("insert completed; reader may or may not have seen new rows depending on query timing.");
    }

    cleanup_data_path(&path);

    if let Some(err) = write_result.error {
        return Err(chdb_rust::error::Error::QueryError(err));
    }

    Ok(())
}

struct WriteResult {
    rows_written: u64,
    error: Option<String>,
}
