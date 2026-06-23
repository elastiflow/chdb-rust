//! Example: concurrent streaming reads through one shared session
//!
//! # Thread safety
//!
//! At the Rust type level, [`Session`] and [`Connection`] are [`Send`] but not [`Sync`],
//! so you cannot share `&Session` across threads without synchronization such as
//! [`Mutex`]. That is independent of chDB's C API, which documents that concurrent
//! use of the same connection should be synchronized.
//!
//! Additionally, [`QueryStream`] borrows the session's connection for its entire
//! lifetime, so only one streaming read can be active on a session at a time in safe
//! Rust. This example wraps the session in [`Arc<Mutex<Session>>`] and spawns three
//! reader threads; the mutex serializes access so each thread completes one full
//! streaming read before the next begins.
//!
//! Run with: cargo run --example 11_concurrent_reads_same_session

mod concurrency_common;

use std::sync::{Arc, Mutex};
use std::time::Instant;

use concurrency_common::{
    example_data_path, expect_row_count, join_read_outcomes, open_session, prepare_num_table,
    print_outcomes, spawn_streaming_read, ROW_COUNT,
};

const READER_COUNT: usize = 3;

fn main() -> Result<(), chdb_rust::error::Error> {
    let path = example_data_path("chdb-concurrent-same-session");
    let session = open_session(path, true)?;
    prepare_num_table(&session)?;

    println!("=== Concurrent reads, one session ===\n");
    println!("Table seeded with {ROW_COUNT} rows.");
    println!("Spawning {READER_COUNT} reader threads sharing Arc<Mutex<Session>>...\n");

    let session = Arc::new(Mutex::new(session));
    let wall_start = Instant::now();

    let handles = (0..READER_COUNT)
        .map(|i| spawn_streaming_read(Arc::clone(&session), reader_label(i)))
        .collect::<Vec<_>>();

    let outcomes = join_read_outcomes(handles);
    let wall_elapsed = wall_start.elapsed();

    println!();
    print_outcomes(&outcomes);
    for outcome in &outcomes {
        if let Some(err) = &outcome.error {
            eprintln!("{} failed: {err}", outcome.label);
            return Err(chdb_rust::error::Error::QueryError(err.clone()));
        }
        expect_row_count(outcome.rows, &outcome.label)?;
    }

    println!("All {READER_COUNT} reads completed successfully.");
    println!("Wall time: {wall_elapsed:.2?}");
    println!(
        "Note: Session is Send but not Sync, so Arc<Mutex<Session>> is required to \
         share it across threads. QueryStream also borrows the connection for the \
         duration of the stream, so reads run one-at-a-time on this session."
    );

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
