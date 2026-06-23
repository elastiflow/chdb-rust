//! Example: concurrent streaming reads from separate sessions on one database
//!
//! Each reader thread opens its own [`Session`] pointing at the same on-disk data
//! path. Unlike example 11, no mutex is needed because each thread owns its session.
//!
//! Run with: cargo run --example 12_concurrent_reads_different_sessions

mod concurrency_common;

use std::time::Instant;

use concurrency_common::{
    cleanup_data_path, example_data_path, expect_row_count, join_read_outcomes, open_session,
    prepare_num_table, print_outcomes, spawn_session_reader, ROW_COUNT,
};

const READER_COUNT: usize = 3;

fn main() -> Result<(), chdb_rust::error::Error> {
    let path = example_data_path("chdb-concurrent-different-sessions");

    {
        let setup = open_session(path.clone(), false)?;
        prepare_num_table(&setup)?;
    }

    println!("=== Concurrent reads, separate sessions ===\n");
    println!("Table seeded with {ROW_COUNT} rows at {}.", path.display());
    println!("Spawning {READER_COUNT} reader threads, each with its own session...\n");

    let wall_start = Instant::now();
    let handles = (0..READER_COUNT)
        .map(|i| spawn_session_reader(path.clone(), reader_label(i)))
        .collect::<Vec<_>>();

    let outcomes = join_read_outcomes(handles);
    let wall_elapsed = wall_start.elapsed();

    println!();
    print_outcomes(&outcomes);
    for outcome in &outcomes {
        if let Some(err) = &outcome.error {
            eprintln!("{} failed: {err}", outcome.label);
            cleanup_data_path(&path);
            return Err(chdb_rust::error::Error::QueryError(err.clone()));
        }
        expect_row_count(outcome.rows, &outcome.label)?;
    }

    println!("\nAll {READER_COUNT} reads completed successfully.");
    println!("Wall time: {wall_elapsed:.2?}");
    if wall_elapsed < outcomes.iter().map(|o| o.elapsed).sum::<std::time::Duration>() / READER_COUNT as u32
    {
        println!("Wall time is less than the average per-thread time, indicating overlap.");
    }

    cleanup_data_path(&path);
    Ok(())
}

fn reader_label(index: usize) -> &'static str {
    match index {
        0 => "session-a",
        1 => "session-b",
        2 => "session-c",
        _ => "session",
    }
}
