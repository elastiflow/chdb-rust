//! Shared helpers for concurrency examples.

#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use chdb_rust::arg::Arg;
use chdb_rust::error::{Error, Result};
use chdb_rust::format::OutputFormat;
use chdb_rust::query_stream::QueryStream;
use chdb_rust::session::{Session, SessionBuilder};

pub const ROW_COUNT: u64 = 300_000;
pub const READ_QUERY: &str = "SELECT n FROM num ORDER BY n";
pub const STREAM_ARGS: &[Arg] = &[Arg::OutputFormat(OutputFormat::JSONEachRow)];
pub const WRITE_BATCH_SIZE: u64 = 1_000;
pub const CONCURRENT_READERS: usize = 3;
pub const CONCURRENT_WRITERS: usize = 3;

pub fn example_data_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(name)
}

pub fn open_session(path: PathBuf, auto_cleanup: bool) -> Result<Session> {
    SessionBuilder::new()
        .with_data_path(path)
        .with_auto_cleanup(auto_cleanup)
        .build()
}

pub fn prepare_num_table(session: &Session) -> Result<()> {
    session.execute(
        "CREATE TABLE num (n UInt64) ENGINE = MergeTree() ORDER BY n",
        None,
    )?;
    session.execute(
        &format!("INSERT INTO num SELECT number FROM numbers({ROW_COUNT})"),
        None,
    )?;
    Ok(())
}

pub fn drain_stream(mut stream: QueryStream<'_>) -> Result<u64> {
    let mut rows = 0u64;
    while let Some(chunk) = stream.next_chunk()? {
        rows += chunk.rows_read();
    }
    Ok(rows)
}

pub fn streaming_read(session: &Session, label: &str) -> Result<u64> {
    let started = Instant::now();
    let stream = session.execute_stream(READ_QUERY, Some(STREAM_ARGS))?;
    let rows = drain_stream(stream)?;
    println!(
        "{label}: read {rows} rows in {:.2?}",
        started.elapsed()
    );
    Ok(rows)
}

pub struct ReadOutcome {
    pub label: String,
    pub rows: u64,
    pub elapsed: Duration,
    pub error: Option<String>,
}

pub struct WriteOutcome {
    pub label: String,
    pub rows_written: u64,
    pub elapsed: Duration,
    pub error: Option<String>,
}

pub fn count_rows(session: &Session) -> Result<u64> {
    let result = session.execute(
        "SELECT count() AS c FROM num",
        Some(&[Arg::OutputFormat(OutputFormat::JSONEachRow)]),
    )?;
    let text = result.data_utf8_lossy();
    let count = text
        .lines()
        .next()
        .and_then(|line| line.split(':').nth(1))
        .and_then(|value| value.trim_end_matches(|c: char| !c.is_ascii_digit()).parse().ok())
        .ok_or_else(|| Error::QueryError(format!("unexpected COUNT output: {text}")))?;
    Ok(count)
}

pub fn insert_batch(session: &Session, start: u64, count: u64) -> Result<u64> {
    session.execute(
        &format!("INSERT INTO num SELECT {start} + number FROM numbers({count})"),
        None,
    )?;
    Ok(count)
}

pub fn print_write_outcomes(outcomes: &[WriteOutcome]) {
    for outcome in outcomes {
        match &outcome.error {
            Some(err) => println!(
                "{}: FAILED after {:.2?} — {err}",
                outcome.label, outcome.elapsed
            ),
            None => println!(
                "{}: OK — inserted {} rows in {:.2?}",
                outcome.label, outcome.rows_written, outcome.elapsed
            ),
        }
    }
}

pub fn join_write_outcomes(handles: Vec<thread::JoinHandle<WriteOutcome>>) -> Vec<WriteOutcome> {
    handles
        .into_iter()
        .map(|handle| handle.join().expect("writer thread panicked"))
        .collect()
}

pub fn spawn_streaming_read(
    session: Arc<Mutex<Session>>,
    label: &'static str,
) -> thread::JoinHandle<ReadOutcome> {
    thread::spawn(move || {
        let started = Instant::now();
        let read = (|| {
            let session = session.lock().expect("session mutex poisoned");
            streaming_read(&session, label)
        })();
        ReadOutcome {
            label: label.to_string(),
            rows: read.as_ref().copied().unwrap_or(0),
            elapsed: started.elapsed(),
            error: read.err().map(|e| e.to_string()),
        }
    })
}

pub fn expect_row_count(rows: u64, context: &str) -> Result<()> {
    if rows != ROW_COUNT {
        return Err(Error::QueryError(format!(
            "{context}: expected {ROW_COUNT} rows, got {rows}"
        )));
    }
    Ok(())
}

pub fn print_outcomes(outcomes: &[ReadOutcome]) {
    for outcome in outcomes {
        match &outcome.error {
            Some(err) => println!(
                "{}: FAILED after {:.2?} — {err}",
                outcome.label, outcome.elapsed
            ),
            None => println!(
                "{}: OK — {} rows in {:.2?}",
                outcome.label, outcome.rows, outcome.elapsed
            ),
        }
    }
}

pub fn spawn_session_reader(
    path: PathBuf,
    label: &'static str,
) -> thread::JoinHandle<ReadOutcome> {
    thread::spawn(move || {
        let started = Instant::now();
        let read = (|| {
            let session = open_session(path, false)?;
            streaming_read(&session, label)
        })();
        ReadOutcome {
            label: label.to_string(),
            rows: read.as_ref().copied().unwrap_or(0),
            elapsed: started.elapsed(),
            error: read.err().map(|e| e.to_string()),
        }
    })
}

pub fn join_read_outcomes(handles: Vec<thread::JoinHandle<ReadOutcome>>) -> Vec<ReadOutcome> {
    handles
        .into_iter()
        .map(|handle| handle.join().expect("reader thread panicked"))
        .collect()
}

pub fn cleanup_data_path(path: &PathBuf) {
    let _ = std::fs::remove_dir_all(path);
}
