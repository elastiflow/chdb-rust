//! Example: table layout experiments with Arrow streaming → OTAP conversion
//!
//! Scaffold for trying different DDL layouts and converting the resulting
//! [`arrow::record_batch::RecordBatch`] streams into OTAP payloads.
//!
//! Edit the DDL constants below, then run:
//!
//! ```bash
//! cargo run --features arrow --example 11_otap_ddl_arrow
//! ```

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use chdb_rust::arg::Arg;
use chdb_rust::session::SessionBuilder;

const DATABASE_DDL: &str = "CREATE DATABASE otap_demo; USE otap_demo";

/// Replace with your table layout experiments.
const CREATE_TABLE_DDL: &str = r#"
CREATE TABLE hello (
    message String
)
ENGINE = MergeTree()
ORDER BY ();
"#;

/// Optional seed data so the stream has rows to inspect.
const SEED_DATA_DDL: &str = r#"
INSERT INTO hello VALUES ('world')
"#;

const QUERY: &str = r#"
SELECT * FROM hello
"#;

const PREVIEW_ROWS: usize = 5;

fn record_batch_to_otap(_batch: &RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
    // TODO: convert record batches to OTAP payloads
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = std::env::temp_dir().join("chdb-otap-ddl-example");
    let session = SessionBuilder::new()
        .with_data_path(tmp)
        .with_auto_cleanup(true)
        .build()?;

    session.execute(DATABASE_DDL, Some(&[Arg::MultiQuery]))?;
    session.execute(CREATE_TABLE_DDL, None)?;
    session.execute(SEED_DATA_DDL, None)?;

    let stream = session.execute_stream_arrow(QUERY)?;

    println!("Streaming query results (Arrow C Data Interface):\n");
    let mut batch_count = 0;
    let mut total_rows = 0usize;

    for batch in stream {
        let batch = batch?;
        let row_count = batch.num_rows();
        total_rows += row_count;

        record_batch_to_otap(&batch)?;

        let preview = if row_count > PREVIEW_ROWS {
            batch.slice(0, PREVIEW_ROWS)
        } else {
            batch
        };

        println!("batch {batch_count}: {row_count} rows");
        println!("{}", pretty_format_batches(&[preview])?);
        if row_count > PREVIEW_ROWS {
            println!(
                "  ... ({remaining} more rows)",
                remaining = row_count - PREVIEW_ROWS
            );
        }
        batch_count += 1;
    }

    println!("\nReceived {batch_count} batches, {total_rows} rows total.");
    Ok(())
}
