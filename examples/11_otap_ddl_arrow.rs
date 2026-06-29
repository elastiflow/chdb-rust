//! Example: load OTAP log payloads into ClickHouse via a mounted Arrow batch.
//!
//! This example:
//! 1. Creates a small `logs` table with four scalar columns
//! 2. Generates randomized OTAP log payloads
//! 3. Mounts the OTAP logs batch with `chdb_arrow_array_scan`
//!    (via [`Connection::register_arrow_array`]) as `arrowstream('stg_logs')`
//! 4. Copies four fields into the table with `INSERT … SELECT`
//!
//! Run with:
//!
//! ```bash
//! cargo run --features arrow --example 11_otap_ddl_arrow
//! ```

mod common;

use chdb_rust::connection::Connection;
use chdb_rust::format::OutputFormat;
use common::otap_chdb_loader::{insert_otap_logs, mount_otap_logs};
use common::otap_payload_gen::{self, StreamConfig};
use otap_df_pdata::OtapArrowRecords;

/*
CREATE TABLE IF NOT EXISTS resources (
    schema_url String,
    dropped_attributes_count Int32,
    attributes Nested(
        key String,
        type Enum8('Empty' = 0, 'Str' = 1, 'Int' = 2, 'Double' = 3, 'Bool' = 4, 'Map' = 5, 'Slice' = 6, 'Bytes' = 7),
        str String,
        int Nullable(Int64),
        double Nullable(Float64),
        bool Nullable(Bool),
        bytes String,
        ser String
    ),
    resource_id String MATERIALIZED xxh3(tuple(schema_url, attributes))
)
ENGINE = ReplacingMergeTree()
ORDER BY resource_id;

CREATE TABLE IF NOT EXISTS scopes (
    name String,
    version String,
    schema_url String,
    dropped_attributes_count Int32,
    attributes Nested(
        key String,
        type Enum8('Empty' = 0, 'Str' = 1, 'Int' = 2, 'Double' = 3, 'Bool' = 4, 'Map' = 5, 'Slice' = 6, 'Bytes' = 7),
        str String,
        int Nullable(Int64),
        double Nullable(Float64),
        bool Nullable(Bool),
        bytes String,
        ser String
    ),
    scope_id String MATERIALIZED xxh3(tuple(name, version, schema_url, attributes))
)
ENGINE = ReplacingMergeTree()
ORDER BY scope_id;

CREATE TABLE IF NOT EXISTS logs (
    resource_schema_url String,
    resource_dropped_attributes_count Int32,
    scope_name String,
    scope_version String,
    scope_dropped_attributes_count Int32,
    schema_url String,
    time_unix_nano UInt64,
    observed_time_unix_nano UInt64,
    trace_id String,
    span_id String,
    severity_number Nullable(Int32),
    severity_text String,
    event_name String,
    body_type Enum8('Empty' = 0, 'Str' = 1, 'Int' = 2, 'Double' = 3, 'Bool' = 4, 'Map' = 5, 'Slice' = 6, 'Bytes' = 7),
    body_str String,
    body_int Nullable(Int64),
    body_double Nullable(Float64),
    body_bool Nullable(Bool),
    body_bytes String,
    body_ser String,
    dropped_attributes_count Int32,
    flags UInt32,
    attributes Nested(
        key String,
        type Enum8('Empty' = 0, 'Str' = 1, 'Int' = 2, 'Double' = 3, 'Bool' = 4, 'Map' = 5, 'Slice' = 6, 'Bytes' = 7),
        str String,
        int Nullable(Int64),
        double Nullable(Float64),
        bool Nullable(Bool),
        bytes String,
        ser String
    ),
    // ? this seems wrong
    // resource_id String MATERIALIZED xxh3(tuple(resource_schema_url, scope_name, scope_version))
)
ENGINE = MergeTree()
ORDER BY (time_unix_nano, scope_name);
*/

const CREATE_TABLE_DDL: &str = r#"
CREATE TABLE logs (
    time_unix_nano DateTime64(9),
    scope_name String,
    severity_text String,
    body_str String
)
ENGINE = MergeTree()
ORDER BY time_unix_nano
"#;

const PREVIEW_ROWS: usize = 5;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = Connection::open_in_memory()?;
    conn.query(CREATE_TABLE_DDL, OutputFormat::TabSeparated)?;

    let config = StreamConfig::new(10).batch_size(10).seed(42);
    let payload = otap_payload_gen::logs(config)
        .next()
        .expect("generator yields at least one payload");

    let mut records: OtapArrowRecords = payload.try_into()?;
    records.decode_transport_optimized_ids()?;

    println!(
        "Loading OTAP payload with {} log records…",
        records.num_items()
    );

    let mounted = mount_otap_logs(&conn, &records)?;
    println!("Mounted OTAP logs batch as arrowstream('stg_logs')");

    insert_otap_logs(&conn, &mounted)?;
    println!("Copied staged data into ClickHouse table");
    mounted.unmount(&conn)?;

    let result = conn.query("SELECT count() AS rows FROM logs", OutputFormat::TabSeparated)?;
    println!("logs: {}", result.data_utf8_lossy().trim());

    let preview = conn.query(
        &format!(
            "SELECT time_unix_nano, scope_name, severity_text, body_str
             FROM logs
             ORDER BY time_unix_nano
             LIMIT {PREVIEW_ROWS}"
        ),
        OutputFormat::TabSeparated,
    )?;
    println!("\nSample log rows:\n{}", preview.data_utf8_lossy());

    Ok(())
}
