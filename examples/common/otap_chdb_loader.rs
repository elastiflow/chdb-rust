//! Load OTAP log payloads into the example ClickHouse table via a mounted Arrow batch.

use chdb_rust::connection::Connection;
use chdb_rust::error::Result;
use chdb_rust::format::OutputFormat;
use otap_df_pdata::OtapArrowRecords;
use otap_df_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;

use super::arrow_mount::MountedArrowBatch;
use super::otap_materialize::prepare_logs_for_chdb;

const STG_LOGS: &str = "stg_logs";

/// Mount a decoded OTAP logs batch as a chDB `arrowstream()` source.
pub fn mount_otap_logs(conn: &Connection, records: &OtapArrowRecords) -> Result<Box<MountedArrowBatch>> {
    let logs = records
        .get(ArrowPayloadType::Logs)
        .ok_or_else(|| chdb_rust::error::Error::QueryError("OTAP batch missing Logs".into()))?;
    let prepared = prepare_logs_for_chdb(logs)
        .map_err(|e| chdb_rust::error::Error::QueryError(e.to_string()))?;
    MountedArrowBatch::mount(conn, STG_LOGS, &prepared)
}

/// Copy the mounted logs batch into the persistent `logs` table.
pub fn insert_otap_logs(conn: &Connection, _mounted: &MountedArrowBatch) -> Result<()> {
    conn.query(INSERT_LOGS, OutputFormat::TabSeparated).map(|_| ())
}

const INSERT_LOGS: &str =
    "INSERT INTO logs SELECT time_unix_nano, scope_name, severity_text, body_str FROM arrowstream('stg_logs')";
