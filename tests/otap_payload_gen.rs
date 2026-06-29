#[path = "../examples/common/mod.rs"]
mod common;

use common::otap_payload_gen::{self, MetricKind, StreamConfig};
use otap_df_pdata::OtapPayload;

#[test]
fn logs_iterator_respects_total_and_batch_size() {
    let config = StreamConfig::new(2_500).batch_size(1_000).seed(7);
    let iter = otap_payload_gen::logs(config);
    assert_eq!(iter.total_items(), 2_500);
    assert_eq!(iter.items_per_payload(), 1_000);
    assert_eq!(iter.len(), 3);

    let payloads = iter.collect::<Vec<_>>();
    assert_eq!(payloads.len(), 3);
    assert_eq!(
        payloads.iter().map(OtapPayload::num_items).sum::<usize>(),
        2_500
    );
}

#[test]
fn all_metric_kinds_produce_non_empty_payloads() {
    let config = StreamConfig::new(50).batch_size(10).seed(99);
    for (kind, iter) in otap_payload_gen::all_metric_kinds(config) {
        let payloads = iter.collect::<Vec<_>>();
        assert!(!payloads.is_empty(), "{kind:?} produced no payloads");
        assert!(
            payloads.iter().all(|payload| !payload.is_empty()),
            "{kind:?} produced an empty payload"
        );
    }
}

#[test]
fn spans_iterator_yields_otap_records() {
    let config = StreamConfig::new(25).batch_size(5).seed(1);
    let payloads = otap_payload_gen::spans(config).collect::<Vec<_>>();
    assert_eq!(payloads.len(), 5);
    assert!(payloads.iter().all(|payload| !payload.is_empty()));
}

#[test]
fn otap_logs_batch_prepares_for_chdb() {
    use arrow_otap::datatypes::{DataType, TimeUnit};
    use common::otap_materialize::prepare_logs_for_chdb;
    use otap_df_pdata::OtapArrowRecords;
    use otap_df_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;

    let config = StreamConfig::new(10).batch_size(10).seed(42);
    let payload = otap_payload_gen::logs(config).next().expect("payload");
    let mut records: OtapArrowRecords = payload.try_into().expect("records");
    records.decode_transport_optimized_ids().expect("decode");

    let logs = records
        .get(ArrowPayloadType::Logs)
        .expect("logs batch");
    let prepared = prepare_logs_for_chdb(logs).expect("prepare");
    assert_eq!(prepared.num_rows(), logs.num_rows());
    assert_eq!(prepared.num_columns(), 4);
    for name in ["time_unix_nano", "scope_name", "severity_text", "body_str"] {
        assert!(prepared.schema().field_with_name(name).is_ok(), "missing {name}");
    }
    assert!(matches!(
        prepared
            .schema()
            .field_with_name("time_unix_nano")
            .expect("time_unix_nano")
            .data_type(),
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    ));
}

#[test]
fn mount_generated_otap_resource_attrs() -> chdb_rust::error::Result<()> {
    use arrow_otap::array::{Array, StructArray};
    use chdb_rust::arrow_stream::{ArrowArray, ArrowSchema};
    use chdb_rust::connection::Connection;
    use chdb_rust::format::OutputFormat;
    use common::otap_materialize::materialize_for_chdb;
    use otap_df_pdata::OtapArrowRecords;
    use otap_df_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;

    let config = StreamConfig::new(10).batch_size(10).seed(42);
    let payload = otap_payload_gen::logs(config).next().expect("payload");
    let mut records: OtapArrowRecords = payload.try_into().expect("records");
    records.decode_transport_optimized_ids().expect("decode");
    let batch = materialize_for_chdb(
        records
            .get(ArrowPayloadType::ResourceAttrs)
            .expect("resource attrs"),
    )
    .expect("materialize");

    let conn = Connection::open_in_memory()?;
    let struct_array = StructArray::from(batch);
    let (mut ffi_array, mut ffi_schema) = arrow_otap::ffi::to_ffi(&struct_array.to_data()).unwrap();
    let arrow_schema = unsafe {
        ArrowSchema::from_raw((&mut ffi_schema as *mut arrow_otap::ffi::FFI_ArrowSchema).cast())
    };
    let arrow_array = unsafe {
        ArrowArray::from_raw((&mut ffi_array as *mut arrow_otap::ffi::FFI_ArrowArray).cast())
    };
    conn.register_arrow_array("resource_attrs", &arrow_schema, &arrow_array)?;
    let result = conn.query(
        "SELECT count() FROM arrowstream('resource_attrs')",
        OutputFormat::TabSeparated,
    )?;
    assert_ne!(result.data_utf8_lossy().trim(), "0");
    conn.unregister_arrow_table("resource_attrs")?;
    Ok(())
}

#[test]
fn insert_prepared_otap_logs_via_arrowstream() -> chdb_rust::error::Result<()> {
    use chdb_rust::connection::Connection;
    use chdb_rust::format::OutputFormat;
    use common::otap_materialize::prepare_logs_for_chdb;
    use otap_df_pdata::OtapArrowRecords;
    use otap_df_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;

    let config = StreamConfig::new(10).batch_size(10).seed(42);
    let payload = otap_payload_gen::logs(config).next().expect("payload");
    let mut records: OtapArrowRecords = payload.try_into().expect("records");
    records.decode_transport_optimized_ids().expect("decode");
    let logs = records.get(ArrowPayloadType::Logs).expect("logs");
    let prepared = prepare_logs_for_chdb(logs).expect("prepare");

    let conn = Connection::open_in_memory()?;
    conn.query(
        "CREATE TABLE logs (time_unix_nano DateTime64(9), scope_name String, severity_text String, body_str String) ENGINE=MergeTree ORDER BY time_unix_nano",
        OutputFormat::TabSeparated,
    )?;

    let mounted = common::arrow_mount::MountedArrowBatch::mount(&conn, "stg_logs", &prepared)?;
    common::otap_chdb_loader::insert_otap_logs(&conn, &mounted)?;
    let result = conn.query("SELECT count() FROM logs", OutputFormat::TabSeparated)?;
    assert_eq!(result.data_utf8_lossy().trim(), "10");
    mounted.unmount(&conn)?;
    Ok(())
}

#[test]
fn each_metric_kind_can_be_streamed() {
    let config = StreamConfig::new(12).batch_size(4).seed(3);
    for kind in MetricKind::ALL {
        let payloads = otap_payload_gen::metrics(kind, config).collect::<Vec<_>>();
        assert_eq!(payloads.len(), 3, "{kind:?}");
        assert_eq!(
            payloads.iter().map(OtapPayload::num_items).sum::<usize>(),
            12,
            "{kind:?}"
        );
    }
}
