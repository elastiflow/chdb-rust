//! Integration tests for mounting Arrow arrays via chdb_arrow_array_scan.

use arrow_otap::array::{Array, Int32Array, RecordBatch, StringArray, StructArray};
use arrow_otap::datatypes::{DataType, Field, Schema};
use chdb_rust::arrow_stream::{ArrowArray, ArrowSchema};
use chdb_rust::connection::Connection;
use chdb_rust::format::OutputFormat;
use std::sync::Arc;

#[test]
fn mount_simple_batch_via_arrow_array_scan() -> chdb_rust::error::Result<()> {
    let conn = Connection::open_in_memory()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["x", "y"])),
            Arc::new(Int32Array::from(vec![1, 2])),
        ],
    )
    .unwrap();

    let struct_array = StructArray::from(batch.clone());
    let (mut ffi_array, mut ffi_schema) = arrow_otap::ffi::to_ffi(&struct_array.to_data()).unwrap();

    let arrow_schema = unsafe {
        ArrowSchema::from_raw((&mut ffi_schema as *mut arrow_otap::ffi::FFI_ArrowSchema).cast())
    };
    let arrow_array = unsafe {
        ArrowArray::from_raw((&mut ffi_array as *mut arrow_otap::ffi::FFI_ArrowArray).cast())
    };

    conn.register_arrow_array("simple", &arrow_schema, &arrow_array)?;

    let result = conn.query(
        "SELECT * FROM arrowstream('simple') ORDER BY b",
        OutputFormat::TabSeparated,
    )?;
    assert_eq!(result.data_utf8_lossy(), "x\t1\ny\t2\n");

    conn.unregister_arrow_table("simple")?;
    Ok(())
}

#[test]
fn mount_batch_with_binary_columns() -> chdb_rust::error::Result<()> {
    use arrow_otap::array::{BinaryArray, UInt64Array};

    let conn = Connection::open_in_memory()?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Binary, true),
        Field::new("time_unix_nano", DataType::UInt64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(BinaryArray::from_iter([
                Some([1u8; 16].as_slice()),
                None,
            ])),
            Arc::new(UInt64Array::from(vec![1_u64, 2])),
        ],
    )
    .unwrap();

    let struct_array = StructArray::from(batch);
    let (mut ffi_array, mut ffi_schema) = arrow_otap::ffi::to_ffi(&struct_array.to_data()).unwrap();
    let arrow_schema = unsafe {
        ArrowSchema::from_raw((&mut ffi_schema as *mut arrow_otap::ffi::FFI_ArrowSchema).cast())
    };
    let arrow_array = unsafe {
        ArrowArray::from_raw((&mut ffi_array as *mut arrow_otap::ffi::FFI_ArrowArray).cast())
    };

    conn.register_arrow_array("binary_cols", &arrow_schema, &arrow_array)?;
    let result = conn.query(
        "SELECT count() FROM arrowstream('binary_cols')",
        OutputFormat::TabSeparated,
    )?;
    assert_eq!(result.data_utf8_lossy().trim(), "2");
    conn.unregister_arrow_table("binary_cols")?;
    Ok(())
}

#[test]
fn mount_materialized_otap_resource_attrs() -> chdb_rust::error::Result<()> {
    use arrow_otap::array::UInt8Array;

    let conn = Connection::open_in_memory()?;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("parent_id", DataType::UInt16, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("type", DataType::UInt8, false),
            Field::new("str", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(arrow_otap::array::UInt16Array::from_iter_values(vec![0, 0, 1])),
            Arc::new(StringArray::from(vec!["service.name", "host.name", "service.name"])),
            Arc::new(UInt8Array::from_iter_values(vec![1, 1, 1])),
            Arc::new(StringArray::from(vec![Some("checkout"), Some("host-a"), Some("search")])),
        ],
    )
    .unwrap();

    let struct_array = StructArray::from(batch);
    let (mut ffi_array, mut ffi_schema) = arrow_otap::ffi::to_ffi(&struct_array.to_data()).unwrap();
    let arrow_schema = unsafe {
        ArrowSchema::from_raw((&mut ffi_schema as *mut arrow_otap::ffi::FFI_ArrowSchema).cast())
    };
    let arrow_array = unsafe {
        ArrowArray::from_raw((&mut ffi_array as *mut arrow_otap::ffi::FFI_ArrowArray).cast())
    };
    conn.register_arrow_array("otap_resource_attrs", &arrow_schema, &arrow_array)?;

    let result = conn.query(
        "SELECT count() FROM arrowstream('otap_resource_attrs')",
        OutputFormat::TabSeparated,
    )?;
    assert_eq!(result.data_utf8_lossy().trim(), "3");

    conn.unregister_arrow_table("otap_resource_attrs")?;
    Ok(())
}
