//! Convert OTAP Arrow record batches into a shape chDB can import via `arrowstream()`.
//!
//! OTAP batches use dictionary encoding and fixed-size binary IDs that chDB's Arrow
//! importer does not always handle. These helpers decode those types and project a
//! small set of scalar log columns for the example DDL.

use arrow_otap::array::{Array, ArrayRef, RecordBatch, StructArray};
use arrow_otap::compute::cast;
use arrow_otap::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;

const LOG_COLUMN_NAMES: [&str; 4] = [
    "time_unix_nano",
    "scope_name",
    "severity_text",
    "body_str",
];

const TIME_UNIX_NANO_TYPE: DataType = DataType::Timestamp(TimeUnit::Nanosecond, None);

/// Prepare an OTAP logs batch with just the columns used by the example table.
pub fn prepare_logs_for_chdb(batch: &RecordBatch) -> Result<RecordBatch, arrow_otap::error::ArrowError> {
    let decoded = if batch.schema().field_with_name("resource").is_ok()
        || batch.schema().field_with_name("scope").is_ok()
        || batch.schema().field_with_name("body").is_ok()
    {
        flatten_logs_batch(batch)?
    } else {
        materialize_for_chdb(batch)?
    };
    project_log_columns(&decoded)
}

fn project_log_columns(batch: &RecordBatch) -> Result<RecordBatch, arrow_otap::error::ArrowError> {
    let mut fields = Vec::with_capacity(LOG_COLUMN_NAMES.len());
    let mut columns = Vec::with_capacity(LOG_COLUMN_NAMES.len());

    for name in LOG_COLUMN_NAMES {
        let nullable = batch
            .schema()
            .field_with_name(name)
            .map(|field| field.is_nullable())
            .map_err(|_| {
                arrow_otap::error::ArrowError::SchemaError(format!("missing log column: {name}"))
            })?;
        let column = batch
            .column_by_name(name)
            .expect("field present but column missing");
        let data_type = if name == "time_unix_nano" {
            TIME_UNIX_NANO_TYPE.clone()
        } else {
            column.data_type().clone()
        };
        fields.push(Field::new(name, data_type, nullable));
        columns.push(column.clone());
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
}

pub fn materialize_for_chdb(batch: &RecordBatch) -> Result<RecordBatch, arrow_otap::error::ArrowError> {
    let columns: Vec<ArrayRef> = batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| decode_column(field, column))
        .collect::<Result<_, _>>()?;

    let fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .zip(columns.iter())
        .map(|(field, column)| {
            Field::new(
                field.name(),
                column.data_type().clone(),
                field.is_nullable(),
            )
        })
        .collect();

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
}

fn decode_column(field: &Field, column: &ArrayRef) -> Result<ArrayRef, arrow_otap::error::ArrowError> {
    match field.data_type() {
        DataType::Dictionary(_, value_type) => {
            let decoded = cast(column, value_type)?;
            decode_column(
                &Field::new(field.name(), value_type.as_ref().clone(), field.is_nullable()),
                &decoded,
            )
        }
        DataType::FixedSizeBinary(_) => cast(column, &DataType::Binary),
        DataType::Struct(subfields) => {
            let struct_array = column.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
                arrow_otap::error::ArrowError::CastError(format!(
                    "expected struct array for column {}",
                    field.name()
                ))
            })?;
            let decoded: Result<Vec<ArrayRef>, _> = subfields
                .iter()
                .zip(struct_array.columns())
                .map(|(subfield, subcolumn)| decode_column(subfield, subcolumn))
                .collect();
            let decoded = decoded?;
            let fields: arrow_otap::datatypes::Fields = subfields
                .iter()
                .zip(decoded.iter())
                .map(|(subfield, subcolumn)| {
                    Field::new(
                        subfield.name(),
                        subcolumn.data_type().clone(),
                        subfield.is_nullable(),
                    )
                })
                .collect();
            Ok(Arc::new(StructArray::try_new(
                fields,
                decoded,
                struct_array.nulls().cloned(),
            )?))
        }
        _ => Ok(column.clone()),
    }
}

fn flatten_logs_batch(batch: &RecordBatch) -> Result<RecordBatch, arrow_otap::error::ArrowError> {
    let mut fields = Vec::new();
    let mut columns = Vec::new();

    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        match (field.name().as_str(), field.data_type()) {
            ("scope", DataType::Struct(subfields)) => {
                let struct_array = column.as_any().downcast_ref::<StructArray>().unwrap();
                for (subfield, subcolumn) in subfields.iter().zip(struct_array.columns()) {
                    let decoded = decode_column(subfield, subcolumn)?;
                    let name = match subfield.name().as_str() {
                        "name" => "scope_name",
                        "version" => "scope_version",
                        other => other,
                    };
                    fields.push(Field::new(
                        name,
                        decoded.data_type().clone(),
                        subfield.is_nullable(),
                    ));
                    columns.push(decoded);
                }
            }
            ("body", DataType::Struct(subfields)) => {
                let struct_array = column.as_any().downcast_ref::<StructArray>().unwrap();
                for (subfield, subcolumn) in subfields.iter().zip(struct_array.columns()) {
                    let decoded = decode_column(subfield, subcolumn)?;
                    let name = format!("body_{}", subfield.name());
                    fields.push(Field::new(
                        name,
                        decoded.data_type().clone(),
                        subfield.is_nullable(),
                    ));
                    columns.push(decoded);
                }
            }
            _ => {
                let decoded = decode_column(field, column)?;
                fields.push(Field::new(
                    field.name(),
                    decoded.data_type().clone(),
                    field.is_nullable(),
                ));
                columns.push(decoded);
            }
        }
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
}
