//! Mount Arrow record batches as chDB `arrowstream()` table functions.
//!
//! OTAP batches use arrow 57; chDB reads them through the Arrow C Data Interface.
//! Keep returned [`MountedArrowBatch`] values alive until queries finish, then call
//! [`Connection::unregister_arrow_table`](chdb_rust::connection::Connection::unregister_arrow_table).

use arrow_otap::array::{Array, StructArray};
use arrow_otap::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_otap::record_batch::RecordBatch;
use chdb_rust::arrow_stream::{ArrowArray, ArrowSchema};
use chdb_rust::connection::Connection;
use chdb_rust::error::{Error, Result};

/// Keeps Arrow C Data Interface exports alive while chDB reads them.
pub struct MountedArrowBatch {
    pub name: String,
    _ffi_array: FFI_ArrowArray,
    _ffi_schema: FFI_ArrowSchema,
}

impl MountedArrowBatch {
    /// Register a record batch under `name` so it can be queried as `arrowstream('name')`.
    pub fn mount(conn: &Connection, name: &str, batch: &RecordBatch) -> Result<Box<Self>> {
        let struct_array = StructArray::from(batch.clone());
        let (ffi_array, ffi_schema) =
            arrow_otap::ffi::to_ffi(&struct_array.to_data()).map_err(|e| Error::QueryError(e.to_string()))?;

        let mut mounted = Box::new(Self {
            name: name.to_string(),
            _ffi_array: ffi_array,
            _ffi_schema: ffi_schema,
        });

        let schema = unsafe {
            ArrowSchema::from_raw((&mut mounted._ffi_schema as *mut FFI_ArrowSchema).cast())
        };
        let array = unsafe {
            ArrowArray::from_raw((&mut mounted._ffi_array as *mut FFI_ArrowArray).cast())
        };

        conn.register_arrow_array(name, &schema, &array)?;

        Ok(mounted)
    }

    pub fn table_name(&self) -> &str {
        &self.name
    }

    pub fn unmount(&self, conn: &Connection) -> Result<()> {
        conn.unregister_arrow_table(self.table_name())
    }
}
