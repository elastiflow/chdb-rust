//! ClickHouse query parameter values for `{name:Type}` placeholders.
//!
//! Values are encoded as strings for the libchdb C API.
//!
//! After the encoded values have been sent to chDB, they are parsed by the chDB core library and
//! substituted in the query during planning.

use std::borrow::Cow;
use std::ffi::{c_char, CString};

use crate::error::Result;

/// Builder for mixed-type parameter maps.
///
/// # Examples
///
/// ```no_run
/// use chdb_rust::connection::Connection;
/// use chdb_rust::format::OutputFormat;
/// use chdb_rust::query_param::QueryParams;
///
/// let conn = Connection::open_in_memory()?;
/// let params = QueryParams::new()
///     .bind("x", 5_u64)
///     .bind("label", "ok");
/// let _ = conn.query_with_params(
///     "SELECT {x:UInt64} AS x, {label:String} AS label",
///     OutputFormat::CSV,
///     params,
/// )?;
/// # Ok::<(), chdb_rust::error::Error>(())
/// ```
#[derive(Debug, Default, Clone)]
pub struct QueryParams {
    pairs: Vec<(String, QueryParam)>,
}

impl QueryParams {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bind(mut self, name: impl AsRef<str>, value: impl Into<QueryParam>) -> Self {
        self.pairs.push((name.as_ref().to_owned(), value.into()));
        self
    }
}

impl IntoIterator for QueryParams {
    type Item = (String, QueryParam);
    type IntoIter = std::vec::IntoIter<(String, QueryParam)>;

    fn into_iter(self) -> Self::IntoIter {
        self.pairs.into_iter()
    }
}

impl<'a> IntoIterator for &'a QueryParams {
    type Item = (&'a str, &'a QueryParam);
    type IntoIter = std::iter::Map<
        std::slice::Iter<'a, (String, QueryParam)>,
        fn(&'a (String, QueryParam)) -> (&'a str, &'a QueryParam),
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.pairs
            .iter()
            .map(|(name, value)| (name.as_str(), value))
    }
}

/// A value bound to a `{name:Type}` placeholder in a parameterized query.
///
/// Use [`From`] conversions for scalars, strings, options, and arrays. For pre-formatted
/// ClickHouse literals (tuples, etc.), use [`Self::raw`].
#[derive(Debug, Clone, PartialEq)]
pub enum QueryParam {
    /// SQL NULL for `Nullable(...)` placeholders (`\N`).
    Null,
    Bool(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    /// Raw text for `String`, `Date`, `Identifier`, and similar placeholders.
    Text(String),
    /// Pre-formatted ClickHouse literal (e.g. `(7,'x')`, `[1, 2, 3]`).
    Raw(String),
    /// ClickHouse `Array(...)` value, encoded as `[...]`.
    Array(Vec<QueryParam>),
}

impl QueryParam {
    /// Pass a pre-formatted ClickHouse parameter literal through unchanged.
    pub fn raw(value: impl Into<String>) -> Self {
        Self::Raw(value.into())
    }

    /// Encode this value as a root-level ClickHouse query parameter string.
    ///
    /// Root strings are unquoted. Nested values inside [`Self::Array`] use
    /// nested rules (quoted strings, `NULL`).
    pub fn encode(&self) -> Result<Cow<'_, str>> {
        Ok(match self {
            Self::Null => Cow::Borrowed("\\N"),
            Self::Bool(true) => Cow::Borrowed("true"),
            Self::Bool(false) => Cow::Borrowed("false"),
            Self::Int64(value) => Cow::Owned(value.to_string()),
            Self::UInt64(value) => Cow::Owned(value.to_string()),
            Self::Float64(value) => Cow::Owned(value.to_string()),
            Self::Text(value) | Self::Raw(value) => Cow::Borrowed(value),
            Self::Array(values) => Cow::Owned(Self::encode_array(values)?),
        })
    }

    /// Encode this value as an element inside an array/tuple/map literal.
    fn encode_nested(&self) -> Result<Cow<'_, str>> {
        Ok(match self {
            Self::Null => Cow::Borrowed("NULL"),
            Self::Bool(true) => Cow::Borrowed("true"),
            Self::Bool(false) => Cow::Borrowed("false"),
            Self::Int64(value) => Cow::Owned(value.to_string()),
            Self::UInt64(value) => Cow::Owned(value.to_string()),
            Self::Float64(value) => Cow::Owned(value.to_string()),
            Self::Text(value) => Cow::Owned(Self::quote_nested_string(value)),
            Self::Raw(value) => Cow::Borrowed(value),
            Self::Array(values) => Cow::Owned(Self::encode_array(values)?),
        })
    }

    fn encode_array(values: &[QueryParam]) -> Result<String> {
        let mut parts = Vec::with_capacity(values.len());
        for value in values {
            parts.push(value.encode_nested()?.into_owned());
        }
        Ok(format!("[{}]", parts.join(", ")))
    }

    fn quote_nested_string(value: &str) -> String {
        let mut encoded = String::with_capacity(value.len() + 2);
        encoded.push('\'');
        for ch in value.chars() {
            if ch == '\'' || ch == '\\' {
                encoded.push('\\');
            }
            encoded.push(ch);
        }
        encoded.push('\'');
        encoded
    }
}

/// NUL-terminated name/value C strings ready for `chdb_query_with_params`.
///
/// Pointers returned by [`Self::names_ptr`] / [`Self::values_ptr`] are valid only
/// while this struct remains alive. Callers must keep `EncodedParams` live across
/// the FFI call that consumes those pointers. The chDB C API is assumed to copy
/// parameter names and values during that call and not retain the pointers.
pub(crate) struct EncodedParams {
    _name_cstrs: Vec<CString>,
    _value_cstrs: Vec<CString>,
    name_ptrs: Vec<*const c_char>,
    value_ptrs: Vec<*const c_char>,
}

impl EncodedParams {
    pub(crate) fn encode<K, V, I>(params: I) -> Result<Self>
    where
        K: AsRef<str>,
        V: Into<QueryParam>,
        I: IntoIterator<Item = (K, V)>,
    {
        let mut name_cstrs = Vec::new();
        let mut value_cstrs = Vec::new();

        for (name, value) in params {
            let param = value.into();
            let encoded = param.encode()?;
            name_cstrs.push(CString::new(name.as_ref())?);
            value_cstrs.push(CString::new(encoded.as_ref())?);
        }

        let name_ptrs = name_cstrs.iter().map(|s| s.as_ptr()).collect();
        let value_ptrs = value_cstrs.iter().map(|s| s.as_ptr()).collect();

        Ok(Self {
            _name_cstrs: name_cstrs,
            _value_cstrs: value_cstrs,
            name_ptrs,
            value_ptrs,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.name_ptrs.len()
    }

    /// Pointer to the parallel name C-string array, or null when empty.
    pub(crate) fn names_ptr(&self) -> *const *const c_char {
        if self.name_ptrs.is_empty() {
            std::ptr::null()
        } else {
            self.name_ptrs.as_ptr()
        }
    }

    /// Pointer to the parallel value C-string array, or null when empty.
    pub(crate) fn values_ptr(&self) -> *const *const c_char {
        if self.value_ptrs.is_empty() {
            std::ptr::null()
        } else {
            self.value_ptrs.as_ptr()
        }
    }
}

impl From<bool> for QueryParam {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i8> for QueryParam {
    fn from(value: i8) -> Self {
        Self::Int64(i64::from(value))
    }
}

impl From<i16> for QueryParam {
    fn from(value: i16) -> Self {
        Self::Int64(i64::from(value))
    }
}

impl From<i32> for QueryParam {
    fn from(value: i32) -> Self {
        Self::Int64(i64::from(value))
    }
}

impl From<i64> for QueryParam {
    fn from(value: i64) -> Self {
        Self::Int64(value)
    }
}

impl From<u8> for QueryParam {
    fn from(value: u8) -> Self {
        Self::UInt64(u64::from(value))
    }
}

impl From<u16> for QueryParam {
    fn from(value: u16) -> Self {
        Self::UInt64(u64::from(value))
    }
}

impl From<u32> for QueryParam {
    fn from(value: u32) -> Self {
        Self::UInt64(u64::from(value))
    }
}

impl From<u64> for QueryParam {
    fn from(value: u64) -> Self {
        Self::UInt64(value)
    }
}

impl From<f32> for QueryParam {
    fn from(value: f32) -> Self {
        Self::Float64(f64::from(value))
    }
}

impl From<f64> for QueryParam {
    fn from(value: f64) -> Self {
        Self::Float64(value)
    }
}

impl From<String> for QueryParam {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<&str> for QueryParam {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

impl From<&QueryParam> for QueryParam {
    fn from(value: &QueryParam) -> Self {
        value.clone()
    }
}

impl<T> From<Option<T>> for QueryParam
where
    T: Into<QueryParam>,
{
    fn from(value: Option<T>) -> Self {
        match value {
            None => Self::Null,
            Some(value) => value.into(),
        }
    }
}

impl<T> From<Vec<T>> for QueryParam
where
    T: Into<QueryParam>,
{
    fn from(values: Vec<T>) -> Self {
        Self::Array(values.into_iter().map(Into::into).collect())
    }
}

impl<T> From<&[T]> for QueryParam
where
    T: Into<QueryParam> + Clone,
{
    fn from(values: &[T]) -> Self {
        Self::from(values.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;

    #[test]
    fn encode_root_covers_every_variant() -> Result<()> {
        assert_eq!(QueryParam::Null.encode()?, "\\N");
        assert_eq!(QueryParam::Bool(true).encode()?, "true");
        assert_eq!(QueryParam::Bool(false).encode()?, "false");
        assert_eq!(QueryParam::Int64(-42).encode()?, "-42");
        assert_eq!(QueryParam::UInt64(42).encode()?, "42");
        assert_eq!(QueryParam::Float64(1.5).encode()?, "1.5");
        assert_eq!(QueryParam::Text(String::new()).encode()?, "");
        assert_eq!(QueryParam::Text("hello".into()).encode()?, "hello");
        assert_eq!(QueryParam::Text("it's".into()).encode()?, "it's");
        assert_eq!(QueryParam::Text(r"a\b".into()).encode()?, r"a\b");
        assert_eq!(QueryParam::raw("").encode()?, "");
        assert_eq!(QueryParam::raw("(7,'x')").encode()?, "(7,'x')");
        assert_eq!(QueryParam::raw("[1, 2, 3]").encode()?, "[1, 2, 3]");
        assert_eq!(QueryParam::Array(vec![]).encode()?, "[]");
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Int64(1), QueryParam::Int64(2)]).encode()?,
            "[1, 2]"
        );
        Ok(())
    }

    #[test]
    fn encode_nested_covers_every_variant_via_array() -> Result<()> {
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Null]).encode()?,
            "[NULL]"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Bool(true), QueryParam::Bool(false)]).encode()?,
            "[true, false]"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Int64(-1), QueryParam::UInt64(2)]).encode()?,
            "[-1, 2]"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Float64(1.5)]).encode()?,
            "[1.5]"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Text(String::new())]).encode()?,
            "['']"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Text("hello".into())]).encode()?,
            "['hello']"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::raw("(7,'x')")]).encode()?,
            "[(7,'x')]"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Array(vec![
                QueryParam::Int64(1),
                QueryParam::Int64(2),
            ])])
            .encode()?,
            "[[1, 2]]"
        );
        Ok(())
    }

    #[test]
    fn encode_nested_escapes_quotes_and_backslashes_in_text() -> Result<()> {
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Text("'".into())]).encode()?,
            r"['\'']"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Text(r"\".into())]).encode()?,
            r"['\\']"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Text("it's".into())]).encode()?,
            r"['it\'s']"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Text(r"a\b".into())]).encode()?,
            r"['a\\b']"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Text(r"'\".into())]).encode()?,
            r"['\'\\']"
        );
        assert_eq!(
            QueryParam::Array(vec![
                QueryParam::Text("a".into()),
                QueryParam::Text("b".into()),
            ])
            .encode()?,
            "['a', 'b']"
        );
        Ok(())
    }

    #[test]
    fn encode_array_mixed_and_nested_shapes() -> Result<()> {
        assert_eq!(
            QueryParam::Array(vec![
                QueryParam::Int64(1),
                QueryParam::Null,
                QueryParam::Text("x".into()),
                QueryParam::Bool(true),
            ])
            .encode()?,
            "[1, NULL, 'x', true]"
        );
        assert_eq!(
            QueryParam::Array(vec![
                QueryParam::Array(vec![QueryParam::Int64(1), QueryParam::Int64(2)]),
                QueryParam::Array(vec![QueryParam::Int64(3), QueryParam::Int64(4)]),
            ])
            .encode()?,
            "[[1, 2], [3, 4]]"
        );
        assert_eq!(
            QueryParam::Array(vec![QueryParam::Array(vec![QueryParam::Text(
                "it's".into()
            )])])
            .encode()?,
            r"[['it\'s']]"
        );
        Ok(())
    }

    #[test]
    fn from_option_none_encodes_as_root_null() -> Result<()> {
        let value: Option<i64> = None;
        assert_eq!(QueryParam::from(value).encode()?, "\\N");
        Ok(())
    }

    #[test]
    fn from_vec_and_slice_encode_as_arrays() -> Result<()> {
        assert_eq!(QueryParam::from(Vec::<u64>::new()).encode()?, "[]");
        assert_eq!(QueryParam::from(vec![1_u64, 2, 3]).encode()?, "[1, 2, 3]");
        assert_eq!(QueryParam::from(vec![1_i32, 2, 3]).encode()?, "[1, 2, 3]");
        assert_eq!(
            QueryParam::from([4_i64, 5, 6].as_slice()).encode()?,
            "[4, 5, 6]"
        );
        assert_eq!(
            QueryParam::from(vec!["a".to_owned(), "b".to_owned()]).encode()?,
            "['a', 'b']"
        );
        let values: Vec<Option<i64>> = vec![Some(1), None, Some(3)];
        assert_eq!(QueryParam::from(values).encode()?, "[1, NULL, 3]");
        assert_eq!(
            QueryParam::from(vec![vec![1_u64, 2], vec![3, 4]]).encode()?,
            "[[1, 2], [3, 4]]"
        );
        Ok(())
    }

    #[test]
    fn from_vec_builds_array_variant() {
        assert_eq!(
            QueryParam::from(vec![1_i64, 2]),
            QueryParam::Array(vec![QueryParam::Int64(1), QueryParam::Int64(2)])
        );
    }

    #[test]
    fn query_params_builder_collects_mixed_types() {
        let params = QueryParams::new().bind("x", 5_u64).bind("label", "ok");
        let collected: Vec<_> = params.into_iter().collect();
        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0].0, "x");
        assert_eq!(collected[0].1, QueryParam::UInt64(5));
        assert_eq!(collected[1].0, "label");
        assert_eq!(collected[1].1, QueryParam::Text("ok".into()));
    }

    #[test]
    fn encoded_params_empty_exposes_null_name_and_value_ptrs() -> Result<()> {
        let encoded = EncodedParams::encode(std::iter::empty::<(&str, QueryParam)>())?;

        assert_eq!(encoded.len(), 0);
        assert!(encoded.names_ptr().is_null());
        assert!(encoded.values_ptr().is_null());
        Ok(())
    }

    #[test]
    fn encoded_params_exposes_parallel_name_and_value_ptrs() -> Result<()> {
        let encoded = EncodedParams::encode([
            ("x", QueryParam::from(5_u64)),
            ("label", QueryParam::from("ok")),
        ])?;

        assert_eq!(encoded.len(), 2);
        assert!(!encoded.names_ptr().is_null());
        assert!(!encoded.values_ptr().is_null());

        unsafe {
            assert_eq!(CStr::from_ptr(*encoded.names_ptr()).to_bytes(), b"x");
            assert_eq!(
                CStr::from_ptr(*encoded.names_ptr().add(1)).to_bytes(),
                b"label"
            );
            assert_eq!(CStr::from_ptr(*encoded.values_ptr()).to_bytes(), b"5");
            assert_eq!(
                CStr::from_ptr(*encoded.values_ptr().add(1)).to_bytes(),
                b"ok"
            );
        }
        Ok(())
    }
}
