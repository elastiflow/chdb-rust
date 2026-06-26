//! Randomized [`OtapPayload`] generators for OTAP table-layout experiments.
//!
//! Each generator is an iterator that yields payloads containing batches of
//! log records, spans, or metric data points. Use the builders to control
//! volume and reproducibility:
//!
//! ```no_run
//! use common::otap_payload_gen::{self, MetricKind, StreamConfig};
//!
//! let config = StreamConfig::new(100_000).batch_size(1_000).seed(42);
//!
//! for payload in otap_payload_gen::logs(config) {
//!     // stream payloads into a database loader, round-trip test, etc.
//!     let _ = payload.num_items();
//! }
//!
//! for payload in otap_payload_gen::metrics(MetricKind::Histogram, config) {
//!     let _ = payload;
//! }
//! ```

use otap_df_pdata::OtapPayload;
use otap_df_pdata::proto::OtlpProtoMessage;
use otap_df_pdata::proto::opentelemetry::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use otap_df_pdata::proto::opentelemetry::logs::v1::{
    LogRecord, LogsData, ResourceLogs, ScopeLogs, SeverityNumber,
};
use otap_df_pdata::proto::opentelemetry::metrics::v1::exponential_histogram_data_point::Buckets;
use otap_df_pdata::proto::opentelemetry::metrics::v1::summary_data_point::ValueAtQuantile;
use otap_df_pdata::proto::opentelemetry::metrics::v1::{
    AggregationTemporality, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
    HistogramDataPoint, Metric, MetricsData, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
    Summary, SummaryDataPoint,
};
use otap_df_pdata::proto::opentelemetry::resource::v1::Resource;
use otap_df_pdata::proto::opentelemetry::trace::v1::span::{Event, Link, SpanKind};
use otap_df_pdata::proto::opentelemetry::trace::v1::status::StatusCode;
use otap_df_pdata::proto::opentelemetry::trace::v1::{
    ResourceSpans, ScopeSpans, Span, Status, TracesData,
};
use otap_df_pdata::testing::round_trip::otlp_to_otap;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;

const BASE_TIME_NS: u64 = 1_736_937_000_000_000_000;
const ONE_SEC_NS: u64 = 1_000_000_000;

const SERVICE_NAMES: &[&str] = &[
    "checkout",
    "inventory",
    "search",
    "billing",
    "notifications",
    "auth",
];

const HOST_NAMES: &[&str] = &[
    "host-a",
    "host-b",
    "host-c",
    "host-d",
    "host-e",
];

const SCOPE_NAMES: &[&str] = &[
    "io.opentelemetry.http",
    "io.opentelemetry.grpc",
    "io.opentelemetry.db",
    "io.opentelemetry.messaging",
];

const LOG_MESSAGES: &[&str] = &[
    "request completed",
    "cache miss",
    "retry scheduled",
    "connection reset",
    "validation failed",
    "downstream timeout",
];

const SPAN_NAMES: &[&str] = &[
    "HTTP GET",
    "HTTP POST",
    "grpc.invoke",
    "db.query",
    "publish message",
    "process batch",
];

const METRIC_UNITS: &[&str] = &["1", "ms", "s", "By", "MiBy"];

const ATTR_KEYS: &[&str] = &[
    "service.version",
    "deployment.environment",
    "cloud.region",
    "cloud.availability_zone",
    "k8s.pod.name",
    "k8s.namespace.name",
    "http.method",
    "http.route",
    "db.system",
    "messaging.system",
];

/// OTLP metric aggregation families supported by the generators.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricKind {
    Gauge,
    Sum,
    Histogram,
    ExponentialHistogram,
    Summary,
}

impl MetricKind {
    pub const ALL: [Self; 5] = [
        Self::Gauge,
        Self::Sum,
        Self::Histogram,
        Self::ExponentialHistogram,
        Self::Summary,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Gauge => "gauge",
            Self::Sum => "sum",
            Self::Histogram => "histogram",
            Self::ExponentialHistogram => "exponential_histogram",
            Self::Summary => "summary",
        }
    }
}

/// Controls how many items are generated and how they are batched.
///
/// Pass to [`logs`], [`spans`], or [`metrics`]. The iterator reads
/// `total_items` and `items_per_payload` at construction time and yields
/// that many primary records (log records, spans, or data points) split
/// across batches of at most `items_per_payload` per [`OtapPayload`].
#[derive(Clone, Copy, Debug)]
pub struct StreamConfig {
    total_items: usize,
    items_per_payload: usize,
    seed: u64,
}

impl StreamConfig {
    pub const fn new(total_items: usize) -> Self {
        Self {
            total_items,
            items_per_payload: 1_000,
            seed: 0,
        }
    }

    pub fn batch_size(mut self, items_per_payload: usize) -> Self {
        self.items_per_payload = items_per_payload.max(1);
        self
    }

    pub const fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self::new(10_000)
    }
}

enum PayloadKind {
    Logs,
    Spans,
    Metrics(MetricKind),
}

/// Iterator that yields randomized [`OtapPayload`] values in batches.
pub struct OtapPayloadIter {
    total_items: usize,
    remaining: usize,
    batch_size: usize,
    rng: StdRng,
    kind: PayloadKind,
    sequence: u64,
}

impl OtapPayloadIter {
    fn new(kind: PayloadKind, config: StreamConfig) -> Self {
        let batch_size = config.items_per_payload.max(1);
        Self {
            total_items: config.total_items,
            remaining: config.total_items,
            batch_size,
            rng: StdRng::seed_from_u64(config.seed),
            kind,
            sequence: 0,
        }
    }

    /// Total primary records this iterator will yield across all payloads.
    pub const fn total_items(&self) -> usize {
        self.total_items
    }

    /// Maximum primary records packed into each yielded payload.
    pub const fn items_per_payload(&self) -> usize {
        self.batch_size
    }
}

impl ExactSizeIterator for OtapPayloadIter {
    fn len(&self) -> usize {
        self.remaining.div_ceil(self.batch_size)
    }
}

impl Iterator for OtapPayloadIter {
    type Item = OtapPayload;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        let batch_count = self.batch_size.min(self.remaining);
        self.remaining -= batch_count;

        let payload = match self.kind {
            PayloadKind::Logs => logs_payload(&mut self.rng, batch_count, self.sequence),
            PayloadKind::Spans => spans_payload(&mut self.rng, batch_count, self.sequence),
            PayloadKind::Metrics(kind) => {
                metrics_payload(&mut self.rng, kind, batch_count, self.sequence)
            }
        };
        self.sequence += batch_count as u64;

        Some(payload)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let payloads = self.remaining.div_ceil(self.batch_size);
        (payloads, Some(payloads))
    }
}

/// Generate randomized log payloads.
#[must_use]
pub fn logs(config: StreamConfig) -> OtapPayloadIter {
    OtapPayloadIter::new(PayloadKind::Logs, config)
}

/// Generate randomized span payloads.
#[must_use]
pub fn spans(config: StreamConfig) -> OtapPayloadIter {
    OtapPayloadIter::new(PayloadKind::Spans, config)
}

/// Generate randomized metric payloads for a single metric type.
#[must_use]
pub fn metrics(kind: MetricKind, config: StreamConfig) -> OtapPayloadIter {
    OtapPayloadIter::new(PayloadKind::Metrics(kind), config)
}

/// Generate iterators for every metric type using the same stream config.
pub fn all_metric_kinds(
    config: StreamConfig,
) -> impl Iterator<Item = (MetricKind, OtapPayloadIter)> {
    MetricKind::ALL
        .into_iter()
        .map(move |kind| (kind, metrics(kind, config)))
}

fn logs_payload(rng: &mut StdRng, count: usize, sequence: u64) -> OtapPayload {
    let records: Vec<LogRecord> = (0..count)
        .map(|offset| random_log_record(rng, sequence + offset as u64))
        .collect();

    let logs = LogsData::new(vec![ResourceLogs::new(
        random_resource(rng),
        vec![ScopeLogs::new(random_scope(rng), records)],
    )]);

    OtapPayload::from(otlp_to_otap(&OtlpProtoMessage::Logs(logs)))
}

fn spans_payload(rng: &mut StdRng, count: usize, sequence: u64) -> OtapPayload {
    let spans: Vec<Span> = (0..count)
        .map(|offset| random_span(rng, sequence + offset as u64))
        .collect();

    let traces = TracesData::new(vec![ResourceSpans::new(
        random_resource(rng),
        vec![ScopeSpans::new(random_scope(rng), spans)],
    )]);

    OtapPayload::from(otlp_to_otap(&OtlpProtoMessage::Traces(traces)))
}

fn metrics_payload(rng: &mut StdRng, kind: MetricKind, count: usize, sequence: u64) -> OtapPayload {
    let metric = random_metric(rng, kind, count, sequence);
    let metrics = MetricsData::new(vec![ResourceMetrics::new(
        random_resource(rng),
        vec![ScopeMetrics::new(random_scope(rng), vec![metric])],
    )]);

    OtapPayload::from(otlp_to_otap(&OtlpProtoMessage::Metrics(metrics)))
}

fn random_resource(rng: &mut StdRng) -> Resource {
    Resource::build()
        .attributes(vec![
            KeyValue::new(
                "service.name",
                AnyValue::new_string(pick(rng, SERVICE_NAMES).to_string()),
            ),
            KeyValue::new(
                "host.name",
                AnyValue::new_string(pick(rng, HOST_NAMES).to_string()),
            ),
            KeyValue::new(
                "service.instance.id",
                AnyValue::new_string(format!("instance-{}", rng.random_range(1..10_000))),
            ),
        ])
        .finish()
}

fn random_scope(rng: &mut StdRng) -> InstrumentationScope {
    let attr_count = rng.random_range(0..=3);
    InstrumentationScope::build()
        .name(pick(rng, SCOPE_NAMES).to_string())
        .version(format!(
            "{}.{}.{}",
            rng.random_range(0..5),
            rng.random_range(0..20),
            rng.random_range(0..100)
        ))
        .attributes(random_attributes(rng, attr_count))
        .finish()
}

fn random_log_record(rng: &mut StdRng, sequence: u64) -> LogRecord {
    let severity = random_severity(rng);
    let time = BASE_TIME_NS + sequence.wrapping_mul(ONE_SEC_NS / 10);
    let observed = time + rng.random_range(1_000..50_000_000);

    let attr_count = rng.random_range(1..=6);
    let mut builder = LogRecord::build()
        .time_unix_nano(time)
        .observed_time_unix_nano(observed)
        .severity_number(severity)
        .severity_text(severity.as_str_name())
        .body(AnyValue::new_string(format!(
            "{} (#{sequence})",
            pick(rng, LOG_MESSAGES)
        )))
        .attributes(random_attributes(rng, attr_count));

    if rng.random_bool(0.6) {
        builder = builder.event_name(format!("event.{sequence}"));
    }

    if rng.random_bool(0.4) {
        builder = builder
            .trace_id(random_trace_id(rng))
            .span_id(random_span_id(rng));
    }

    builder.finish()
}

/// Generates a random span.
///
/// Does not result a coherent trace when generating multiple spans; parent_span_id
/// and trace_id are randomized.
fn random_span(rng: &mut StdRng, sequence: u64) -> Span {
    let start = BASE_TIME_NS + sequence.wrapping_mul(ONE_SEC_NS / 5);
    let duration = rng.random_range(100_000..500_000_000);
    let trace_id = random_trace_id(rng);
    let span_id = random_span_id(rng);

    let attr_count = rng.random_range(1..=8);
    let mut builder = Span::build()
        .trace_id(trace_id)
        .span_id(span_id)
        .name(format!("{} #{sequence}", pick(rng, SPAN_NAMES)))
        .kind(random_span_kind(rng))
        .start_time_unix_nano(start)
        .end_time_unix_nano(start + duration)
        .status(random_status(rng))
        .attributes(random_attributes(rng, attr_count));

    if rng.random_bool(0.5) {
        builder = builder.parent_span_id(random_span_id(rng));
    }

    if rng.random_bool(0.5) {
        let event_count = rng.random_range(1..=3);
        let events: Vec<Event> = (0..event_count)
            .map(|idx| {
                let event_attr_count = rng.random_range(0..=3);
                Event::build()
                    .name(format!("event-{idx}"))
                    .time_unix_nano(start + rng.random_range(0..duration))
                    .attributes(random_attributes(rng, event_attr_count))
                    .finish()
            })
            .collect();
        builder = builder.events(events);
    }

    if rng.random_bool(0.3) {
        let link_attr_count = rng.random_range(0..=2);
        builder = builder.links(vec![Link::build()
            .trace_id(random_trace_id(rng))
            .span_id(random_span_id(rng))
            .attributes(random_attributes(rng, link_attr_count))
            .finish()]);
    }

    builder.finish()
}

fn random_metric(rng: &mut StdRng, kind: MetricKind, point_count: usize, sequence: u64) -> Metric {
    let name = format!("{}.{}", kind.as_str(), sequence);
    let metadata_count = rng.random_range(0..=2);
    let mut builder = Metric::build()
        .name(name)
        .description(format!("Random {} metric", kind.as_str()))
        .unit(pick(rng, METRIC_UNITS).to_string())
        .metadata(random_attributes(rng, metadata_count));

    match kind {
        MetricKind::Gauge => builder = builder.data_gauge(Gauge::new(random_number_points(
            rng, point_count, sequence,
        ))),
        MetricKind::Sum => builder = builder.data_sum(Sum::new(
            random_temporality(rng),
            rng.random_bool(0.7),
            random_number_points(rng, point_count, sequence),
        )),
        MetricKind::Histogram => builder = builder.data_histogram(Histogram::new(
            random_temporality(rng),
            random_histogram_points(rng, point_count, sequence),
        )),
        MetricKind::ExponentialHistogram => {
            builder = builder.data_exponential_histogram(ExponentialHistogram::new(
                random_temporality(rng),
                random_exponential_histogram_points(rng, point_count, sequence),
            ));
        }
        MetricKind::Summary => builder = builder.data_summary(Summary::new(
            random_summary_points(rng, point_count, sequence),
        )),
    }

    builder.finish()
}

fn random_number_points(rng: &mut StdRng, count: usize, sequence: u64) -> Vec<NumberDataPoint> {
    (0..count)
        .map(|offset| {
            let idx = sequence + offset as u64;
            let time = BASE_TIME_NS + idx.wrapping_mul(ONE_SEC_NS / 20);
            let attr_count = rng.random_range(1..=5);
            let mut builder = NumberDataPoint::build()
                .start_time_unix_nano(time.saturating_sub(rng.random_range(0..ONE_SEC_NS)))
                .time_unix_nano(time)
                .attributes(random_attributes(rng, attr_count));

            builder = if rng.random_bool(0.5) {
                builder.value_double(rng.random_range(-1_000.0..1_000.0))
            } else {
                builder.value_int(rng.random_range(-1_000_000..1_000_000))
            };

            builder.finish()
        })
        .collect()
}

fn random_histogram_points(
    rng: &mut StdRng,
    count: usize,
    sequence: u64,
) -> Vec<HistogramDataPoint> {
    (0..count)
        .map(|offset| {
            let idx = sequence + offset as u64;
            let time = BASE_TIME_NS + idx.wrapping_mul(ONE_SEC_NS / 20);
            let bucket_count = rng.random_range(2..=6);
            let bounds = (0..bucket_count - 1)
                .map(|i| (i as f64 + 1.0) * rng.random_range(1.0..25.0))
                .collect::<Vec<_>>();
            let bucket_counts = (0..bucket_count)
                .map(|_| rng.random_range(0..100))
                .collect::<Vec<_>>();

            let attr_count = rng.random_range(1..=5);
            HistogramDataPoint::build()
                .start_time_unix_nano(time.saturating_sub(rng.random_range(0..ONE_SEC_NS)))
                .time_unix_nano(time)
                .count(bucket_counts.iter().copied().sum::<u64>())
                .sum(rng.random_range(0.0..10_000.0))
                .bucket_counts(bucket_counts)
                .explicit_bounds(bounds)
                .min(rng.random_range(0.0..100.0))
                .max(rng.random_range(100.0..1_000.0))
                .attributes(random_attributes(rng, attr_count))
                .finish()
        })
        .collect()
}

fn random_exponential_histogram_points(
    rng: &mut StdRng,
    count: usize,
    sequence: u64,
) -> Vec<ExponentialHistogramDataPoint> {
    (0..count)
        .map(|offset| {
            let idx = sequence + offset as u64;
            let time = BASE_TIME_NS + idx.wrapping_mul(ONE_SEC_NS / 20);
            let positive_len = rng.random_range(1..=5);
            let negative_len = rng.random_range(0..=4);

            let attr_count = rng.random_range(1..=5);
            ExponentialHistogramDataPoint::build()
                .start_time_unix_nano(time.saturating_sub(rng.random_range(0..ONE_SEC_NS)))
                .time_unix_nano(time)
                .count(rng.random_range(1_u64..10_000))
                .sum(rng.random_range(0.0..10_000.0))
                .scale(rng.random_range(-5..=8))
                .zero_count(rng.random_range(0_u64..100))
                .zero_threshold(rng.random_range(0.0..5.0))
                .positive(Buckets::new(
                    rng.random_range(-3..=3),
                    (0..positive_len)
                        .map(|_| rng.random_range(0_u64..500))
                        .collect::<Vec<_>>(),
                ))
                .negative(Buckets::new(
                    rng.random_range(-3..=3),
                    (0..negative_len)
                        .map(|_| rng.random_range(0_u64..500))
                        .collect::<Vec<_>>(),
                ))
                .attributes(random_attributes(rng, attr_count))
                .finish()
        })
        .collect()
}

fn random_summary_points(rng: &mut StdRng, count: usize, sequence: u64) -> Vec<SummaryDataPoint> {
    (0..count)
        .map(|offset| {
            let idx = sequence + offset as u64;
            let time = BASE_TIME_NS + idx.wrapping_mul(ONE_SEC_NS / 20);
            let quantile_count = rng.random_range(2..=5);
            let quantile_values: Vec<ValueAtQuantile> = (0..quantile_count)
                .map(|q| ValueAtQuantile {
                    quantile: (q as f64 + 1.0) / (quantile_count as f64 + 1.0),
                    value: rng.random_range(0.0..1_000.0),
                })
                .collect();

            let attr_count = rng.random_range(1..=5);
            SummaryDataPoint::build()
                .start_time_unix_nano(time.saturating_sub(rng.random_range(0..ONE_SEC_NS)))
                .time_unix_nano(time)
                .count(rng.random_range(1_u64..10_000))
                .sum(rng.random_range(0.0..10_000.0))
                .quantile_values(quantile_values)
                .attributes(random_attributes(rng, attr_count))
                .finish()
        })
        .collect()
}

fn random_attributes(rng: &mut StdRng, count: usize) -> Vec<KeyValue> {
    (0..count)
        .map(|_| KeyValue::new(pick(rng, ATTR_KEYS).to_string(), random_any_value(rng)))
        .collect()
}

fn random_any_value(rng: &mut StdRng) -> AnyValue {
    match rng.random_range(0..4) {
        0 => AnyValue::new_string(format!("str-{}", rng.random_range(0..10_000))),
        1 => AnyValue::new_int(rng.random_range(-1_000..1_000)),
        2 => AnyValue::new_bool(rng.random_bool(0.5)),
        _ => AnyValue::new_double(rng.random_range(-100.0..100.0)),
    }
}

fn random_severity(rng: &mut StdRng) -> SeverityNumber {
    const LEVELS: [SeverityNumber; 4] = [
        SeverityNumber::Debug,
        SeverityNumber::Info,
        SeverityNumber::Warn,
        SeverityNumber::Error,
    ];
    *LEVELS.choose(rng).expect("non-empty severity levels")
}

fn random_span_kind(rng: &mut StdRng) -> SpanKind {
    const KINDS: [SpanKind; 5] = [
        SpanKind::Internal,
        SpanKind::Server,
        SpanKind::Client,
        SpanKind::Producer,
        SpanKind::Consumer,
    ];
    *KINDS.choose(rng).expect("non-empty span kinds")
}

fn random_status(rng: &mut StdRng) -> Status {
    if rng.random_bool(0.85) {
        Status::new(StatusCode::Ok, "ok")
    } else {
        Status::new(
            StatusCode::Error,
            format!("error-{}", rng.random_range(0..100)),
        )
    }
}

fn random_temporality(rng: &mut StdRng) -> AggregationTemporality {
    if rng.random_bool(0.5) {
        AggregationTemporality::Delta
    } else {
        AggregationTemporality::Cumulative
    }
}

fn random_trace_id(rng: &mut StdRng) -> Vec<u8> {
    u128::to_be_bytes(rng.random()).to_vec()
}

fn random_span_id(rng: &mut StdRng) -> Vec<u8> {
    u64::to_be_bytes(rng.random()).to_vec()
}

fn pick<'a>(rng: &mut StdRng, values: &'a [&str]) -> &'a str {
    values.choose(rng).copied().expect("non-empty slice")
}
