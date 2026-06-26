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
