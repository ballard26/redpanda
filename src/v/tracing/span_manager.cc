/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "tracing/span_manager.h"

#include "base/seastarx.h"
#include "base/vassert.h"
#include "base/vlog.h"
#include "bytes/iobuf.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "container/chunked_circular_buffer.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"
#include "random/generators.h"
#include "ssx/future-util.h"
#include "tracing/logger.h"
#include "tracing/serialization.h"
#include "tracing/span_exporter.h"
#include "tracing/trace.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/timer.hh>

#include <algorithm>
#include <exception>

namespace tracing {

// Defined in trace.cc
void set_local_span_manager(span_manager* c) noexcept;

namespace {

constexpr size_t scope_index(scope_id s) noexcept {
    return std::min(
      static_cast<size_t>(s), static_cast<size_t>(scope_id::num_scopes) - 1);
}

using name_key = std::pair<scope_id, ss::sstring>;
using name_key_view = std::pair<scope_id, std::string_view>;

// Transparent hash: always hashes as (scope_id, string_view) so stored
// keys and lookup keys produce the same hash.
struct name_key_hash {
    using is_transparent = void;
    using is_avalanching = void;

    auto operator()(const name_key& k) const noexcept -> uint64_t {
        return absl::Hash<name_key_view>{}({k.first, k.second});
    }

    auto operator()(name_key_view k) const noexcept -> uint64_t {
        return absl::Hash<name_key_view>{}(k);
    }
};

// Transparent equality: compares scope_id directly, string via string_view.
struct name_key_eq {
    using is_transparent = void;

    bool operator()(const name_key& a, const name_key& b) const noexcept {
        return a == b;
    }

    bool operator()(const name_key& a, name_key_view b) const noexcept {
        return a.first == b.first
               && std::string_view{a.second} == b.second;
    }

    bool operator()(name_key_view a, const name_key& b) const noexcept {
        return a.first == b.first
               && a.second == std::string_view{b.second};
    }
};

class sampler_state {
public:
    void rebuild(const sampling_config& cfg) noexcept {
        _default_rate = cfg.default_rate;

        _scope_rates.fill(std::nullopt);
        _name_rates.clear();

        for (const auto& ovr : cfg.overrides) {
            auto rate = ovr.rate.value_or(cfg.default_rate);
            if (ovr.name.empty()) {
                auto idx = scope_index(ovr.scope);
                if (idx < _scope_rates.size()) {
                    _scope_rates[idx] = rate;
                }
            } else {
                _name_rates[name_key{ovr.scope, ovr.name}] = rate;
            }
        }
    }

    bool should_sample(scope_id scope, std::string_view name) noexcept {
        auto rate = resolve_rate(scope, name);
        if (rate <= 0.0) {
            return false;
        }
        if (rate >= 1.0) {
            return true;
        }
        return _rng.get_real<double>() < rate;
    }

private:
    double resolve_rate(scope_id scope, std::string_view name) noexcept {
        if (!_name_rates.empty()) {
            auto it = _name_rates.find(name_key_view{scope, name});
            if (it != _name_rates.end()) {
                return it->second;
            }
        }
        auto idx = scope_index(scope);
        if (_scope_rates[idx]) {
            return *_scope_rates[idx];
        }
        return _default_rate;
    }

    random_generators::rng _rng;
    double _default_rate = 0.0;
    std::array<std::optional<double>, static_cast<size_t>(scope_id::num_scopes)>
      _scope_rates;
    chunked_hash_map<name_key, double, name_key_hash, name_key_eq> _name_rates;
};

} // namespace

struct span_manager::impl {
    impl() = default;

    // Shard 0 only — created in constructor, used by flush()
    std::unique_ptr<span_exporter> exporter;
    config cfg;
    resource resource;
    sampler_state sampler;
    chunked_circular_buffer<span> buffer;
    ss::gate gate;
    ss::abort_source as;
    ss::timer<> flush_timer;

    uint32_t active_spans = 0;

    uint64_t spans_recorded = 0;
    uint64_t spans_dropped_buffer = 0;
    uint64_t spans_flushed = 0;
    uint64_t flush_errors = 0;
    uint64_t spans_limited_depth = 0;
    uint64_t spans_limited_children = 0;
    uint64_t spans_limited_active = 0;
    uint64_t spans_sampled_out = 0;
    uint64_t attributes_dropped = 0;
    uint64_t events_dropped = 0;

    metrics::internal_metric_groups internal_metrics;
    metrics::public_metric_groups public_metrics;

    void setup_metrics();
    void setup_resource_attrs();
};

void span_manager::impl::setup_metrics() {
    namespace sm = ss::metrics;

    auto group_name = prometheus_sanitize::metrics_name("tracing");

    auto metric_defs = std::vector<ss::metrics::metric_definition>{};
    metric_defs.reserve(12);

    metric_defs.emplace_back(
      sm::make_counter(
        "spans_recorded_total",
        [this] { return spans_recorded; },
        sm::description("Total spans written to the flush buffer"))
        .aggregate({sm::shard_label}));
    metric_defs.emplace_back(
      sm::make_counter(
        "spans_dropped_total",
        [this] { return spans_dropped_buffer; },
        sm::description(
          "Total spans dropped because the flush buffer was full"))
        .aggregate({sm::shard_label}));
    metric_defs.emplace_back(
      sm::make_counter(
        "spans_flushed_total",
        [this] { return spans_flushed; },
        sm::description("Total spans drained from the buffer by flush()"))
        .aggregate({sm::shard_label}));
    metric_defs.emplace_back(
      sm::make_counter(
        "spans_sampled_out_total",
        [this] { return spans_sampled_out; },
        sm::description("Total root spans rejected by the sampler"))
        .aggregate({sm::shard_label}));
    metric_defs.emplace_back(
      sm::make_counter(
        "spans_limited_depth_total",
        [this] { return spans_limited_depth; },
        sm::description(
          "Total spans rejected because they exceeded max_span_depth"))
        .aggregate({sm::shard_label}));
    metric_defs.emplace_back(
      sm::make_counter(
        "spans_limited_children_total",
        [this] { return spans_limited_children; },
        sm::description(
          "Total spans rejected because they exceeded "
          "max_children_per_span"))
        .aggregate({sm::shard_label}));
    metric_defs.emplace_back(
      sm::make_counter(
        "spans_limited_active_total",
        [this] { return spans_limited_active; },
        sm::description(
          "Total spans rejected because active spans reached "
          "max_active_spans_per_shard"))
        .aggregate({sm::shard_label}));
    metric_defs.emplace_back(
      sm::make_counter(
        "attributes_dropped_total",
        [this] { return attributes_dropped; },
        sm::description(
          "Total span attributes dropped for exceeding "
          "max_attributes_per_span"))
        .aggregate({sm::shard_label}));
    metric_defs.emplace_back(
      sm::make_counter(
        "events_dropped_total",
        [this] { return events_dropped; },
        sm::description(
          "Total span events dropped for exceeding max_events_per_span"))
        .aggregate({sm::shard_label}));
    metric_defs.emplace_back(
      sm::make_counter(
        "flush_errors_total",
        [this] { return flush_errors; },
        sm::description(
          "Total flush failures (e.g., OTLP endpoint unreachable)"))
        .aggregate({sm::shard_label}));
    metric_defs.emplace_back(
      sm::make_gauge(
        "active_spans",
        [this] { return active_spans; },
        sm::description("Currently in-flight spans on this shard"))
        .aggregate({sm::shard_label}));
    metric_defs.emplace_back(
      sm::make_gauge(
        "buffer_utilization",
        [this] {
            return cfg.buffer_size > 0
                     ? static_cast<double>(buffer.size())
                         / static_cast<double>(cfg.buffer_size)
                     : 0.0;
        },
        sm::description("Ratio of buffered spans to configured buffer size"))
        .aggregate({sm::shard_label}));

    if (!::config::shard_local_cfg().disable_metrics()) {
        internal_metrics.add_group(group_name, metric_defs);
    }
    if (!::config::shard_local_cfg().disable_public_metrics()) {
        public_metrics.add_group(group_name, metric_defs);
    }
}

void span_manager::impl::setup_resource_attrs() {
    // OTel requires service.name on every resource. Without it,
    // backends (e.g., Tempo) fail to attribute spans to a service and
    // display "<root span not yet received>" in place of the service.
    resource.attributes.push_back(
      key_value{
        .key = static_str{"service.name"},
        .value = ss::sstring("redpanda"),
      });
    // service.instance.id distinguishes spans per node in a cluster.
    // Resolved from node_config; empty on first boot before node_id
    // assignment — spans exported before assignment lack the attribute,
    // which is acceptable.
    if (auto node_id = ::config::node().node_id(); node_id.has_value()) {
        resource.attributes.push_back(
          key_value{
            .key = static_str{"service.instance.id"},
            .value = ss::sstring(fmt::to_string(node_id->operator()())),
          });
    }
    // service.namespace groups nodes sharing a cluster_id.
    if (auto cluster_id = ::config::shard_local_cfg().cluster_id();
        cluster_id.has_value() && !cluster_id->empty()) {
        resource.attributes.push_back(
          key_value{
            .key = static_str{"service.namespace"},
            .value = ss::sstring{*cluster_id},
          });
    }
    // thread.id = shard_id. Each shard's ResourceSpans batch is sent
    // separately, so per-shard resources correctly partition spans.
    resource.attributes.push_back(
      key_value{
        .key = static_str{"thread.id"},
        .value = static_cast<int64_t>(ss::this_shard_id()),
      });
}

span_manager::span_manager()
  : _impl(std::make_unique<impl>()) {
    set_local_span_manager(this);
    _impl->setup_resource_attrs();
    if (ss::this_shard_id() == 0) {
        _impl->exporter = std::make_unique<noop_span_exporter>();
        _impl->flush_timer.set_callback([this] {
            ssx::spawn_with_gate(_impl->gate, [this] { return flush(); });
        });
    }
    _impl->setup_metrics();
}

span_manager::~span_manager() = default;

ss::future<>
span_manager::set_exporter(std::unique_ptr<span_exporter> exporter) {
    vassert(
      ss::this_shard_id() == 0,
      "set_exporter must be called on shard 0, got {}",
      ss::this_shard_id());
    if (_impl->exporter) {
        co_await _impl->exporter->stop();
    }
    _impl->exporter = std::move(exporter);
}

ss::future<> span_manager::stop() {
    set_local_span_manager(nullptr);
    _impl->cfg.enabled = false;
    _impl->as.request_abort();
    _impl->flush_timer.cancel();
    co_await _impl->gate.close();
    if (ss::this_shard_id() == 0) {
        co_await flush();
        if (_impl->exporter) {
            co_await _impl->exporter->stop();
        }
    }
}

void span_manager::update_config(config cfg) noexcept {
    _impl->cfg = std::move(cfg);
    _impl->sampler.rebuild(_impl->cfg.sampling);

    if (ss::this_shard_id() != 0) {
        return;
    }

    _impl->flush_timer.cancel();
    if (_impl->cfg.enabled && _impl->cfg.flush_interval.count() > 0) {
        _impl->flush_timer.arm_periodic(_impl->cfg.flush_interval);
    }
}

void span_manager::record_span(span&& s) noexcept {
    if (_impl->active_spans > 0) {
        --_impl->active_spans;
    }
    if (_impl->gate.is_closed()) {
        return;
    }
    if (_impl->buffer.size() >= _impl->cfg.buffer_size) {
        _impl->buffer.pop_front();
        ++_impl->spans_dropped_buffer;
    }
    _impl->buffer.push_back(std::move(s));
    ++_impl->spans_recorded;
}

bool span_manager::try_start_root_span(
  scope_id scope, static_str name) noexcept {
    if (!_impl->cfg.enabled) [[likely]] {
        return false;
    }
    if (!_impl->sampler.should_sample(scope, name)) {
        ++_impl->spans_sampled_out;
        return false;
    }
    return try_start_span(0, 0);
}

bool span_manager::try_start_span(
  uint16_t depth, uint16_t sibling_count) noexcept {
    if (!_impl->cfg.enabled) [[likely]] {
        return false;
    }
    if (_impl->active_spans >= _impl->cfg.limits.max_active_spans_per_shard) {
        ++_impl->spans_limited_active;
        return false;
    }
    if (depth >= _impl->cfg.limits.max_span_depth) {
        ++_impl->spans_limited_depth;
        return false;
    }
    if (sibling_count >= _impl->cfg.limits.max_children_per_span) {
        ++_impl->spans_limited_children;
        return false;
    }
    ++_impl->active_spans;
    return true;
}

void span_manager::on_attribute_dropped() noexcept {
    ++_impl->attributes_dropped;
}

void span_manager::on_event_dropped() noexcept { ++_impl->events_dropped; }

const span_limits& span_manager::limits() const noexcept {
    return _impl->cfg.limits;
}

const span_manager::config& span_manager::current_config() const noexcept {
    return _impl->cfg;
}

span_manager::status span_manager::get_status() const noexcept {
    return {
      .active_spans = _impl->active_spans,
      .spans_recorded = _impl->spans_recorded,
      .spans_dropped = _impl->spans_dropped_buffer,
      .spans_flushed = _impl->spans_flushed,
      .spans_limited_depth = _impl->spans_limited_depth,
      .spans_limited_children = _impl->spans_limited_children,
      .spans_limited_active = _impl->spans_limited_active,
      .spans_sampled_out = _impl->spans_sampled_out,
      .attributes_dropped = _impl->attributes_dropped,
      .events_dropped = _impl->events_dropped,
      .flush_errors = _impl->flush_errors,
    };
}

ss::future<iobuf> span_manager::serialize_and_drain() {
    chunked_vector<span> batch;
    batch.reserve(_impl->buffer.size());
    while (!_impl->buffer.empty()) {
        batch.push_back(std::move(_impl->buffer.front()));
        _impl->buffer.pop_front();
    }
    _impl->spans_flushed += batch.size();
    if (batch.empty()) {
        co_return iobuf{};
    }
    co_return co_await serialize_resource_spans(
      _impl->resource, std::move(batch));
}

ss::future<> span_manager::flush() {
    if (!_impl->exporter) {
        co_return;
    }

    auto per_shard = co_await container().map(
      [](span_manager& sm) { return sm.serialize_and_drain(); });

    bool empty = !std::any_of(
      per_shard.begin(), per_shard.end(), [](const auto& buf) {
          return !buf.empty();
      });
    if (empty) {
        co_return;
    }

    try {
        co_await _impl->exporter->export_spans(
          wrap_export_request(std::move(per_shard)));
    } catch (...) {
        ++_impl->flush_errors;
        vlog(
          tracing_log.info, "Span export failed: {}", std::current_exception());
    }
}

} // namespace tracing
