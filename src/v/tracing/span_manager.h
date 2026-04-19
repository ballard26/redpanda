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

#pragma once

#include "base/seastarx.h"
#include "container/chunked_vector.h"
#include "tracing/scope.h"
#include "tracing/span_exporter.h"
#include "tracing/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>

#include <chrono>
#include <memory>
#include <optional>

namespace tracing {

class span_exporter;

struct span_limits {
    uint32_t max_active_spans_per_shard = 5000;
    uint32_t max_span_depth = 64;
    uint32_t max_children_per_span = 256;
    uint32_t max_attributes_per_span = 128;
    uint32_t max_events_per_span = 128;
    uint32_t max_links_per_span = 128;
};

struct sampling_override {
    scope_id scope;
    ss::sstring name; // empty = scope-level override
    std::optional<double> rate;
};

struct sampling_config {
    double default_rate = 0.0;
    chunked_vector<sampling_override> overrides;
};

/// Sharded service that manages tracing: sampling decisions, span
/// collection, and periodic flush to the exporter.
class span_manager : public ss::peering_sharded_service<span_manager> {
public:
    struct config {
        bool enabled = false;
        sampling_config sampling;
        size_t buffer_size = 4096;
        std::chrono::milliseconds flush_interval{5000};
        span_limits limits;
    };

    span_manager();
    ~span_manager();

    span_manager(span_manager&&) = delete;
    span_manager& operator=(span_manager&&) = delete;
    span_manager(const span_manager&) = delete;
    span_manager& operator=(const span_manager&) = delete;

    ss::future<> stop();

    /// Replace the span exporter (shard 0 only). Stops the previous
    /// exporter before installing the new one.
    ss::future<> set_exporter(std::unique_ptr<span_exporter> exporter);

    void update_config(config cfg) noexcept;

    /// Record a completed span into the ring buffer. Drops oldest
    /// when full. Never blocks or allocates on the request path.
    void record_span(span&& s) noexcept;

    /// Sampling + limit check for root spans. Returns true if the
    /// request should be traced (sampler says yes AND limits allow).
    bool try_start_root_span(scope_id scope, static_str name) noexcept;

    /// Limit check for child spans. Returns true if depth and
    /// sibling count are within limits.
    bool try_start_span(uint16_t depth, uint16_t sibling_count) noexcept;

    void on_attribute_dropped() noexcept;
    void on_event_dropped() noexcept;

    const span_limits& limits() const noexcept;
    const config& current_config() const noexcept;

    struct status {
        uint32_t active_spans = 0;
        uint64_t spans_recorded = 0;
        uint64_t spans_dropped = 0;
        uint64_t spans_flushed = 0;
        uint64_t spans_limited_depth = 0;
        uint64_t spans_limited_children = 0;
        uint64_t spans_limited_active = 0;
        uint64_t spans_sampled_out = 0;
        uint64_t attributes_dropped = 0;
        uint64_t events_dropped = 0;
        uint64_t flush_errors = 0;
    };
    status get_status() const noexcept;

    /// Collect spans from all shards and push to the exporter.
    /// Only meaningful on shard 0 (uses container().map()).
    ss::future<> flush();

private:
    ss::future<iobuf> serialize_and_drain();

    struct impl;
    std::unique_ptr<impl> _impl;
};

} // namespace tracing
