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
#include "bytes/iobuf.h"

#include <seastar/core/future.hh>

namespace tracing {

class span_exporter {
public:
    virtual ~span_exporter() = default;

    /// Serialized OTLP ExportTraceServiceRequest protobuf.
    virtual ss::future<> export_spans(iobuf request) = 0;

    virtual ss::future<> stop() { return ss::make_ready_future<>(); }
};

/// Drops all spans. Used before the real OTLP exporter is configured.
class noop_span_exporter : public span_exporter {
public:
    ss::future<> export_spans(iobuf) override {
        return ss::make_ready_future<>();
    }
};

} // namespace tracing
