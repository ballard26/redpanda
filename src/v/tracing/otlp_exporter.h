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
#include "tracing/span_exporter.h"
#include "utils/unresolved_address.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sstring.hh>

#include <chrono>
#include <memory>

namespace http {
class client;
}

namespace tracing {

inline constexpr uint16_t default_otlp_port = 4318;
inline constexpr std::string_view default_otlp_path = "/v1/traces";

struct exporter_config {
    net::unresolved_address endpoint{"localhost", default_otlp_port};
    ss::sstring path{default_otlp_path};
    ss::sstring auth_header;
    std::chrono::milliseconds timeout{2000};
};

/// Pushes serialized ExportTraceServiceRequest to an OTLP/HTTP
/// endpoint via POST. Runs on shard 0 only. Keeps a persistent
/// connection to the endpoint, reconnecting as needed.
class otlp_exporter : public span_exporter {
public:
    explicit otlp_exporter(exporter_config cfg);
    ~otlp_exporter() override;

    ss::future<> export_spans(iobuf request) override;

    ss::future<> stop() override;

private:
    ss::future<> ensure_connected();

    exporter_config _cfg;
    ss::sstring _host_header;
    ss::abort_source _as;
    std::unique_ptr<http::client> _client;
};

} // namespace tracing
