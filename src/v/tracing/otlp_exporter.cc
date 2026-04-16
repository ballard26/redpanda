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

#include "tracing/otlp_exporter.h"

#include "http/client.h"
#include "tracing/logger.h"
#include "utils/prefix_logger.h"

namespace tracing {

otlp_exporter::otlp_exporter(exporter_config cfg)
  : _cfg(std::move(cfg))
  , _host_header(
      fmt::format("{}:{}", _cfg.endpoint.host(), _cfg.endpoint.port())) {
    net::base_transport::configuration client_cfg;
    client_cfg.server_addr = _cfg.endpoint;
    client_cfg.disable_metrics = net::metrics_disabled::yes;
    _client = std::make_unique<http::client>(client_cfg, _as);
}

otlp_exporter::~otlp_exporter() = default;

ss::future<> otlp_exporter::ensure_connected() {
    prefix_logger ctxlog(tracing_log, "otlp");
    auto res = co_await _client->get_connected(_cfg.timeout, ctxlog);
    if (res != http::reconnect_result_t::connected) {
        throw std::runtime_error("OTLP export: connection timeout");
    }
}

ss::future<> otlp_exporter::export_spans(iobuf request) {
    if (request.empty()) {
        co_return;
    }

    co_await ensure_connected();

    http::client::request_header header;
    header.method(boost::beast::http::verb::post);
    header.target(std::string(_cfg.path));
    header.insert(
      boost::beast::http::field::host, std::string_view(_host_header));
    header.insert(
      boost::beast::http::field::content_type, "application/x-protobuf");
    header.insert(
      boost::beast::http::field::content_length,
      fmt::format("{}", request.size_bytes()));
    if (!_cfg.auth_header.empty()) {
        header.insert(
          boost::beast::http::field::authorization,
          std::string(_cfg.auth_header));
    }

    auto resp = co_await _client->request_and_collect_response(
      std::move(header), std::move(request), _cfg.timeout);
    if (resp.status != boost::beast::http::status::ok) {
        throw std::runtime_error(
          fmt::format("OTLP export: HTTP {}", static_cast<int>(resp.status)));
    }
}

ss::future<> otlp_exporter::stop() {
    _as.request_abort();
    co_await _client->stop();
}

} // namespace tracing
