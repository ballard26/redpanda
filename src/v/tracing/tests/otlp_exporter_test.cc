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

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "http/tests/http_imposter.h"
#include "test_utils/boost_fixture.h"
#include "tracing/otlp_exporter.h"
#include "tracing/serialization.h"
#include "tracing/tests/test_span.h"
#include "tracing/trace.h"

#include <seastar/core/sleep.hh>

#include <boost/test/unit_test.hpp>
#include <opentelemetry/proto/collector/trace/v1/trace_service.pb.h>

using namespace tracing;

namespace {

constexpr uint16_t test_port = 44201;

iobuf make_test_request() {
    chunked_vector<span> spans;
    auto s = tracing::testing::make_test_span(scope_id::kafka);
    s.attributes.push_back(
      key_value{.key = static_str{"test.key"}, .value = ss::sstring("val")});
    spans.push_back(std::move(s));

    resource res;
    res.attributes.push_back(
      key_value{
        .key = static_str{"service.name"}, .value = ss::sstring("redpanda")});

    auto rs_buf = serialize_resource_spans(res, std::move(spans)).get();
    std::vector<iobuf> per_shard;
    per_shard.push_back(std::move(rs_buf));
    return wrap_export_request(std::move(per_shard));
}

} // namespace

class otlp_fixture : public http_imposter_fixture {
public:
    otlp_fixture()
      : http_imposter_fixture(test_port) {}
};

FIXTURE_TEST(test_export_sends_valid_protobuf, otlp_fixture) {
    when()
      .request("/v1/traces")
      .with_method(ss::httpd::POST)
      .then_reply_with(ss::http::reply::status_type::ok);
    listen();

    exporter_config cfg{
      .endpoint = {ss::sstring(httpd_host_name), httpd_port_number()},
      .path = "/v1/traces",
    };
    otlp_exporter exporter(cfg);

    auto request = make_test_request();
    exporter.export_spans(std::move(request)).get();
    exporter.stop().get();

    BOOST_REQUIRE(has_call("/v1/traces"));

    auto req = get_latest_request("/v1/traces");
    BOOST_REQUIRE(req.has_value());
    const auto& info = req->get();

    // Verify Content-Type header
    auto ct = info.header("Content-Type");
    BOOST_REQUIRE(ct.has_value());
    BOOST_REQUIRE_EQUAL(*ct, "application/x-protobuf");

    // Deserialize with generated OTel proto types
    opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest
      parsed;
    BOOST_REQUIRE(parsed.ParseFromString(std::string(info.content)));

    // Verify structure: 1 ResourceSpans with resource + scope_spans
    BOOST_REQUIRE_EQUAL(parsed.resource_spans_size(), 1);
    const auto& rs = parsed.resource_spans(0);

    // Resource has our "service.name" attribute
    BOOST_REQUIRE_GT(rs.resource().attributes_size(), 0);
    BOOST_REQUIRE_EQUAL(rs.resource().attributes(0).key(), "service.name");
    BOOST_REQUIRE_EQUAL(
      rs.resource().attributes(0).value().string_value(), "redpanda");

    // At least one ScopeSpans with one span
    BOOST_REQUIRE_GT(rs.scope_spans_size(), 0);
    const auto& ss = rs.scope_spans(0);
    BOOST_REQUIRE_GT(ss.spans_size(), 0);

    const auto& span = ss.spans(0);
    BOOST_REQUIRE_EQUAL(span.name(), "test_span");
    BOOST_REQUIRE_EQUAL(span.trace_id().size(), 16);
    BOOST_REQUIRE_EQUAL(span.span_id().size(), 8);

    // Verify the attribute round-trips
    BOOST_REQUIRE_GT(span.attributes_size(), 0);
    BOOST_REQUIRE_EQUAL(span.attributes(0).key(), "test.key");
    BOOST_REQUIRE_EQUAL(span.attributes(0).value().string_value(), "val");
}

FIXTURE_TEST(test_export_sends_auth_header, otlp_fixture) {
    when()
      .request("/v1/traces")
      .with_method(ss::httpd::POST)
      .then_reply_with(ss::http::reply::status_type::ok);
    listen();

    exporter_config cfg{
      .endpoint = {ss::sstring(httpd_host_name), httpd_port_number()},
      .path = "/v1/traces",
      .auth_header = "Bearer test-token-123",
    };
    otlp_exporter exporter(cfg);

    auto request = make_test_request();
    exporter.export_spans(std::move(request)).get();
    exporter.stop().get();

    auto req = get_latest_request("/v1/traces");
    BOOST_REQUIRE(req.has_value());
    auto auth = req->get().header("Authorization");
    BOOST_REQUIRE(auth.has_value());
    BOOST_REQUIRE_EQUAL(*auth, "Bearer test-token-123");
}

FIXTURE_TEST(test_export_no_auth_header_when_empty, otlp_fixture) {
    when()
      .request("/v1/traces")
      .with_method(ss::httpd::POST)
      .then_reply_with(ss::http::reply::status_type::ok);
    listen();

    exporter_config cfg{
      .endpoint = {ss::sstring(httpd_host_name), httpd_port_number()},
      .path = "/v1/traces",
    };
    otlp_exporter exporter(cfg);

    auto request = make_test_request();
    exporter.export_spans(std::move(request)).get();
    exporter.stop().get();

    auto req = get_latest_request("/v1/traces");
    BOOST_REQUIRE(req.has_value());
    auto auth = req->get().header("Authorization");
    BOOST_REQUIRE(!auth.has_value());
}

FIXTURE_TEST(test_export_connection_failure_throws, otlp_fixture) {
    // Point at a port with nothing listening (don't call listen())
    exporter_config cfg{
      .endpoint = {ss::sstring(httpd_host_name), uint16_t(test_port + 1)},
      .path = "/v1/traces",
      .timeout = std::chrono::milliseconds(500),
    };
    otlp_exporter exporter(cfg);

    auto request = make_test_request();
    BOOST_REQUIRE_THROW(
      exporter.export_spans(std::move(request)).get(), std::exception);
    exporter.stop().get();
}

FIXTURE_TEST(test_export_non_ok_status_throws, otlp_fixture) {
    when()
      .request("/v1/traces")
      .with_method(ss::httpd::POST)
      .then_reply_with(ss::http::reply::status_type::service_unavailable);
    listen();

    exporter_config cfg{
      .endpoint = {ss::sstring(httpd_host_name), httpd_port_number()},
      .path = "/v1/traces",
    };
    otlp_exporter exporter(cfg);

    auto request = make_test_request();
    BOOST_REQUIRE_THROW(
      exporter.export_spans(std::move(request)).get(), std::exception);
    exporter.stop().get();
}
