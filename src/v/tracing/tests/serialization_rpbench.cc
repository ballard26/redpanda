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
#include "tracing/serialization.h"
#include "tracing/tests/test_span.h"
#include "tracing/trace.h"
#include "tracing/types.h"

#include <seastar/testing/perf_tests.hh>

#include <vector>

using namespace tracing;

namespace {

span make_bench_span(scope_id scope = scope_id::kafka) {
    auto s = tracing::testing::make_test_span(scope);
    s.attributes.push_back(
      key_value{.key = static_str{"key"}, .value = ss::sstring("val")});
    return s;
}

chunked_vector<span> make_batch(int count) {
    chunked_vector<span> batch;
    for (int i = 0; i < count; ++i) {
        batch.push_back(make_bench_span(
          static_cast<scope_id>(i % static_cast<int>(scope_id::num_scopes))));
    }
    return batch;
}

} // namespace

struct tracing_serialization {};

PERF_TEST_C(tracing_serialization, serialize_resource_spans_1000) {
    auto batch = make_batch(1000);

    perf_tests::start_measuring_time();
    auto buf = co_await serialize_resource_spans(
      tracing::resource{}, std::move(batch));
    perf_tests::stop_measuring_time();
    perf_tests::do_not_optimize(buf);
}

PERF_TEST_C(tracing_serialization, serialize_resource_spans_4096) {
    auto batch = make_batch(4096);

    perf_tests::start_measuring_time();
    auto buf = co_await serialize_resource_spans(
      tracing::resource{}, std::move(batch));
    perf_tests::stop_measuring_time();
    perf_tests::do_not_optimize(buf);
}
