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

#include "tracing/trace.h"
#include "tracing/types.h"

namespace tracing::testing {

inline span make_test_span(scope_id scope = scope_id::kafka) {
    span s;
    s.trace_id = generate_trace_id();
    s.span_id = generate_span_id();
    s.parent_span_id = generate_span_id();
    s.name = "test_span";
    s.kind = span_kind::server;
    s.scope = scope;
    s.start_time_unix_nano = trace_now_ns();
    s.end_time_unix_nano = s.start_time_unix_nano + 1000;
    return s;
}

} // namespace tracing::testing
