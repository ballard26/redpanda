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
#include "tracing/types.h"

#include <seastar/core/future.hh>

#include <vector>

class iobuf;

namespace tracing {

/// Resource metadata attached to all spans from this node.
struct resource {
    chunked_vector<key_value> attributes;
};

/// Serialize a single shard's spans into a ResourceSpans protobuf
/// submessage. Groups spans by scope_id internally. Yields
/// periodically to avoid reactor stalls on large batches.
ss::future<iobuf>
serialize_resource_spans(const resource& res, chunked_vector<span> spans);

/// Wrap pre-serialized ResourceSpans iobufs into an
/// ExportTraceServiceRequest envelope.
iobuf wrap_export_request(std::vector<iobuf> per_shard_resource_spans);

namespace testing {

iobuf serialize_span(const span& s);
iobuf serialize_key_value(const key_value& kv);
iobuf serialize_span_event(const span_event& ev);
iobuf serialize_span_link(const span_link& link);

} // namespace testing

} // namespace tracing
