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

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "serde/protobuf/wire_format.h"
#include "tracing/serialization.h"
#include "tracing/tests/test_span.h"
#include "tracing/trace.h"
#include "tracing/types.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>

using namespace tracing;
namespace pb = serde::pb;

namespace {

// Simple protobuf field reader for test verification.
// Reads the next field from a parser and returns tag + raw value.
struct parsed_field {
    int32_t field_number;
    pb::wire_type type;

    // Populated based on wire type
    uint64_t varint_val = 0;
    uint64_t fixed64_val = 0;
    iobuf length_val;
};

parsed_field read_field(iobuf_parser& parser) {
    auto t = pb::tag::read(&parser);
    parsed_field f;
    f.field_number = t.field_number;
    f.type = t.wire_type;
    switch (t.wire_type) {
    case pb::wire_type::varint:
        f.varint_val = pb::read_varint<uint64_t>(&parser);
        break;
    case pb::wire_type::i64: {
        auto bytes = parser.peek_bytes(sizeof(uint64_t));
        std::memcpy(&f.fixed64_val, bytes.data(), sizeof(uint64_t));
        parser.skip(sizeof(uint64_t));
        break;
    }
    case pb::wire_type::length: {
        auto len = pb::read_length(&parser);
        f.length_val = parser.share(static_cast<size_t>(len));
        break;
    }
    default:
        pb::skip_unknown_field(&parser, t.wire_type);
        break;
    }
    return f;
}

ss::sstring iobuf_to_string(const iobuf& buf) {
    iobuf_const_parser parser(buf);
    return parser.read_string_unsafe(parser.bytes_left());
}

span make_test_span() { return tracing::testing::make_test_span(); }

} // namespace

TEST(Serialization, KeyValueString) {
    key_value kv{
      .key = static_str{"mykey"},
      .value = ss::sstring("myval"),
    };
    auto buf = tracing::testing::serialize_key_value(kv);
    iobuf_parser parser(std::move(buf));

    // field 1: key (string)
    auto f1 = read_field(parser);
    EXPECT_EQ(f1.field_number, 1);
    EXPECT_EQ(iobuf_to_string(f1.length_val), "mykey");

    // field 2: value (submessage AnyValue with string_value=1)
    auto f2 = read_field(parser);
    EXPECT_EQ(f2.field_number, 2);
    iobuf_parser val_parser(std::move(f2.length_val));
    auto sv = read_field(val_parser);
    EXPECT_EQ(sv.field_number, 1); // AnyValue.string_value
    EXPECT_EQ(iobuf_to_string(sv.length_val), "myval");
}

TEST(Serialization, KeyValueInt) {
    key_value kv{
      .key = static_str{"count"},
      .value = int64_t{42},
    };
    auto buf = tracing::testing::serialize_key_value(kv);
    iobuf_parser parser(std::move(buf));

    read_field(parser); // skip key
    auto f2 = read_field(parser);
    iobuf_parser val_parser(std::move(f2.length_val));
    auto iv = read_field(val_parser);
    EXPECT_EQ(iv.field_number, 3); // AnyValue.int_value
    EXPECT_EQ(iv.varint_val, 42);
}

TEST(Serialization, KeyValueBool) {
    key_value kv{
      .key = static_str{"flag"},
      .value = true,
    };
    auto buf = tracing::testing::serialize_key_value(kv);
    iobuf_parser parser(std::move(buf));

    read_field(parser); // skip key
    auto f2 = read_field(parser);
    iobuf_parser val_parser(std::move(f2.length_val));
    auto bv = read_field(val_parser);
    EXPECT_EQ(bv.field_number, 2); // AnyValue.bool_value
    EXPECT_EQ(bv.varint_val, 1);
}

TEST(Serialization, KeyValueDouble) {
    key_value kv{
      .key = static_str{"rate"},
      .value = 3.14,
    };
    auto buf = tracing::testing::serialize_key_value(kv);
    iobuf_parser parser(std::move(buf));

    read_field(parser); // skip key
    auto f2 = read_field(parser);
    iobuf_parser val_parser(std::move(f2.length_val));
    auto dv = read_field(val_parser);
    EXPECT_EQ(dv.field_number, 4); // AnyValue.double_value
    double actual;
    std::memcpy(&actual, &dv.fixed64_val, sizeof(double));
    EXPECT_DOUBLE_EQ(actual, 3.14);
}

TEST(Serialization, SpanBasicFields) {
    auto s = make_test_span();
    s.start_time_unix_nano = 1000000;
    s.end_time_unix_nano = 2000000;
    auto buf = tracing::testing::serialize_span(s);
    iobuf_parser parser(std::move(buf));

    // trace_id (field 1, bytes)
    auto f1 = read_field(parser);
    EXPECT_EQ(f1.field_number, 1);
    EXPECT_EQ(f1.length_val.size_bytes(), 16);

    // span_id (field 2, bytes)
    auto f2 = read_field(parser);
    EXPECT_EQ(f2.field_number, 2);
    EXPECT_EQ(f2.length_val.size_bytes(), 8);

    // parent_span_id (field 4, bytes)
    auto f4 = read_field(parser);
    EXPECT_EQ(f4.field_number, 4);
    EXPECT_EQ(f4.length_val.size_bytes(), 8);

    // name (field 5, string)
    auto f5 = read_field(parser);
    EXPECT_EQ(f5.field_number, 5);
    EXPECT_EQ(iobuf_to_string(f5.length_val), "test_span");

    // kind (field 6, enum)
    auto f6 = read_field(parser);
    EXPECT_EQ(f6.field_number, 6);
    EXPECT_EQ(f6.varint_val, static_cast<uint64_t>(span_kind::server));

    // start_time_unix_nano (field 7, fixed64)
    auto f7 = read_field(parser);
    EXPECT_EQ(f7.field_number, 7);
    EXPECT_EQ(f7.fixed64_val, 1000000);

    // end_time_unix_nano (field 8, fixed64)
    auto f8 = read_field(parser);
    EXPECT_EQ(f8.field_number, 8);
    EXPECT_EQ(f8.fixed64_val, 2000000);
}

TEST(Serialization, SpanWithAttributes) {
    auto s = make_test_span();
    s.attributes.push_back(
      key_value{.key = static_str{"k"}, .value = int64_t{1}});
    auto buf = tracing::testing::serialize_span(s);
    iobuf_parser parser(std::move(buf));

    // Skip to field 9 (attributes)
    while (parser.bytes_left() > 0) {
        auto f = read_field(parser);
        if (f.field_number == 9) {
            // Found attribute submessage — parse the KeyValue
            iobuf_parser kv_parser(std::move(f.length_val));
            auto key_field = read_field(kv_parser);
            EXPECT_EQ(iobuf_to_string(key_field.length_val), "k");
            return;
        }
    }
    FAIL() << "attribute field 9 not found";
}

TEST(Serialization, EmptySpanOmitsScalarZeroFields) {
    span s;
    s.name = "minimal";
    auto buf = tracing::testing::serialize_span(s);
    iobuf_parser parser(std::move(buf));

    // All byte-array IDs default to zeros. Proto3 treats all-zero bytes
    // as unset — omitted from the wire. Only name(5) is present.
    std::vector<int32_t> fields;
    while (parser.bytes_left() > 0) {
        auto f = read_field(parser);
        fields.push_back(f.field_number);
    }
    ASSERT_EQ(fields.size(), 1);
    EXPECT_EQ(fields[0], 5); // name
}

TEST(Serialization, RootSpanOmitsParentSpanId) {
    // A root span has a non-zero trace_id and span_id but no parent.
    // The all-zero parent_span_id must be omitted from the wire so
    // OTLP receivers recognize the span as a root.
    span s;
    s.trace_id = generate_trace_id();
    s.span_id = generate_span_id();
    s.name = "root";
    auto buf = tracing::testing::serialize_span(s);
    iobuf_parser parser(std::move(buf));

    std::vector<int32_t> fields;
    while (parser.bytes_left() > 0) {
        auto f = read_field(parser);
        fields.push_back(f.field_number);
    }
    EXPECT_NE(std::find(fields.begin(), fields.end(), 1), fields.end());
    EXPECT_NE(std::find(fields.begin(), fields.end(), 2), fields.end());
    EXPECT_EQ(std::find(fields.begin(), fields.end(), 4), fields.end());
    EXPECT_NE(std::find(fields.begin(), fields.end(), 5), fields.end());
}

TEST(Serialization, ResourceSpansGroupsByScope) {
    chunked_vector<span> spans;

    span s1;
    s1.trace_id = generate_trace_id();
    s1.span_id = generate_span_id();
    s1.name = "kafka_span";
    s1.scope = scope_id::kafka;
    s1.start_time_unix_nano = 100;
    spans.push_back(std::move(s1));

    span s2;
    s2.trace_id = generate_trace_id();
    s2.span_id = generate_span_id();
    s2.name = "raft_span";
    s2.scope = scope_id::raft;
    s2.start_time_unix_nano = 200;
    spans.push_back(std::move(s2));

    span s3;
    s3.trace_id = generate_trace_id();
    s3.span_id = generate_span_id();
    s3.name = "kafka_span2";
    s3.scope = scope_id::kafka;
    s3.start_time_unix_nano = 300;
    spans.push_back(std::move(s3));

    auto buf = serialize_resource_spans(resource{}, std::move(spans)).get();
    iobuf_parser parser(std::move(buf));

    // ResourceSpans: resource (field 1), scope_spans (field 2, repeated)
    int scope_spans_count = 0;
    while (parser.bytes_left() > 0) {
        auto f = read_field(parser);
        if (f.field_number == 2) {
            ++scope_spans_count;
        }
    }
    EXPECT_EQ(scope_spans_count, 2);
}

TEST(Serialization, WrapExportRequestRoundTrip) {
    chunked_vector<span> spans;
    spans.push_back(make_test_span());
    resource res;
    res.attributes.push_back(
      key_value{.key = static_str{"node"}, .value = ss::sstring("1")});

    auto rs_buf = serialize_resource_spans(res, std::move(spans)).get();
    EXPECT_GT(rs_buf.size_bytes(), 0);

    std::vector<iobuf> per_shard;
    per_shard.push_back(std::move(rs_buf));
    auto buf = wrap_export_request(std::move(per_shard));
    EXPECT_GT(buf.size_bytes(), 0);

    // Parse outer: ExportTraceServiceRequest.resource_spans (field 1)
    iobuf_parser parser(std::move(buf));
    auto rs_field = read_field(parser);
    EXPECT_EQ(rs_field.field_number, 1);
    EXPECT_EQ(parser.bytes_left(), 0);

    // Parse ResourceSpans: resource (field 1), scope_spans (field 2)
    iobuf_parser rs_parser(std::move(rs_field.length_val));
    auto res_field = read_field(rs_parser);
    EXPECT_EQ(res_field.field_number, 1); // resource

    auto ss_field = read_field(rs_parser);
    EXPECT_EQ(ss_field.field_number, 2); // scope_spans
}

TEST(Serialization, EmptySpanListProducesEmptyOutput) {
    chunked_vector<span> empty;
    auto buf = serialize_resource_spans(resource{}, std::move(empty)).get();
    EXPECT_EQ(buf.size_bytes(), 0);
}
