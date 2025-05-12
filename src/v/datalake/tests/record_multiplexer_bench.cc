/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "base/vlog.h"
#include "cloud_io/provider.h"
#include "container/fragmented_vector.h"
#include "datalake/catalog_schema_manager.h"
#include "datalake/location.h"
#include "datalake/record_multiplexer.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/record_translator.h"
#include "datalake/tests/catalog_and_registry_fixture.h"
#include "datalake/tests/record_generator.h"
#include "datalake/tests/test_data_writer.h"
#include "datalake/tests/test_utils.h"
#include "model/compression.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "serde/avro/tests/data_generator.h"
#include "serde/protobuf/tests/data_generator.h"
#include "storage/parser_utils.h"

#include <seastar/testing/perf_tests.hh>
#include <seastar/util/log.hh>

#include <optional>
#include <string_view>

namespace {

[[maybe_unused]]
std::vector<char> one_out_of(size_t n) {
    std::vector<char> ret(n, 'a');
    ret[0] = 'b';
    return ret;
}

[[maybe_unused]]
std::vector<char> n_unique(size_t n) {
    std::vector<char> ret;
    ret.reserve(n);
    for(size_t i = 0; i < n; i++) {
        ret.push_back((char)i);
    }
    return ret;
}

/**
 * Generates a linear protobuf schema.
 *
 * I.e, if total_fields=3 then the following would be generated;
 *
 * syntax = "proto3";
 * message Linear {
 *      string a1 = 1;
 *      string a2 = 2;
 *      string a3 = 3;
 * }
 */
std::string generate_linear_proto(size_t total_fields) {
    constexpr auto proto_template = R"(
    syntax = "proto3";
    message Linear {{
        {}
    }})";

    std::string fields = "";
    for (size_t i = 1; i <= total_fields; i++) {
        fields += std::format("string a{} = {};\n", i, i);
    }

    return std::format(proto_template, fields);
}

chunked_vector<model::record_batch>
share_batches(chunked_vector<model::record_batch>& batches) {
    chunked_vector<model::record_batch> ret;
    for (auto& batch : batches) {
        ret.push_back(batch.share());
    }
    return ret;
}

struct counting_consumer {
    size_t total_bytes = 0;
    datalake::record_multiplexer mux;
    ss::abort_source& as;
    ss::future<ss::stop_iteration> operator()(model::record_batch&& batch) {
        total_bytes += batch.size_bytes();
        return mux.do_multiplex(std::move(batch), kafka::offset{}, as);
    }
    ss::future<counting_consumer> end_of_stream() {
        auto res = co_await std::move(mux).finish();
        if (res.has_error()) [[unlikely]] {
            throw std::runtime_error(
              fmt::format("failed to end stream: {}", res.error()));
        }
        co_return std::move(*this);
    }
};

struct decompressing_consumer {
    size_t total_compressed_bytes = 0;
    size_t total_decompressed_bytes = 0;

    ss::future<ss::stop_iteration> operator()(model::record_batch batch) {
        total_compressed_bytes += batch.size_bytes();
        if (batch.compressed()) {
            batch = co_await storage::internal::decompress_batch(
              std::move(batch));
        }
        total_decompressed_bytes += batch.size_bytes();
        co_return ss::stop_iteration::no;
    }
    ss::future<decompressing_consumer> end_of_stream() {
        co_return std::move(*this);
    }
};

ss::logger tlogger("rmb");

} // namespace

class record_multiplexer_bench_fixture
  : public datalake::tests::catalog_and_registry_fixture {
public:
    record_multiplexer_bench_fixture()
      : _schema_cache({10, 5})
      , _schema_mgr(catalog)
      , _type_resolver(registry, _schema_cache)
      , _record_gen(&registry)
      , _table_creator(_type_resolver, _schema_mgr) {}

    template<typename T>
    requires std::same_as<T, ::testing::protobuf_generator_config>
             || std::same_as<T, ::testing::avro_generator_config>
    ss::future<> configure_bench(
      T gen_config,
      std::string schema,
      size_t batches,
      size_t records_per_batch,
      model::compression compression_type = model::compression::zstd) {
        if constexpr (std::is_same_v<T, ::testing::protobuf_generator_config>) {
            _batch_data = co_await generate_protobuf_batches(
              records_per_batch,
              batches,
              compression_type,
              "proto_schema",
              schema,
              {0},
              gen_config);
        } else {
            _batch_data = co_await generate_avro_batches(
              records_per_batch,
              batches,
              compression_type,
              "avro_schema",
              schema,
              gen_config);
        }
    }

    ss::future<size_t> run_bench() {
        auto reader = model::make_fragmented_memory_record_batch_reader(
          share_batches(_batch_data));
        auto consumer = decompressing_consumer{};
        
        perf_tests::start_measuring_time();
        auto res = co_await reader.consume(
          std::move(consumer), model::no_timeout);
        perf_tests::stop_measuring_time();

        vlog(
          tlogger.error,
          "compression ratio: {}",
          (float)res.total_decompressed_bytes / res.total_compressed_bytes);

        co_return res.total_decompressed_bytes;
    }

private:
    const model::ntp ntp{
      model::ns{"rp"}, model::topic{"t"}, model::partition_id{0}};
    const model::revision_id topic_rev{123};

    std::unordered_set<std::string> _added_names;
    datalake::chunked_schema_cache _schema_cache;
    datalake::catalog_schema_manager _schema_mgr;
    datalake::record_schema_resolver _type_resolver;
    datalake::tests::record_generator _record_gen;
    datalake::default_translator _translator;
    datalake::direct_table_creator _table_creator;
    datalake::translation_probe _translation_probe{ntp};
    chunked_vector<model::record_batch> _batch_data;
    ss::abort_source _as;

    datalake::record_multiplexer create_mux() {
        return datalake::record_multiplexer(
          ntp,
          topic_rev,
          std::make_unique<datalake::test_serde_parquet_writer_factory>(),
          _schema_mgr,
          _type_resolver,
          _translator,
          _table_creator,
          model::iceberg_invalid_record_action::dlq_table,
          datalake::location_provider(
            scoped_remote->remote.local().provider(), bucket_name),
          _translation_probe);
    }

    ss::future<>
    try_add_avro_schema(std::string_view name, std::string_view schema) {
        auto [_, added] = _added_names.emplace(name);
        if (!added) {
            co_return;
        }

        auto reg_res = co_await _record_gen.register_avro_schema(name, schema);
        if (reg_res.has_error()) [[unlikely]] {
            throw std::runtime_error(fmt::format(
              "failed to register avro schema: {}", reg_res.error()));
        }
    }

    ss::future<>
    try_add_protobuf_schema(std::string_view name, std::string_view schema) {
        auto [_, added] = _added_names.emplace(name);
        if (!added) {
            co_return;
        }

        auto reg_res = co_await _record_gen.register_protobuf_schema(
          name, schema);
        if (reg_res.has_error()) [[unlikely]] {
            throw std::runtime_error(fmt::format(
              "failed to register protobuf schema: {}", reg_res.error()));
        }
    }

    ss::future<chunked_vector<model::record_batch>> generate_batches(
      size_t records_per_batch,
      size_t batches,
      model::compression compression_type,
      std::function<ss::future<
        checked<std::nullopt_t, datalake::tests::record_generator::error>>(
        storage::record_batch_builder&)> add_record) {
        chunked_vector<model::record_batch> ret;
        ret.reserve(batches);

        model::offset o{0};
        for (size_t i = 0; i < batches; ++i) {
            storage::record_batch_builder batch_builder(
              model::record_batch_type::raft_data, o);

            // Add some records per batch.
            for (size_t r = 0; r < records_per_batch; ++r) {
                auto res = co_await add_record(batch_builder);
                ++o;

                if (res.has_error()) [[unlikely]] {
                    throw std::runtime_error(
                      fmt::format("unable to add record: {}", res.error()));
                }
            }

            auto batch = std::move(batch_builder).build();
            if (compression_type != model::compression::none) {
                batch = co_await storage::internal::compress_batch(
                  compression_type, std::move(batch));
            }
            ret.emplace_back(std::move(batch));
        }

        co_return ret;
    }

    ss::future<chunked_vector<model::record_batch>> generate_protobuf_batches(
      size_t records_per_batch,
      size_t batches,
      model::compression compression_type,
      std::string schema_name,
      std::string proto_schema,
      std::vector<int32_t> msg_idx,
      ::testing::protobuf_generator_config gen_config) {
        co_await try_add_protobuf_schema(schema_name, proto_schema);
        co_return co_await generate_batches(
          records_per_batch, batches, compression_type, [&](auto& bb) {
              return _record_gen.add_random_protobuf_record(
                bb, schema_name, msg_idx, std::nullopt, gen_config);
          });
    }

    ss::future<chunked_vector<model::record_batch>> generate_avro_batches(
      size_t records_per_batch,
      size_t batches,
      model::compression compression_type,
      std::string schema_name,
      std::string avro_schema,
      ::testing::avro_generator_config gen_config) {
        co_await try_add_avro_schema(schema_name, avro_schema);
        co_return co_await generate_batches(
          records_per_batch, batches, compression_type, [&](auto& bb) {
              return _record_gen.add_random_avro_record(
                bb, schema_name, std::nullopt, gen_config);
          });
    }
};

namespace {

// Specifies how many batches should be in the test dataset.
static constexpr size_t batches = 1000;
// Specifies how many records should be in each batch of the test dataset.
static constexpr size_t records_per_batch = 1;

} // namespace

PERF_TEST_CN(
  record_multiplexer_bench_fixture, protobuf_381_byte_message_linear_1_field) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{.string_length_range{302, 302}, .string_characters=n_unique(1)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_1_5x) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{.string_length_range{453, 453}, .string_characters=n_unique(2)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_2x) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{2 * 302, 2 * 302}, .string_characters=n_unique(3)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_3x) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{3 * 302, 3 * 302}, .string_characters=n_unique(4)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_4x) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{4 * 302, 4 * 302}, .string_characters=n_unique(5)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_5x) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{5 * 302, 5 * 302}, .string_characters=n_unique(5)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_6x) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{6 * 302, 6 * 302}, .string_characters=n_unique(5)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_7x) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{7 * 302, 7 * 302}, .string_characters=n_unique(6)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_8x) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{8 * 302, 8 * 302}, .string_characters=n_unique(6)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_9x) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{9 * 302, 9 * 302}, .string_characters=n_unique(6)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_10x) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{10 * 302, 10 * 302}, .string_characters=n_unique(6)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_15x) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{15 * 302, 15 * 302}, .string_characters=n_unique(7)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_20x) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{20 * 302, 20 * 302}, .string_characters=n_unique(7)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_40x) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{40 * 302, 40 * 302}, .string_characters=n_unique(7)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_80x) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{80 * 302, 80 * 302}, .string_characters=n_unique(8)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_160x) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{160 * 302, 160 * 302}, .string_characters=n_unique(8)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}
/*

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_4_uniq) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{15*302, 15*302}, .string_characters = n_unique(4)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_6_uniq) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{15*302, 15*302}, .string_characters = n_unique(6)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_8_uniq) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{15*302, 15*302}, .string_characters = n_unique(8)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_10_uniq) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{15*302, 15*302}, .string_characters = n_unique(10)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_20_uniq) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{15*302, 15*302}, .string_characters = n_unique(20)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_1_2_b) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{15*302, 15*302}, .string_characters = {'a', 'b',}},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_1_5_b) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{15*302, 15*302}, .string_characters = one_out_of(5)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_1_10_b) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{15*302, 15*302}, .string_characters = one_out_of(10)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_1_15_b) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{15*302, 15*302}, .string_characters = one_out_of(15)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_1_20_b) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{15*302, 15*302}, .string_characters = one_out_of(20)},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_1_40_b) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{15*302, 15*302},
        .string_characters = one_out_of(40),
      },
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}

PERF_TEST_CN(
  record_multiplexer_bench_fixture,
  protobuf_381_byte_message_linear_1_field_0_b) {
    co_await configure_bench(
      ::testing::protobuf_generator_config{
        .string_length_range{15*302, 15*302}, .string_characters = {'a'}},
      generate_linear_proto(1),
      batches,
      records_per_batch);
    co_return co_await run_bench();
}
*/
