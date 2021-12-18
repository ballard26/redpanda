/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "bytes/iobuf.h"
#include "cluster/security_frontend.h"
#include "kafka/protocol/fwd.h"
#include "kafka/protocol/request_reader.h"
#include "kafka/server/connection_context.h"
#include "kafka/server/fetch_session_cache.h"
#include "kafka/server/logger.h"
#include "kafka/server/protocol.h"
#include "kafka/server/response.h"
#include "kafka/types.h"
#include "seastarx.h"
#include "vlog.h"

#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/unaligned.hh>
#include <seastar/util/log.hh>

#include <memory>

namespace kafka {

constexpr auto request_header_size = sizeof(int16_t) + sizeof(int16_t)
                                     + sizeof(correlation_id::type)
                                     + sizeof(int16_t);

struct request_header {
    api_key key;
    api_version version;
    correlation_id correlation;
    ss::temporary_buffer<char> client_id_buffer;
    std::optional<std::string_view> client_id;
};

std::ostream& operator<<(std::ostream&, const request_header&);

class request_context {
public:
    request_context(
      ss::lw_shared_ptr<connection_context> conn,
      request_header&& header,
      iobuf&& request,
      ss::lowres_clock::duration throttle_delay) noexcept
      : _conn(std::move(conn))
      , _header(std::move(header))
      , _reader(std::move(request))
      , _throttle_delay(throttle_delay) {}

    request_context(const request_context&) = delete;
    request_context& operator=(const request_context&) = delete;
    request_context(request_context&& o) noexcept = default;
    request_context& operator=(request_context&& o) noexcept = default;
    ~request_context() noexcept = default;

    const request_header& header() const { return _header; }

    ss::lw_shared_ptr<connection_context> connection() { return _conn; }

    request_reader& reader() { return _reader; }

    latency_probe& probe() { return _conn->server().probe(); }

    const cluster::metadata_cache& metadata_cache() const {
        return _conn->server().metadata_cache();
    }

    cluster::metadata_cache& metadata_cache() {
        return _conn->server().metadata_cache();
    }

    cluster::topics_frontend& topics_frontend() const {
        return _conn->server().topics_frontend();
    }

    cluster::id_allocator_frontend& id_allocator_frontend() const {
        return _conn->server().id_allocator_frontend();
    }

    bool is_idempotence_enabled() {
        return _conn->server().is_idempotence_enabled();
    }

    bool are_transactions_enabled() {
        return _conn->server().are_transactions_enabled();
    }

    cluster::tx_gateway_frontend& tx_gateway_frontend() const {
        return _conn->server().tx_gateway_frontend();
    }

    int32_t throttle_delay_ms() const {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                 _throttle_delay)
          .count();
    }

    kafka::group_router& groups() { return _conn->server().group_router(); }

    cluster::shard_table& shards() { return _conn->server().shard_table(); }

    ss::sharded<coproc::partition_manager>& coproc_partition_manager() {
        return _conn->server().coproc_partition_manager();
    }

    ss::sharded<cluster::partition_manager>& partition_manager() {
        return _conn->server().partition_manager();
    }

    fetch_session_cache& fetch_sessions() {
        return _conn->server().fetch_sessions_cache();
    }

    fetch_metadata_cache& get_fetch_metadata_cache() {
        return _conn->server().get_fetch_metadata_cache();
    }

    // clang-format off
    template<typename ResponseType>
    CONCEPT(requires requires (
            ResponseType r, response_writer& writer, api_version version) {
        { r.encode(writer, version) } -> std::same_as<void>;
    })
    // clang-format on
    ss::future<response_ptr> respond(ResponseType r) {
        vlog(
          klog.trace,
          "[{}:{}] sending {}:{} response {}",
          _conn->client_host(),
          _conn->client_port(),
          ResponseType::api_type::key,
          ResponseType::api_type::name,
          r);
        auto resp = std::make_unique<response>();
        r.encode(resp->writer(), header().version);
        return ss::make_ready_future<response_ptr>(std::move(resp));
    }

    coordinator_ntp_mapper& coordinator_mapper() {
        return _conn->server().coordinator_mapper();
    }

    const ss::sstring& listener() const { return _conn->listener(); }
    security::sasl_server& sasl() { return _conn->sasl(); }
    security::credential_store& credentials() {
        return _conn->server().credentials();
    }

    template<typename T>
    bool authorized(
      security::acl_operation operation,
      const T& name,
      authz_quiet quiet = authz_quiet{false}) {
        return _conn->authorized(operation, name, quiet);
    }

    cluster::security_frontend& security_frontend() const {
        return _conn->server().security_frontend();
    }

    v8_engine::data_policy_table& data_policy_table() const {
        return _conn->server().data_policy_table();
    }

    security::authorizer& authorizer() { return _conn->server().authorizer(); }

    cluster::controller_api& controller_api() {
        return _conn->server().controller_api();
    }

private:
    ss::lw_shared_ptr<connection_context> _conn;
    request_header _header;
    request_reader _reader;
    ss::lowres_clock::duration _throttle_delay;
};

// Executes the API call identified by the specified request_context.
process_result_stages process_request(request_context&&, ss::smp_service_group);

bool track_latency(api_key);

} // namespace kafka
