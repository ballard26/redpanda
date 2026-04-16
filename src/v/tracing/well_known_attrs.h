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

#include "strings/static_str.h"

namespace trace_attrs {

// OTel semantic conventions for messaging systems
constexpr static_str topic{"messaging.destination.name"};
constexpr static_str partition{"messaging.destination.partition.id"};
constexpr static_str offset{"messaging.kafka.offset"};
constexpr static_str batch_size{"messaging.batch.message_count"};
constexpr static_str client_id{"messaging.client_id"};
constexpr static_str api_key{"rpc.method"};
constexpr static_str api_version{"messaging.kafka.api_version"};
constexpr static_str error_code{"messaging.kafka.error_code"};
constexpr static_str shard_id{"thread.id"};
constexpr static_str node_id{"service.instance.id"};

} // namespace trace_attrs
