/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <seastar/core/iostream.hh>

namespace cloud_storage {
struct serialized_data_stream {
    ss::input_stream<char> stream;
    size_t size_bytes;
};

enum class manifest_type {
    topic,
    partition,
    tx_range,
    cluster_metadata,
    spillover,
};

std::ostream& operator<<(std::ostream& s, manifest_type t);

enum class manifest_format {
    json,
    serde,
};

std::ostream& operator<<(std::ostream& s, manifest_format t);
class base_manifest {
public:
    virtual ~base_manifest();

    /// Update manifest file from input_stream (remote set)
    virtual ss::future<> update(ss::input_stream<char> is) = 0;

    /// default implementation for derived classes that don't support multiple
    /// formats
    virtual ss::future<> update(manifest_format, ss::input_stream<char> is) {
        return update(std::move(is));
    }

    /// Serialize manifest object
    ///
    /// \return asynchronous input_stream with the serialized json
    virtual ss::future<serialized_data_stream> serialize() const = 0;

    /// Manifest object format and name in S3
    virtual remote_manifest_path get_manifest_path() const = 0;

    /// default implementation for derived classed that don't support multiple
    /// formats
    virtual std::pair<manifest_format, remote_manifest_path>
    get_manifest_format_and_path() const {
        return {manifest_format::json, get_manifest_path()};
    }

    /// Get manifest type
    virtual manifest_type get_manifest_type() const = 0;
};
} // namespace cloud_storage
