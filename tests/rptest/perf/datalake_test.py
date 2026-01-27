# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import json
import random
import string
from typing import Any, cast

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from ducktape.mark import matrix
from ducktape.tests.test import TestContext

from rptest.services.cluster import cluster
from rptest.services.utils import LocalPayloadDirectory
from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark
from rptest.services.openmessaging_benchmark_configs import OMBSampleConfigurations
from rptest.services.redpanda import PandaproxyConfig, SISettings, SchemaRegistryConfig
from rptest.tests.datalake.catalog_service_factory import filesystem_catalog_type
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest


def _make_linear_avro_schema(num_fields: int) -> str:
    """Generate an Avro schema with num_fields string fields named a1, a2, ..., aN."""
    fields = [{"name": f"a{i}", "type": "string"} for i in range(1, num_fields + 1)]
    schema = {
        "type": "record",
        "namespace": "com.redpanda.examples.avro",
        "name": f"Linear{num_fields}",
        "fields": fields,
    }
    return json.dumps(schema)


LINEAR20_AVRO_SCHEMA = _make_linear_avro_schema(20)


class DatalakeTest(RedpandaTest):
    def __init__(self, test_ctx: TestContext, *args: Any, **kwargs: Any):
        self._ctx = test_ctx
        super(DatalakeTest, self).__init__(
            test_ctx,
            num_brokers=3,
            si_settings=SISettings(test_context=test_ctx),
            extra_rp_conf={
                "iceberg_enabled": "true",
            },
            schema_registry_config=SchemaRegistryConfig(),
            pandaproxy_config=PandaproxyConfig(),
            *args,
            **kwargs,
        )

    def setUp(self) -> None:
        # redpanda will be started by DatalakeServices
        pass

    def _get_schema_registry_client(self) -> SchemaRegistryClient:
        schema_registry_url = self.redpanda.schema_reg().split(",", 1)[0]
        schema_registry_conf: dict[str, str] = {"url": schema_registry_url}

        return SchemaRegistryClient(schema_registry_conf)

    def _random_linear_msg(self, num_fields: int, str_len: int) -> dict[str, str]:
        """Generate a random message dict with num_fields string fields named a1, a2, ..., aN."""
        msg: dict[str, str] = {}
        for i in range(1, num_fields + 1):
            random_str = "".join(random.choices(string.ascii_letters, k=str_len))
            msg[f"a{i}"] = random_str
        return msg

    @cluster(num_nodes=6)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_omb(self, cloud_storage_type: str) -> None:
        topic_name = "atestingtopic"
        topic_partitions = 50
        producer_rate_bytes_s = 40 * 1024 * 1024

        num_fields = 20
        msg_field_size_bytes = 13
        total_unique_messages = 100

        with DatalakeServices(
            self._ctx,
            redpanda=self.redpanda,
            include_query_engines=[],
            catalog_type=filesystem_catalog_type(),
        ) as dl:
            dl_any: Any = dl
            dl_any.create_iceberg_enabled_topic(
                name=topic_name,
                partitions=topic_partitions,
                replicas=3,
                iceberg_mode="value_schema_id_prefix",
            )

            schema_registry_client = self._get_schema_registry_client()

            payloads = LocalPayloadDirectory()
            # Note that `avro_serializer` is used to serialize the message and register its schema with Redpanda's schema registry.
            avro_serializer = AvroSerializer(
                schema_registry_client, LINEAR20_AVRO_SCHEMA
            )

            payload_size = 0
            for i in range(0, total_unique_messages):
                msg: dict[str, str] = self._random_linear_msg(num_fields, msg_field_size_bytes)
                msg_bytes: bytes = cast(
                    bytes,
                    avro_serializer(msg, SerializationContext(topic_name, MessageField.VALUE)),
                )
                payload_size = len(msg_bytes)
                payloads.add_payload(f"message_{i}", msg_bytes)

            workload = {
                "name": "DatalakeWorkload",
                "existing_topic_list": [topic_name],
                "subscriptions_per_topic": 1,
                "consumer_per_subscription": 5,
                "producers_per_topic": 5,
                "producer_rate": producer_rate_bytes_s // payload_size,
                "consumer_backlog_size_GB": 0,
                "test_duration_minutes": 1,
                "warmup_duration_minutes": 1,
            }
            driver = {
                "name": "CommonWorkloadDriver",
                "replication_factor": 3,
                "request_timeout": 300000,
                "producer_config": {
                    "enable.idempotence": "true",
                    "acks": "all",
                    "linger.ms": 1,
                    "max.in.flight.requests.per.connection": 5,
                    "batch.size": 16384,
                },
                "consumer_config": {
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": "false",
                    "max.partition.fetch.bytes": 131072,
                },
            }
            validator = {
                OMBSampleConfigurations.AVG_THROUGHPUT_MBPS: [
                    OMBSampleConfigurations.gte(
                        (0.9 * producer_rate_bytes_s) // (1024 * 1024)
                    )
                ]
            }

            benchmark = OpenMessagingBenchmark(
                ctx=self._ctx,
                redpanda=self.redpanda,
                driver=driver,
                workload=(workload, validator),
                topology="ensemble",
                local_payload_dir=payloads,
                # Share the omb driver node with the iceberg catalog node. Both do hardly anything and this saves us a node.
                node=dl.catalog_service.get_node(0),
            )
            benchmark.start()
            benchmark_time_min = benchmark.benchmark_time_mins() + 1
            benchmark.wait(timeout_sec=benchmark_time_min * 60)
            benchmark.check_succeed()
