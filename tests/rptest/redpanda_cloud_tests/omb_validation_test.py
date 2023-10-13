# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import math
from time import sleep

from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from ducktape.tests.test import TestContext
from rptest.services.producer_swarm import ProducerSwarm
from rptest.clients.rpk import RpkTool
from rptest.services.redpanda_cloud import AdvertisedTierConfigs, CloudTierName
from rptest.services.redpanda import (SISettings, RedpandaServiceCloud)
from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark
from rptest.services.openmessaging_benchmark_configs import \
    OMBSampleConfigurations

KiB = 1024
MiB = KiB * KiB
GiB = KiB * MiB
KB = 10**3
MB = 10**6
GB = 10**9
minutes = 60
hours = 60 * minutes


def get_globals_value(globals, key_name, default=None):
    _config = {}
    if RedpandaServiceCloud.GLOBAL_CLOUD_CLUSTER_CONFIG in globals:
        # Load needed config values from cloud section
        # of globals prior to actual cluster creation
        _config = globals[RedpandaServiceCloud.GLOBAL_CLOUD_CLUSTER_CONFIG]
    return _config.get(key_name, default)


class OMBValidationTest(RedpandaTest):
    def __init__(self, test_ctx: TestContext, *args, **kwargs):
        self._ctx = test_ctx
        # Get tier value
        cloud_tier_str = get_globals_value(self._ctx.globals,
                                           "config_profile_name",
                                           default="tier-1-aws")
        cloud_tier = CloudTierName(cloud_tier_str)
        extra_rp_conf = None
        num_brokers = None

        if cloud_tier == CloudTierName.DOCKER:
            # TODO: Bake the docker config into a higher layer that will
            # automatically load these settings upon call to make_rp_service
            config = AdvertisedTierConfigs[CloudTierName.DOCKER]
            num_brokers = config.num_brokers
            extra_rp_conf = {
                'log_segment_size': config.segment_size,
                'cloud_storage_cache_size': config.cloud_cache_size,
                'kafka_connections_max': config.connections_limit,
            }

        super(OMBValidationTest,
              self).__init__(test_ctx,
                             *args,
                             num_brokers=num_brokers,
                             extra_rp_conf=extra_rp_conf,
                             cloud_tier=cloud_tier,
                             disable_cloud_storage_diagnostics=True,
                             **kwargs)

        self.tier_config = self.redpanda.advertised_tier_config
        if cloud_tier == CloudTierName.DOCKER:
            si_settings = SISettings(
                test_ctx,
                log_segment_size=self.small_segment_size,
                cloud_storage_cache_size=self.tier_config.cloud_cache_size,
            )
            self.redpanda.set_si_settings(si_settings)
            self.s3_port = si_settings.cloud_storage_api_endpoint_port

        test_ctx.logger.info(f"Cloud tier {cloud_tier}: {self.tier_config}")

        self.rpk = RpkTool(self.redpanda)

        self.base_validator = {
            OMBSampleConfigurations.E2E_LATENCY_50PCT:
            [OMBSampleConfigurations.lte(20)],
            OMBSampleConfigurations.E2E_LATENCY_75PCT:
            [OMBSampleConfigurations.lte(25)],
            OMBSampleConfigurations.E2E_LATENCY_99PCT:
            [OMBSampleConfigurations.lte(50)],
            OMBSampleConfigurations.E2E_LATENCY_999PCT:
            [OMBSampleConfigurations.lte(100)],
        }

    def _partition_count(self) -> int:
        tier_config = self.redpanda.advertised_tier_config
        machine_config = tier_config.machine_type_config
        return 5 * tier_config.num_brokers * machine_config.num_shards

    def _producer_count(self, ingress_rate) -> int:
        return max(ingress_rate // (4 * MiB), 8)

    def _consumer_count(self, egress_rate) -> int:
        return max(egress_rate // (4 * MiB), 8)

    def _mb_to_mib(self, mb):
        return math.floor(0.9537 * mb)

    @cluster(num_nodes=12)
    def test_max_connections(self):
        tier_config = self.redpanda.advertised_tier_config

        # Constants
        #

        PRODUCER_TIMEOUT_MS = 5000
        OMB_WORKERS = 4
        SWARM_WORKERS = 7

        # OMB parameters
        #

        producer_rate = tier_config.ingress_rate // 5
        subscriptions = max(
            tier_config.egress_rate // tier_config.ingress_rate, 1)
        total_producers = self._producer_count(producer_rate)
        total_consumers = self._consumer_count(producer_rate * subscriptions)
        warmup_duration = 1  # minutes
        test_duration = 5  # minutes

        workload = {
            "name": "MaxConnectionsTestWorkload",
            "topics": 1,
            "partitions_per_topic": self._partition_count(),
            "subscriptions_per_topic": subscriptions,
            "consumer_per_subscription": max(total_consumers // subscriptions,
                                             1),
            "producers_per_topic": total_producers,
            "producer_rate": producer_rate // (1 * KiB),
            "message_size": 1 * KiB,
            "payload_file": "payload/payload-1Kb.data",
            "consumer_backlog_size_GB": 0,
            "test_duration_minutes": test_duration,
            "warmup_duration_minutes": warmup_duration,
        }

        driver = {
            "name": "MaxConnectionsTestDriver",
            "replication_factor": 3,
            "request_timeout": 300000,
            "producer_config": {
                "enable.idempotence": "true",
                "acks": "all",
                "linger.ms": 1,
                "max.in.flight.requests.per.connection": 5,
            },
            "consumer_config": {
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",
            },
        }

        validator = self.base_validator | {
            OMBSampleConfigurations.AVG_THROUGHPUT_MBPS: [
                OMBSampleConfigurations.gte(
                    self._mb_to_mib(producer_rate // (1 * MB))),
            ],
        }

        # ProducerSwarm parameters
        #

        producer_kwargs = {}
        producer_kwargs['min_record_size'] = 64
        producer_kwargs['max_record_size'] = 64

        effective_msg_size = producer_kwargs['min_record_size'] + (
            producer_kwargs['max_record_size'] -
            producer_kwargs['min_record_size']) // 2

        conn_limit = tier_config.connections_limit - 3 * (total_producers +
                                                          total_consumers)
        _target_per_node = conn_limit // SWARM_WORKERS
        _conn_per_node = int(_target_per_node * 0.8)

        msg_rate_per_node = (1 * KiB) // effective_msg_size
        messages_per_sec_per_producer = max(
            msg_rate_per_node // _conn_per_node, 1)

        # single producer runtime
        # Roughly every 500 connection needs 60 seconds to ramp up
        warm_up_time_s = max(60 * math.ceil(_target_per_node / 500), 60)
        target_runtime_s = 60 * (test_duration +
                                 warmup_duration) + warm_up_time_s
        records_per_producer = messages_per_sec_per_producer * target_runtime_s

        self._ctx.logger.warn(
            f"Producers per node: {_conn_per_node} Messages per producer: {records_per_producer} Message rate: {messages_per_sec_per_producer} msg/s"
        )

        producer_kwargs[
            'messages_per_second_per_producer'] = messages_per_sec_per_producer

        benchmark = OpenMessagingBenchmark(self._ctx,
                                           self.redpanda,
                                           driver, (workload, validator),
                                           num_workers=OMB_WORKERS,
                                           topology="ensemble")

        # Create topic for swarm workers after OMB to avoid the reset
        swarm_topic_name = "swarm_topic"
        try:
            self.rpk.delete_topic(swarm_topic_name)
        except:
            # Ignore the exception that is thrown if the topic doesn't exist.
            pass

        self.rpk.create_topic(
            swarm_topic_name,
            #tier_config.partitions_upper_limit -
            self._partition_count(),
            replicas=3)

        swarm = []
        for _ in range(SWARM_WORKERS):
            _swarm_node = ProducerSwarm(
                self._ctx,
                self.redpanda,
                topic=swarm_topic_name,
                producers=_conn_per_node,
                records_per_producer=records_per_producer,
                timeout_ms=PRODUCER_TIMEOUT_MS,
                **producer_kwargs)

            swarm.append(_swarm_node)

        for s in swarm:
            s.start()

        # Allow time for the producers in the swarm to authenticate and start
        self._ctx.logger.info(
            f"waiting {warm_up_time_s} seconds to producer swarm to start")
        sleep(warm_up_time_s)

        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time() + 5

        try:
            benchmark.wait(timeout_sec=benchmark_time_min * 60)

            for s in swarm:
                s.wait(timeout_sec=5 * 60)

            benchmark.check_succeed()

        finally:
            self.rpk.delete_topic(swarm_topic_name)

    @cluster(num_nodes=12)
    def test_max_partitions(self):
        tier_config = self.redpanda.advertised_tier_config

        partitions_per_topic = self.tier_config.partitions_upper_limit
        subscriptions = max(
            tier_config.egress_rate // tier_config.ingress_rate, 1)
        producer_rate = tier_config.ingress_rate // 2
        total_producers = self._producer_count(producer_rate)
        total_consumers = self._consumer_count(producer_rate * subscriptions)

        workload = {
            "name": "MaxPartitionsTestWorkload",
            "topics": 1,
            "partitions_per_topic": partitions_per_topic,
            "subscriptions_per_topic": subscriptions,
            "consumer_per_subscription": max(total_consumers // subscriptions,
                                             1),
            "producers_per_topic": total_producers,
            "producer_rate": producer_rate / (1 * KiB),
            "message_size": 1 * KiB,
            "payload_file": "payload/payload-1Kb.data",
            "consumer_backlog_size_GB": 0,
            "test_duration_minutes": 5,
            "warmup_duration_minutes": 1,
        }

        validator = self.base_validator | {
            OMBSampleConfigurations.AVG_THROUGHPUT_MBPS: [
                OMBSampleConfigurations.gte(
                    self._mb_to_mib(producer_rate // (1 * MB))),
            ],
        }

        benchmark = OpenMessagingBenchmark(
            self._ctx,
            self.redpanda,
            "ACK_ALL_GROUP_LINGER_1MS_IDEM_MAX_IN_FLIGHT",
            (workload, validator),
            num_workers=10,
            topology="ensemble")
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time() + 5
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()

    @cluster(num_nodes=12)
    def test_common_workload(self):
        tier_config = self.redpanda.advertised_tier_config

        subscriptions = max(
            tier_config.egress_rate // tier_config.ingress_rate, 1)
        partitions = self._partition_count()
        total_producers = self._producer_count(tier_config.ingress_rate)
        total_consumers = self._consumer_count(tier_config.egress_rate)
        validator = self.base_validator | {
            OMBSampleConfigurations.AVG_THROUGHPUT_MBPS: [
                OMBSampleConfigurations.gte(
                    self._mb_to_mib(tier_config.ingress_rate // (1 * MB))),
            ],
        }

        workload = {
            "name": "CommonTestWorkload",
            "topics": 1,
            "partitions_per_topic": partitions,
            "subscriptions_per_topic": subscriptions,
            "consumer_per_subscription": max(total_consumers // subscriptions,
                                             1),
            "producers_per_topic": total_producers,
            "producer_rate": tier_config.ingress_rate // (1 * KiB),
            "message_size": 1 * KiB,
            "payload_file": "payload/payload-1Kb.data",
            "consumer_backlog_size_GB": 0,
            "test_duration_minutes": 5,
            "warmup_duration_minutes": 1,
        }

        driver = {
            "name": "CommonTestDriver",
            "reset": "true",
            "replication_factor": 3,
            "request_timeout": 300000,
            "producer_config": {
                "enable.idempotence": "true",
                "acks": "all",
                "linger.ms": 1,
                "max.in.flight.requests.per.connection": 5,
            },
            "consumer_config": {
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",
            },
        }

        benchmark = OpenMessagingBenchmark(self._ctx,
                                           self.redpanda,
                                           driver, (workload, validator),
                                           num_workers=10,
                                           topology="ensemble")
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time() + 5
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()

    @cluster(num_nodes=12)
    def test_retention(self):
        tier_config = self.redpanda.advertised_tier_config

        subscriptions = max(
            tier_config.egress_rate // tier_config.ingress_rate, 1)
        producer_rate = tier_config.ingress_rate
        partitions = self._partition_count()
        segment_bytes = 64 * MiB
        retention_bytes = 2 * segment_bytes
        # This will have 1/2 the test run with segment deletion occuring.
        test_duration_seconds = max(
            (2 * retention_bytes * partitions) // producer_rate, 5 * 60)
        total_producers = 10
        total_consumers = 10

        workload = {
            "name": "RetentionTestWorkload",
            "topics": 1,
            "partitions_per_topic": partitions,
            "subscriptions_per_topic": subscriptions,
            "consumer_per_subscription": max(total_consumers // subscriptions,
                                             1),
            "producers_per_topic": total_producers,
            "producer_rate": producer_rate // (1 * KiB),
            "message_size": 1 * KiB,
            "payload_file": "payload/payload-1Kb.data",
            "consumer_backlog_size_GB": 0,
            "test_duration_minutes": test_duration_seconds // 60,
            "warmup_duration_minutes": 1,
        }

        driver = {
            "name": "RetentionTestDriver",
            "replication_factor": 3,
            "request_timeout": 300000,
            "producer_config": {
                "enable.idempotence": "true",
                "acks": "all",
                "linger.ms": 1,
                "max.in.flight.requests.per.connection": 5,
            },
            "consumer_config": {
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",
            },
            "topic_config": {
                "retention.bytes": retention_bytes,
                "retention.local.target.bytes": retention_bytes,
                "segment.bytes": segment_bytes,
            },
        }

        validator = self.base_validator | {
            OMBSampleConfigurations.AVG_THROUGHPUT_MBPS: [
                OMBSampleConfigurations.gte(
                    self._mb_to_mib(producer_rate // (1 * MB))),
            ],
        }

        benchmark = OpenMessagingBenchmark(self._ctx,
                                           self.redpanda,
                                           driver, (workload, validator),
                                           num_workers=10,
                                           topology="ensemble")
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time() + 5
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()
