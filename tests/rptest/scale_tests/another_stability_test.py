# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import signal
import concurrent.futures
from collections import Counter

from ducktape.mark import matrix
from ducktape.utils.util import wait_until, TimeoutError
import numpy

from rptest.services.cluster import cluster
from rptest.clients.rpk import RpkTool, RpkException
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.services.rpk_consumer import RpkConsumer
from rptest.services.redpanda import ResourceSettings, RESTART_LOG_ALLOW_LIST, SISettings, LoggingConfig, MetricsEndpoint
from rptest.services.kgo_verifier_services import KgoVerifierProducer, KgoVerifierSeqConsumer, KgoVerifierRandomConsumer
from rptest.services.kgo_repeater_service import KgoRepeaterService, repeater_traffic
from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark
from rptest.services.openmessaging_benchmark_configs import OMBSampleConfigurations

# An unreasonably large fetch request: we submit requests like this in the
# expectation that the server will properly clamp the amount of data it
# actually tries to marshal into a response.
# franz-go default maxBrokerReadBytes -- --fetch-max-bytes may not exceed this
BIG_FETCH = 104857600

# How many partitions we will create per shard: this is the primary scaling
# factor that controls how many partitions a given cluster will get.
PARTITIONS_PER_SHARD = 1000

# Large volume of data to write. If tiered storage is enabled this is the
# amount of data to retain total. Otherwise, this can be used as a large volume
# of data to write.
STRESS_DATA_SIZE = 1024 * 1024 * 1024 * 100

# How many partitions we will create per shard not including replicas
PARTITIONS_PER_SHARD = 5

# How many segments to fill S3 with per partition
SEGMENTS_PER_PARTITION = 1200

# Size of segments
SEGMENT_SIZE_BYTES = 512 * 1024 * 1024

# Per node
LOCAL_RETENTION_PERCENT = 0.6

# Per partition
REMOTE_RETENTION_BYTES = 430312 * 1024 * 1024

NUMBER_OF_NODES = 9

# 14 MB/s
PER_SHARD_PRODUCE_RATE_BYTES = 14 * 1024 * 1024

# 71 MB/s
PER_SHARD_PRODUCE_RATE_BYTES = 71 * 1024 * 1024


class ScaleParameters:
    def __init__(self,
                 redpanda,
                 replication_factor,
                 tiered_storage_enabled=False):
        self.redpanda = redpanda
        self.tiered_storage_enabled = tiered_storage_enabled

        node_count = len(self.redpanda.nodes)

        # If we run on nodes with more memory than our HARD_PARTITION_LIMIT, then
        # artificially throttle the nodes' memory to avoid the test being too easy.
        # We are validating that the system works up to the limit, and that it works
        # up to the limit within the default per-partition memory footprint.
        node_memory = self.redpanda.get_node_memory_mb()
        self.node_cpus = self.redpanda.get_node_cpu_count()
        node_disk_free = self.redpanda.get_node_disk_free()

        self.logger.info(
            f"Nodes have {self.node_cpus} cores, {node_memory}MB memory, {node_disk_free / (1024 * 1024)}MB free disk"
        )

        # Calculate how many partitions we will aim to create.
        self.partition_limit = PARTITIONS_PER_SHARD * self.node_cpus * node_count

        self.logger.info(f"Selected partition limit {self.partition_limit}")

        # Emulate seastar's policy for default reserved memory
        reserved_memory = max(1536, int(0.07 * node_memory) + 1)
        effective_node_memory = node_memory - reserved_memory

        partition_replicas_per_node = int(
            (self.partition_limit * replication_factor) / node_count)

        self.retention_bytes = int((node_disk_free * LOCAL_RETENTION_PERCENT) /
                                   partition_replicas_per_node)
        self.local_retention_bytes = None

        # Choose an appropriate segment size to enable retention
        # rules to kick in promptly.
        # TODO: redpanda should figure this out automatically by
        #       rolling segments pre-emptively if low on disk space
        self.segment_size = SEGMENT_SIZE_BYTES

        # Tiered storage will have a warmup period where it will set the
        # segment size and local retention lower to ensure a large number of
        # segments.
        self.segment_size_after_warmup = self.segment_size

        # NOTE: using retention_bytes that is aimed at occupying disk space.
        self.local_retention_after_warmup = self.retention_bytes

        if tiered_storage_enabled:
            # When testing with tiered storage, the tuning goals of the test
            # parameters are different: we want to stress the number of
            # uploaded segments.

            # Locally retain as many segments as a full day.
            self.segment_size = 32 * 1024

            # initially set local retention to a small amount to avoid
            # filling memory with tons of small segments
            self.local_retention_bytes = self.segment_size * 24

            # About 72 hours of remote retention
            self.retention_bytes = int(
                (self.local_retention_after_warmup / 6) * 72)

            # Set a max upload interval such that won't swamp S3 -- we should
            # already be uploading somewhat frequently given the segment size.
            cloud_storage_segment_max_upload_interval_sec = 300
            cloud_storage_housekeeping_interval_ms = cloud_storage_segment_max_upload_interval_sec * 1000

            self.si_settings = SISettings(
                redpanda._context,
                log_segment_size=self.segment_size,
                cloud_storage_segment_max_upload_interval_sec=
                cloud_storage_segment_max_upload_interval_sec,
                cloud_storage_housekeeping_interval_ms=
                cloud_storage_housekeeping_interval_ms,
            )

        # A 24 core i3en.6xlarge has about 1GB/s disk write
        # bandwidth.  Divide by 2 to give comfortable room for variation.
        # This is total bandwidth from a group of producers.
        self.expect_bandwidth = (node_count / replication_factor) * (
            self.node_cpus / 24.0) * 1E9 * 0.5

        # Single-producer tests are slower, bottlenecked on the
        # client side.
        self.expect_single_bandwidth = 200E6

        if tiered_storage_enabled:
            self.expect_bandwidth /= 2
            self.expect_single_bandwidth /= 2

        # On dedicated nodes we will use an explicit reactor stall threshold
        # as a success condition.
        self.redpanda.set_resource_settings(
            ResourceSettings(reactor_stall_threshold=100))

        self.logger.info(
            f"Selected retention.bytes={self.retention_bytes}, retention.local.target.bytes={self.local_retention_bytes}, segment.bytes={self.segment_size}"
        )

        mb_per_partition = 1

        # Should not happen on the expected EC2 instance types where
        # the cores-RAM ratio is sufficient to meet our shards-per-core
        if effective_node_memory < partition_replicas_per_node / mb_per_partition:
            raise RuntimeError(
                f"Node memory is too small ({node_memory}MB - {reserved_memory}MB)"
            )

    @property
    def logger(self):
        return self.redpanda.logger


class AnotherStabilityTest(PreallocNodesTest):
    """
    Validates basic functionality in the presence of larger numbers
    of partitions than most other tests.
    """
    topics = ()

    # Redpanda is responsible for bounding its own startup time via
    # STORAGE_TARGET_REPLAY_BYTES.  The resulting walltime for startup
    # depends on the speed of the disk.  60 seconds is long enough
    # for an i3en.xlarge (and more than enough for faster instance types)
    EXPECT_START_TIME = 60

    LEADER_BALANCER_PERIOD_MS = 30000

    ALLOC_NODES = NUMBER_OF_NODES + 3

    def __init__(self, test_ctx, *args, **kwargs):
        self._ctx = test_ctx
        super(AnotherStabilityTest, self).__init__(
            test_ctx,
            *args,
            num_brokers=NUMBER_OF_NODES,
            node_prealloc_count=3,
            extra_rp_conf={
                # Avoid having to wait 5 minutes for leader balancer to activate
                "leader_balancer_idle_timeout": self.LEADER_BALANCER_PERIOD_MS,
                "leader_balancer_mute_timeout": self.LEADER_BALANCER_PERIOD_MS,

                'disable_metrics': True,
                'disable_public_metrics': False,

                # Enable all the rate limiting things we would have in production, to ensure
                # their effect is accounted for, but with high enough limits that we do
                # not expect to hit them.
                'kafka_connection_rate_limit': 10000,
                'kafka_connections_max': 50000,

                # In testing tiered storage, we care about creating as many
                # cloud segments as possible. To that end, bounding the segment
                # size isn't productive.
                'cloud_storage_segment_size_min': 1,
                'log_segment_size_min': 1024,

                # Disable segment merging: when we create many small segments
                # to pad out tiered storage metadata, we don't want them to
                # get merged together.
                'cloud_storage_enable_segment_merging': False,
            },
            # Configure logging the same way a user would when they have
            # very many partitions: set logs with per-partition messages
            # to warn instead of info.
            log_config=LoggingConfig('info',
                                     logger_levels={
                                         'storage': 'warn',
                                         'storage-gc': 'warn',
                                         'raft': 'warn',
                                         'offset_translator': 'warn'
                                     }),
            **kwargs)
        self.rpk = RpkTool(self.redpanda)

    def _all_elections_done(self, topic_names: list[str], p_per_topic: int):
        any_incomplete = False
        for tn in topic_names:
            try:
                partitions = list(self.rpk.describe_topic(tn, tolerant=True))
            except RpkException as e:
                # One retry.  This is a case where running rpk after a full
                # cluster restart can time out after 30 seconds, but succeed
                # promptly as soon as you retry.
                self.logger.error(f"Retrying describe_topic for {e}")
                partitions = list(self.rpk.describe_topic(tn, tolerant=True))

            if len(partitions) < p_per_topic:
                self.logger.info(f"describe omits partitions for topic {tn}")
                any_incomplete = True
                continue

            assert len(partitions) == p_per_topic
            for p in partitions:
                if p.leader == -1:
                    self.logger.info(
                        f"partition {tn}/{p.id} has no leader yet")
                    any_incomplete = True

        return not any_incomplete

    def _node_leadership_evacuated(self, topic_names: list[str],
                                   p_per_topic: int, node_id: int):
        any_incomplete = False
        for tn in topic_names:
            partitions = list(self.rpk.describe_topic(tn, tolerant=True))
            if len(partitions) < p_per_topic:
                self.logger.info(f"describe omits partitions for topic {tn}")
                any_incomplete = True
                continue

            assert len(partitions) == p_per_topic
            for p in partitions:
                if p.leader == node_id:
                    self.logger.info(
                        f"partition {tn}/{p.id} still on node {node_id}")
                    any_incomplete = True

        return not any_incomplete

    def _node_leadership_balanced(self, topic_names: list[str],
                                  p_per_topic: int):
        node_leader_counts = Counter()
        any_incomplete = False
        for tn in topic_names:
            try:
                partitions = list(self.rpk.describe_topic(tn, tolerant=True))
            except RpkException as e:
                # We can get e.g. timeouts from rpk if it is trying to describe
                # a big topic on a heavily loaded cluster: treat these as retryable
                # and let our caller call us again.
                self.logger.warn(f"RPK error, assuming retryable: {e}")
                return False

            if len(partitions) < p_per_topic:
                self.logger.info(f"describe omits partitions for topic {tn}")
                any_incomplete = True
                continue

            assert len(partitions) == p_per_topic
            node_leader_counts.update(p.leader for p in partitions)

        for n, c in node_leader_counts.items():
            self.logger.info(f"node {n} leaderships: {c}")

        assert len(node_leader_counts) <= len(self.redpanda.nodes)
        if len(node_leader_counts) != len(self.redpanda.nodes):
            self.logger.info("Not all nodes have leaderships")
            return False

        if any_incomplete:
            return False

        data = list(node_leader_counts.values())
        stddev = numpy.std(data)
        error = stddev / (
            (len(topic_names) * p_per_topic) / len(self.redpanda.nodes))

        # FIXME: this isn't the same check the leader balancer itself does, but it
        # should suffice to check the leader balancer is progressing.
        threshold = 0.1
        if (p_per_topic * len(topic_names)) < 5000:
            # Low scale systems have bumpier stats
            threshold = 0.25

        balanced = error < threshold
        self.logger.info(
            f"leadership balanced={balanced} (stddev: {stddev}, error {error})"
        )
        return balanced

    def _consume_all(self, topic_names: list[str], msg_count_per_topic: int,
                     timeout_per_topic: int):
        """
        Don't do anything with the messages, just consume them to demonstrate
        that doing so does not exhaust redpanda resources.
        """
        def consumer_saw_msgs(consumer):
            self.logger.info(
                f"Consumer message_count={consumer.message_count} / {msg_count_per_topic}"
            )
            # Tolerate greater-than, because if there were errors during production
            # there can have been retries.
            return consumer.message_count >= msg_count_per_topic

        for tn in topic_names:
            consumer = RpkConsumer(self._ctx,
                                   self.redpanda,
                                   tn,
                                   save_msgs=False,
                                   fetch_max_bytes=BIG_FETCH,
                                   num_msgs=msg_count_per_topic)
            consumer.start()
            wait_until(lambda: consumer_saw_msgs(consumer),
                       timeout_sec=timeout_per_topic,
                       backoff_sec=5)
            consumer.stop()
            consumer.free()

    def _repeater_worker_count(self, scale):
        workers = 32 * scale.node_cpus
        if self.redpanda.dedicated_nodes:
            # 768 workers on a 24 core node has been seen to work well.
            return workers
        else:
            return min(workers, 4)

    def nodes_report_cloud_segments(self, target_segments):
        """
        Returns true if the nodes in the cluster collectively report having
        above the given number of segments.

        NOTE: we're explicitly not checking the manifest via cloud client
        because we expect the number of items in our bucket to be quite large,
        and for associated ListObjects calls to take a long time.
        """
        num_segments = self.redpanda.metric_sum(
            "redpanda_cloud_storage_segments",
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
        self.logger.info(
            f"Cluster metrics report {num_segments} cloud segments")
        return num_segments >= target_segments

    def setUp(self):
        # defer redpanda startup to the test, it might want to tweak
        # ResourceSettings based on its parameters.
        pass

    def _get_fd_counts(self):
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(self.redpanda.nodes)) as executor:

            return list(
                executor.map(
                    lambda n: tuple(
                        [n, sum(1 for _ in self.redpanda.lsof_node(n))]),
                    self.redpanda.nodes))

    def _single_node_restart(self, scale: ScaleParameters, topic_names: list,
                             n_partitions: int):
        """
        Restart a single node to check stability through the movement of
        leadership to other nodes, plus the subsequent leader balancer
        activity to redistribute after it comes back up.
        """

        node = self.redpanda.nodes[-1]
        self.logger.info(f"Single node restart on node {node.name}")
        node_id = self.redpanda.idx(node)

        self.redpanda.stop_node(node)

        # Wait for leaderships to stabilize on the surviving nodes
        wait_until(
            lambda: self._node_leadership_evacuated(topic_names, n_partitions,
                                                    node_id), 30, 1)

        self.redpanda.start_node(node, timeout=self.EXPECT_START_TIME)

        # Heuristic: in testing we see leaderships transfer at about 10
        # per second.  2x margin for error.  Up to the leader balancer period
        # wait for it to activate.
        transfers_per_sec = 10
        expect_leader_transfer_time = 2 * (
            n_partitions / len(self.redpanda.nodes)) / transfers_per_sec + (
                self.LEADER_BALANCER_PERIOD_MS / 1000) * 2

        # Wait for leaderships to achieve balance.  This is bounded by:
        #  - Time for leader_balancer to issue+await all the transfers
        #  - Time for raft to achieve recovery, a prerequisite for
        #    leadership.
        t1 = time.time()
        wait_until(
            lambda: self._node_leadership_balanced(topic_names, n_partitions),
            expect_leader_transfer_time, 10)
        self.logger.info(
            f"Leaderships balanced in {time.time() - t1:.2f} seconds")

    def _tiered_storage_warmup(self, scale, topic_name):
        """
        When testing tiered storage, we want a realistic amount of metadata in the
        system: it takes too long to actually play in a day or week's worth of data,
        so set a very small segment size, then play in enough data to create a realistic
        number of segments.
        """

        warmup_segment_size = 32 * 1024
        warmup_message_size = 32 * 1024
        target_cloud_segments = SEGMENTS_PER_PARTITION * scale.partition_limit
        warmup_total_size = max(STRESS_DATA_SIZE,
                                target_cloud_segments * warmup_segment_size)

        # Uploads of tiny segments usually progress at a few thousand
        # per second.  This is dominated by the S3 PUT latency combined
        # with limited parallelism of connections.
        expect_upload_rate = 1000

        expect_runtime_bandwidth = (2 *
                                    warmup_total_size) / scale.expect_bandwidth
        expect_runtime_upload = (2 *
                                 target_cloud_segments) / expect_upload_rate
        expect_runtime = max(expect_runtime_upload, expect_runtime_bandwidth)

        try:
            self.logger.info(
                f"Tiered storage warmup: overriding segment size to {warmup_segment_size}"
            )
            # FIXME: this only works if we have one topic globally.  When we add admin API for
            # manifest stats, use that instead.
            self.logger.info(
                f"Tiered storage warmup: waiting {expect_runtime}s for {target_cloud_segments} to be created"
            )
            msg_count = int(warmup_total_size / warmup_message_size)
            try:
                producer = KgoVerifierProducer(
                    self.test_context,
                    self.redpanda,
                    topic_name,
                    warmup_message_size,
                    msg_count,
                    custom_node=[self.preallocated_nodes[0]])
                producer.start()
                wait_until(lambda: self.nodes_report_cloud_segments(
                    target_cloud_segments),
                           timeout_sec=expect_runtime,
                           backoff_sec=5)
                producer.stop()
                producer.wait(timeout_sec=expect_runtime)
            except:
                raise
            finally:
                self.free_preallocated_nodes()
        finally:
            self.logger.info(
                f"Tiered storage warmup: restoring segment size to {scale.segment_size}"
            )
            self.rpk.alter_topic_config(topic_name, 'segment.bytes',
                                        str(scale.segment_size_after_warmup))
            self.rpk.alter_topic_config(
                topic_name, 'retention.local.target.bytes',
                str(scale.local_retention_after_warmup))

    def _write_and_random_read(self, scale: ScaleParameters, topic_names):
        """
        This is a relatively low intensity test, that covers random
        and sequential reads & validates correctness of offsets in the
        partitions written to.

        It answers the question "is the cluster basically working properly"?  Before
        we move on to more stressful testing, and in the process ensures there
        is enough data in the system that we aren't in the "everything fits in
        memory" regime.

        Note: run this before other workloads, so that kgo-verifier's random
        readers are able to validate most of what they read (otherwise they
        will mostly be reading data written by a different workload, which
        drives traffic but is a less strict test because it can't validate
        the offsets of those messages)
        """
        # Now that we've tested basic ability to form consensus and survive some
        # restarts, move on to a more general stress test.
        self.logger.info("Entering traffic stress test")
        target_topic = topic_names[0]

        # Assume fetches will be 10MB, the franz-go default
        fetch_bytes_per_partition = 10 * 1024 * 1024

        # * Need enough data that if a consumer tried to fetch it all at once
        # in a single request, it would run out of memory.  OR the amount of
        # data that would fill a 10MB max_bytes per partition in a fetch, whichever
        # is lower (avoid writing excessive data for tests with fewer partitions).
        # * Then apply a factor of two to make sure we have enough data to drive writes
        # to disk during consumption, not just enough data to hold it all in the batch
        # cache.

        # Partitions per topic
        n_partitions = int(scale.partition_limit / len(topic_names))

        write_bytes_per_topic = min(
            int((self.redpanda.get_node_memory_mb() * 1024 * 1024) /
                len(topic_names)),
            fetch_bytes_per_partition * n_partitions) * 2

        if not self.redpanda.dedicated_nodes:
            # Docker developer mode: likely to be on a workstation with lots of RAM
            # and we don't want to wait to fill it all up.
            write_bytes_per_topic = int(1E9 / len(topic_names))

        msg_size = 128 * 1024
        msg_count_per_topic = int((write_bytes_per_topic / msg_size))

        # Approx time to write or read all messages, for timeouts
        # Pessimistic bandwidth guess, accounting for the sub-disk bandwidth
        # that a single-threaded consumer may see

        expect_transmit_time = int(write_bytes_per_topic /
                                   scale.expect_single_bandwidth)
        expect_transmit_time = max(expect_transmit_time, 30)

        for tn in topic_names:
            t1 = time.time()
            producer = KgoVerifierProducer(
                self.test_context,
                self.redpanda,
                tn,
                msg_size,
                msg_count_per_topic,
                custom_node=[self.preallocated_nodes[0]])
            producer.start()
            producer.wait(timeout_sec=expect_transmit_time)
            self.free_preallocated_nodes()
            duration = time.time() - t1
            self.logger.info(
                f"Wrote {write_bytes_per_topic} bytes to {tn} in {duration}s, bandwidth {(write_bytes_per_topic / duration)/(1024 * 1024)}MB/s"
            )

        stress_msg_size = 32768
        stress_data_size = STRESS_DATA_SIZE

        if not self.redpanda.dedicated_nodes:
            stress_data_size = 2E9

        stress_msg_count = int(stress_data_size / stress_msg_size)
        fast_producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            target_topic,
            stress_msg_size,
            stress_msg_count,
            custom_node=[self.preallocated_nodes[0]])
        fast_producer.start()

        # Don't start consumers until the producer has written out its first
        # checkpoint with valid ranges.
        wait_until(lambda: fast_producer.produce_status.acked > 0,
                   timeout_sec=30,
                   backoff_sec=1.0)

        rand_ios = 100
        rand_parallel = 100
        if self.redpanda.dedicated_nodes:
            rand_parallel = 10
            rand_ios = 10

        rand_consumer = KgoVerifierRandomConsumer(
            self.test_context,
            self.redpanda,
            target_topic,
            msg_size=0,
            rand_read_msgs=rand_ios,
            parallel=rand_parallel,
            nodes=[self.preallocated_nodes[1]])
        rand_consumer.start(clean=False)
        rand_consumer.wait()

        fast_producer.stop()
        fast_producer.wait()
        self.logger.info(
            "Write+randread stress test complete, verifying sequentially")

        # When tiered storage is enabled, don't consume the entire topic, as
        # that could entail millions of segments from the cloud.
        max_msgs = None
        if scale.tiered_storage_enabled:
            max_msgs = 10000

        seq_consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            target_topic,
            0,
            nodes=[self.preallocated_nodes[2]])
        seq_consumer.start(clean=False)

        seq_consumer.wait()
        assert seq_consumer.consumer_status.validator.invalid_reads == 0
        if not scale.tiered_storage_enabled:
            assert seq_consumer.consumer_status.validator.valid_reads >= fast_producer.produce_status.acked + msg_count_per_topic, \
                f"{seq_consumer.consumer_status.validator.valid_reads} >= {fast_producer.produce_status.acked} + {msg_count_per_topic}"

        self.free_preallocated_nodes()

    @cluster(num_nodes=ALLOC_NODES, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_stability(self):
        self._test_stability(compacted=False, tiered_storage_enabled=True)

    def _test_stability(self, compacted, tiered_storage_enabled=False):
        # Scale tests are not run on debug builds
        assert not self.debug_mode

        replication_factor = 3

        scale = ScaleParameters(self.redpanda,
                                replication_factor,
                                tiered_storage_enabled=tiered_storage_enabled)

        # Run with one huge topic: it is more stressful for redpanda when clients
        # request the metadata for many partitions at once, and the simplest way
        # to get traffic generators to do that without the clients supporting
        # writing to arrays of topics is to put all the partitions into one topic.
        n_topics = 1

        # Partitions per topic
        n_partitions = int(scale.partition_limit / n_topics)

        self.logger.info(
            f"Running partition scale test with {n_partitions} partitions on {n_topics} topics"
        )
        if scale.si_settings:
            self.redpanda.set_si_settings(scale.si_settings)

        # Enable large node-wide thoughput limits to verify they work at scale
        # To avoid affecting the result of the test with the limit, set them
        # somewhat above expect_bandwidth value per node
        self.redpanda.add_extra_rp_conf({
            'kafka_throughput_limit_node_in_bps':
            int(scale.expect_bandwidth / len(self.redpanda.nodes) * 3),
            'kafka_throughput_limit_node_out_bps':
            int(scale.expect_bandwidth / len(self.redpanda.nodes) * 3)
        })

        self.redpanda.start(parallel=True)

        self.logger.info("Entering topic creation")
        topic_names = [f"scale_{i:06d}" for i in range(0, n_topics)]
        for tn in topic_names:
            self.logger.info(
                f"Creating topic {tn} with {n_partitions} partitions")
            config = {
                'segment.bytes': scale.segment_size,
                'retention.bytes': scale.retention_bytes
            }
            if scale.local_retention_bytes:
                config[
                    'retention.local.target.bytes'] = scale.local_retention_bytes

            if compacted:
                if tiered_storage_enabled:
                    config['cleanup.policy'] = 'compact,delete'
                else:
                    config['cleanup.policy'] = 'compact'
            else:
                config['cleanup.policy'] = 'delete'

            self.rpk.create_topic(tn,
                                  partitions=n_partitions,
                                  replicas=replication_factor,
                                  config=config)

        self.logger.info(f"Awaiting elections...")
        wait_until(lambda: self._all_elections_done(topic_names, n_partitions),
                   timeout_sec=60,
                   backoff_sec=5)
        self.logger.info(f"Initial elections done.")

        for node_name, file_count in self._get_fd_counts():
            self.logger.info(
                f"Open files after initial elections on {node_name}: {file_count}"
            )

        if scale.tiered_storage_enabled:
            self.logger.info("Entering tiered storage warmup")
            for tn in topic_names:
                self._tiered_storage_warmup(scale, tn)

        self.logger.info(
            "Entering initial traffic test, writes + random reads")
        self._write_and_random_read(scale, topic_names)

        # Start kgo-repeater

        repeater_kwargs = {}
        if compacted:
            # Each parititon gets roughly 10 unique keys, after which
            # compaction should kick in.
            repeater_kwargs['key_count'] = int(scale.partition_limit * 10)
        else:
            # Not doing compaction, doesn't matter how big the keyspace
            # is, use whole 32 bit range to get best possible distribution
            # across partitions.
            repeater_kwargs['key_count'] = 2**32

        # Main test phase: with continuous background traffic, exercise restarts and
        # any other cluster changes that might trip up at scale.
        repeater_msg_size = 16384
        max_buffered_records = 64
        if scale.tiered_storage_enabled:
            max_buffered_records = 1
        with repeater_traffic(context=self._ctx,
                              redpanda=self.redpanda,
                              nodes=self.preallocated_nodes,
                              topic=topic_names[0],
                              msg_size=repeater_msg_size,
                              workers=self._repeater_worker_count(scale),
                              max_buffered_records=max_buffered_records,
                              cleanup=lambda: self.free_preallocated_nodes(),
                              **repeater_kwargs) as repeater:
            repeater_await_bytes = 1E9
            if scale.tiered_storage_enabled or not self.dedicated_nodes:
                # Be much more lenient when tiered storage is enabled, since
                # the repeater incurs reads.
                repeater_await_bytes = 1E8
            repeater_await_msgs = int(repeater_await_bytes / repeater_msg_size)

            def progress_check():
                # Explicit wait for consumer group, because we might have e.g.
                # just restarted the cluster, and don't want to include that
                # delay in our throughput-driven timeout expectations
                self.logger.info(f"Checking repeater group is ready...")
                repeater.await_group_ready()

                t = repeater_await_bytes / scale.expect_bandwidth
                self.logger.info(
                    f"Waiting for {repeater_await_msgs} messages in {t} seconds"
                )
                t1 = time.time()
                repeater.await_progress(repeater_await_msgs, t)
                t2 = time.time()

                # This is approximate, because await_progress isn't returning the very
                # instant the workers hit their collective target.
                self.logger.info(
                    f"Wait complete, approx bandwidth {(repeater_await_bytes / (t2-t1))/(1024*1024.0)}MB/s"
                )

            progress_check()

            self.logger.info(f"Entering single node restart phase")
            self._single_node_restart(scale, topic_names, n_partitions)
            progress_check()

            self.logger.info(
                f"Post-restarts: checking repeater group is ready...")
            repeater.await_group_ready()

            # Done with restarts, now do a longer traffic soak
            self.logger.info(f"Entering traffic soak phase")

            # Normalize by the max_buffered_records.
            soak_await_bytes = int(100E9 / 64 * max_buffered_records)
            if not self.redpanda.dedicated_nodes:
                soak_await_bytes = 10E9

            soak_await_msgs = soak_await_bytes / repeater_msg_size
            t1 = time.time()
            initial_p, _ = repeater.total_messages()
            try:
                repeater.await_progress(
                    soak_await_msgs, soak_await_bytes / scale.expect_bandwidth)
            except TimeoutError:
                t2 = time.time()
                final_p, _ = repeater.total_messages()
                bytes_sent = (final_p - initial_p) * repeater_msg_size
                expect_mbps = scale.expect_bandwidth / (1024 * 1024.0)
                actual_mbps = (bytes_sent / (t2 - t1)) / (1024 * 1024.0)
                self.logger.error(
                    f"Expected throughput {expect_mbps:.2f}, got throughput {actual_mbps:.2f}MB/s"
                )
                raise
