# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import hashlib
import random
import time
from uuid import uuid4, UUID
import requests
from ducktape.cluster.cluster import ClusterNode
from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from rptest.services.admin import Admin, DebugBundleStartConfig, DebugBundleStartConfigParams
from rptest.services.cluster import cluster
from rptest.services.redpanda import LoggingConfig, MetricSamples, MetricsEndpoint
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until_result


class DebugBundleErrorCode:
    SUCCESS = 0
    DEBUG_BUNDLE_PROCESS_RUNNING = 1
    DEBUG_BUNDLE_PROCESS_NOT_RUNNING = 2
    INVALID_PARAMETERS = 3
    PROCESS_FAILED = 4
    INTERNAL_ERROR = 5
    JOB_ID_NOT_RECOGNIZED = 6
    DEBUG_BUNDLE_PROCESS_NEVER_STARTED = 7
    RPK_BINARY_NOT_PRESENT = 8
    DEBUG_BUNDLE_EXPIRED = 9


log_config = LoggingConfig('info',
                           logger_levels={
                               'admin_api_server': 'trace',
                               'debug-bundle-service': 'trace'
                           })


class DebugBundleTest(RedpandaTest):
    debug_bundle_dir_config = "debug_bundle_storage_dir"
    metrics = [
        "last_successful_bundle_timestamp_seconds",
        "last_failed_bundle_timestamp_seconds",
        "successful_generation_count",
        "failed_generation_count",
    ]
    """
    Smoke test for debug bundle admin API
    """
    def __init__(self, context, num_brokers=1, **kwargs) -> None:
        super(DebugBundleTest, self).__init__(context,
                                              num_brokers=num_brokers,
                                              log_config=log_config,
                                              **kwargs)

        self.admin = Admin(self.redpanda)

    def _get_sha256sum(self, node, file):
        cap = node.account.ssh_capture(f"sha256sum -b {file}",
                                       allow_fail=False)
        return "".join(cap).strip().split(maxsplit=1)[0]

    def _get_metrics_from_node(
        self,
        node: ClusterNode,
        endpoint: MetricsEndpoint = MetricsEndpoint.METRICS
    ) -> dict[str, MetricSamples]:
        def get_metrics_from_node_sync(patterns: list[str]):
            samples = self.redpanda.metrics_samples(patterns, [node], endpoint)
            success = samples is not None and set(
                samples.keys()) == set(patterns)
            return success, samples

        samples = wait_until_result(
            lambda: get_metrics_from_node_sync(self.metrics),
            timeout_sec=2,
            backoff_sec=.1,
            err_msg="Failed to fetch metrics from node")
        assert samples, f"Missing expected metrics from node {node.name}"
        assert set(samples.keys()) == set(
            self.metrics), f"Missing expected metrics from node {node.name}"
        return samples

    def _assert_http_error(self, expected_status_code: int,
                           expected_error_code: DebugBundleErrorCode, request,
                           *args, **kwargs):
        try:
            request(*args, **kwargs)
            assert False, f"Expected HTTPError with status code {expected_status_code}"
        except requests.HTTPError as e:
            json = e.response.json()
            assert e.response.status_code == expected_status_code, json
            assert json["code"] == expected_error_code, json

    @cluster(num_nodes=1)
    @matrix(ignore_none=[True, False])
    def test_post_debug_bundle(self, ignore_none: bool):
        """
        Smoke test for the debug bundle.  Verifies behavior of endpoints depending
        on certain states of the bundle service.
        """
        node = random.choice(self.redpanda.started_nodes())

        job_id = uuid4()
        res = self.admin.post_debug_bundle(DebugBundleStartConfig(
            job_id=job_id,
            config=DebugBundleStartConfigParams(cpu_profiler_wait_seconds=16,
                                                metrics_interval_seconds=16)),
                                           ignore_none=ignore_none,
                                           node=node)

        assert res.status_code == requests.codes.ok, res.json()

        # Start a second debug bundle with the same job_id, expect a conflict
        self._assert_http_error(
            requests.codes.conflict,
            DebugBundleErrorCode.DEBUG_BUNDLE_PROCESS_RUNNING,
            self.admin.post_debug_bundle,
            config=DebugBundleStartConfig(job_id=job_id,
                                          config=DebugBundleStartConfigParams(
                                              cpu_profiler_wait_seconds=16,
                                              metrics_interval_seconds=16)),
            node=node)

        # Get the debug bundle status, expect running
        res = self.admin.get_debug_bundle(node=node)
        assert res.status_code == requests.codes.ok, res.json()
        assert res.json()['status'] == 'running', res.json()
        assert res.json()['job_id'] == str(job_id), res.json()

        # Wait until the debug bundle has completed
        try:
            wait_until(lambda: self.admin.get_debug_bundle(node=node).json()[
                'status'] != 'running',
                       timeout_sec=60,
                       backoff_sec=1)
        except Exception as e:
            self.redpanda.logger.warning(
                f"response: {self.admin.get_debug_bundle(node=node).json()}")
            raise e

        # Get the debug bundle status, expect success
        res = self.admin.get_debug_bundle(node=node)
        assert res.status_code == requests.codes.ok, res.json()
        assert res.json()['status'] == 'success', res.json()
        assert res.json()['job_id'] == str(job_id), res.json()
        filename = res.json()['filename']
        assert filename == f"{job_id}.zip", res.json()

        # Delete the debug bundle after it has completed
        self._assert_http_error(
            requests.codes.conflict,
            DebugBundleErrorCode.DEBUG_BUNDLE_PROCESS_NOT_RUNNING,
            self.admin.delete_debug_bundle,
            job_id=job_id,
            node=node)

        res = self.admin.get_debug_bundle_file(filename=filename, node=node)
        assert res.status_code == requests.codes.ok, res.json()
        assert res.headers['Content-Type'] == 'application/zip', res.json()
        data_dir = None
        try:
            data_dir = self.admin.get_cluster_config(
                node=node,
                key=self.debug_bundle_dir_config)[self.debug_bundle_dir_config]
        except requests.HTTPError as e:
            pass

        data_dir = data_dir or self.admin.get_node_config(
            node=node)['data_directory']['data_directory'] + "/debug-bundle"

        file = f"{data_dir}/{filename}"
        assert self._get_sha256sum(node, file) == hashlib.sha256(
            res.content).hexdigest()

        # Delete the debug bundle file
        res = self.admin.delete_debug_bundle_file(filename=filename, node=node)
        assert res.status_code == requests.codes.no_content, res.json()
        assert res.headers['Content-Type'] == 'application/json', res.json()
        assert not node.account.exists(file)

        # Get the non-existant debug bundle
        self._assert_http_error(
            requests.codes.conflict,
            DebugBundleErrorCode.DEBUG_BUNDLE_PROCESS_NEVER_STARTED,
            self.admin.get_debug_bundle,
            node=node)

        # Cancel the non-existant debug bundle
        self._assert_http_error(
            requests.codes.conflict,
            DebugBundleErrorCode.DEBUG_BUNDLE_PROCESS_NEVER_STARTED,
            self.admin.delete_debug_bundle,
            job_id=job_id,
            node=node)

        # Get the non-existant debug bundle file
        self._assert_http_error(
            requests.codes.conflict,
            DebugBundleErrorCode.DEBUG_BUNDLE_PROCESS_NEVER_STARTED,
            self.admin.get_debug_bundle_file,
            filename=filename,
            node=node)

        # Delete the debug bundle file again
        self._assert_http_error(
            requests.codes.conflict,
            DebugBundleErrorCode.DEBUG_BUNDLE_PROCESS_NEVER_STARTED,
            self.admin.delete_debug_bundle_file,
            filename=filename,
            node=node)

    def _run_debug_bundle(self, job_id: UUID, node: ClusterNode,
                          cancel_after_start: bool):
        res = self.admin.post_debug_bundle(
            DebugBundleStartConfig(job_id=job_id), node=node)
        assert res.status_code == requests.codes.ok, f"Failed to start debug bundle: {res.json()}"

        # Wait until the debug bundle is running
        try:
            wait_until(
                lambda: self.admin.get_debug_bundle(node=node).json()[
                    'status'] == 'running',
                timeout_sec=120,
                backoff_sec=1,
                err_msg="Timed out waiting for debug bundle process to start")
        except Exception as e:
            self.redpanda.logger.warning(
                f"response: {self.admin.get_debug_bundle(node=node).json()}")
            raise e

        if cancel_after_start:
            res = self.admin.delete_debug_bundle(job_id=job_id, node=node)
            assert res.status_code == requests.codes.no_content, f"Failed to cancel debug bundle: {res.json()}"

        expected_status = 'error' if cancel_after_start else 'success'

        # Wait until the debug bundle has completed
        try:
            wait_until(
                lambda: self.admin.get_debug_bundle(node=node).json()[
                    'status'] == expected_status,
                timeout_sec=120,
                backoff_sec=1,
                err_msg=
                f"Timed out waiting for debug bundle process to enter {expected_status} state"
            )
        except Exception as e:
            self.redpanda.logger.warning(
                f"response: {self.admin.get_debug_bundle(node=node).json()}")
            raise e

    @cluster(num_nodes=1)
    def test_debug_bundle_metrics(self):
        """
        This test verifies the functionality of the debug bundle metrics
        """
        node = random.choice(self.redpanda.started_nodes())

        job_id = uuid4()
        self._run_debug_bundle(job_id=job_id,
                               node=node,
                               cancel_after_start=False)
        success_generation_time = int(time.time())

        samples = self._get_metrics_from_node(node)
        assert samples['successful_generation_count'].samples[
            0].value == 1, f"Expected 1 successful generation, got {samples['successful_generation_count'].samples[0].value}"
        assert success_generation_time - 2 <= samples[
            'last_successful_bundle_timestamp_seconds'].samples[
                0].value <= success_generation_time + 2, f"Expected to see generation time {samples['last_successful_bundle_timestamp_seconds'].samples[0].value} within 2 seconds of {success_generation_time}"
        assert samples['failed_generation_count'].samples[
            0].value == 0, f"Expected 0 failed generation, got {samples['failed_generation_count'].samples[0].value}"
        assert samples['last_failed_bundle_timestamp_seconds'].samples[
            0].value == 0, f"Expected 0 failed generation, got {samples['last_failed_bundle_timestamp_seconds'].samples[0].value}"

        self._run_debug_bundle(job_id=job_id,
                               node=node,
                               cancel_after_start=True)
        failed_generation_time = int(time.time())

        samples = self._get_metrics_from_node(node)
        assert samples['successful_generation_count'].samples[
            0].value == 1, f"Expected 1 successful generation, got {samples['successful_generation_count'].samples[0].value}"
        assert success_generation_time - 2 <= samples[
            'last_successful_bundle_timestamp_seconds'].samples[
                0].value <= success_generation_time + 2, f"Expected to see generation time {samples['last_successful_bundle_timestamp_seconds'].samples[0].value} within 2 seconds of {success_generation_time}"
        assert samples['failed_generation_count'].samples[
            0].value == 1, f"Expected 0 failed generation, got {samples['failed_generation_count'].samples[0].value}"
        assert failed_generation_time - 2 <= samples[
            'last_failed_bundle_timestamp_seconds'].samples[
                0].value <= failed_generation_time + 2, f"Expected to see failed generation {samples['last_failed_bundle_timestamp_seconds'].samples[0].value} within 2 seconds of {failed_generation_time}"

    @cluster(num_nodes=1)
    @matrix(fail_bundle_generation=[True, False])
    def test_debug_bundle_metrics_restart(self, fail_bundle_generation: bool):
        """
        This test verifies that after restarting a node after a run of the debug bundle,
        the metrics reflect the last state of the debug bundle service
        """
        node = random.choice(self.redpanda.started_nodes())
        job_id = uuid4()

        self._run_debug_bundle(job_id=job_id,
                               node=node,
                               cancel_after_start=fail_bundle_generation)
        finished_time = int(time.time())

        self.redpanda.restart_nodes(self.redpanda.nodes)

        samples = self._get_metrics_from_node(node)

        if fail_bundle_generation:
            expected_successful_generation_count = 0
            expected_failed_generation_count = 1
            expected_last_failed_bundle_timestamp = finished_time
            expected_last_successful_bundle_timestamp = 0
        else:
            expected_successful_generation_count = 1
            expected_failed_generation_count = 0
            expected_last_failed_bundle_timestamp = 0
            expected_last_successful_bundle_timestamp = finished_time

        assert samples['successful_generation_count'].samples[
            0].value == expected_successful_generation_count, f"Expected {expected_successful_generation_count} successful generation, got {samples['successful_generation_count'].samples[0].value}"
        assert samples['failed_generation_count'].samples[
            0].value == expected_failed_generation_count, f"Expected {expected_failed_generation_count} failed generation, got {samples['failed_generation_count'].samples[0].value}"
        assert expected_last_failed_bundle_timestamp - 2 <= samples[
            'last_failed_bundle_timestamp_seconds'].samples[
                0].value <= expected_last_failed_bundle_timestamp + 2, f"Expected to see failed generation {samples['last_failed_bundle_timestamp_seconds'].samples[0].value} within 2 seconds of {expected_last_failed_bundle_timestamp}"
        assert expected_last_successful_bundle_timestamp - 2 <= samples[
            'last_successful_bundle_timestamp_seconds'].samples[
                0].value <= expected_last_successful_bundle_timestamp + 2, f"Expected to see generation time {samples['last_successful_bundle_timestamp_seconds'].samples[0].value} within 2 seconds of {expected_last_successful_bundle_timestamp}"

    @cluster(num_nodes=1)
    def test_delete_cancelled_job(self):
        """
        This test verifies that after a bundle job has been cancelled,
        it can be cleaned up by deleting the file and subsequent requests
        to get the debug bundle status will return a 409
        """
        node = random.choice(self.redpanda.started_nodes())
        job_id = uuid4()

        self._run_debug_bundle(job_id=job_id,
                               node=node,
                               cancel_after_start=True)

        filename = f'{str(job_id)}.zip'
        # Delete the debug bundle after it has been cancelled
        self.admin.delete_debug_bundle_file(filename=filename, node=node)
        self._assert_http_error(
            requests.codes.conflict,
            DebugBundleErrorCode.DEBUG_BUNDLE_PROCESS_NEVER_STARTED,
            self.admin.get_debug_bundle,
            node=node)
