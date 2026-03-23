# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import os
from typing import Any
from unittest import TestCase
from unittest.mock import MagicMock, patch

import pytest


def _load_fixture() -> bytes:
    fixture_path = os.path.join(os.path.dirname(__file__), "testdata", "cloudwatch_metrics_stream.json")
    with open(fixture_path, "rb") as f:
        return f.read()


def _make_sqs_record_body(bucket_name: str = "test-bucket", object_key: str = "metrics/2024/01/15/data.json") -> dict[str, Any]:
    return {
        "Records": [
            {
                "awsRegion": "us-east-1",
                "eventTime": "2024-01-15T10:30:00.000Z",
                "s3": {
                    "bucket": {
                        "name": bucket_name,
                        "arn": f"arn:aws:s3:::{bucket_name}",
                    },
                    "object": {"key": object_key},
                },
            }
        ]
    }


@pytest.mark.unit
class TestHandleCloudwatchMetricsEvent(TestCase):
    @patch("handlers.aws.cloudwatch_metrics_trigger.StorageFactory")
    def test_yields_events_for_each_metric_line(self, mock_storage_factory: MagicMock) -> None:
        from handlers.aws.cloudwatch_metrics_trigger import _handle_cloudwatch_metrics_event

        fixture_data = _load_fixture()
        mock_storage = MagicMock()
        lines = [line for line in fixture_data.split(b"\n") if line.strip()]
        byte_offset = 0
        events_from_storage = []
        for line in lines:
            events_from_storage.append((line, byte_offset, byte_offset + len(line), None))
            byte_offset = byte_offset + len(line) + 1
        mock_storage.get_by_lines.return_value = iter(events_from_storage)
        mock_storage_factory.create.return_value = mock_storage

        sqs_record_body = _make_sqs_record_body()
        events = list(
            _handle_cloudwatch_metrics_event(
                sqs_record_body=sqs_record_body,
                input_id="arn:aws:sqs:us-east-1:123456789012:cw-metrics",
            )
        )

        # 3 valid metric lines, 1 malformed line skipped
        assert len(events) == 3

    @patch("handlers.aws.cloudwatch_metrics_trigger.StorageFactory")
    def test_ec2_event_has_correct_structure(self, mock_storage_factory: MagicMock) -> None:
        from handlers.aws.cloudwatch_metrics_trigger import _handle_cloudwatch_metrics_event

        ec2_line = b'{"metric_stream_name":"test-stream","account_id":"123456789012","region":"us-east-1","namespace":"AWS/EC2","metric_name":"CPUUtilization","dimensions":{"InstanceId":"i-1234"},"timestamp":1705312200000,"value":{"max":95.0,"min":10.0,"sum":500.0,"count":10.0},"unit":"Percent"}'
        mock_storage = MagicMock()
        mock_storage.get_by_lines.return_value = iter([(ec2_line, 0, len(ec2_line), None)])
        mock_storage_factory.create.return_value = mock_storage

        sqs_record_body = _make_sqs_record_body()
        events = list(
            _handle_cloudwatch_metrics_event(
                sqs_record_body=sqs_record_body,
                input_id="arn:aws:sqs:us-east-1:123456789012:cw-metrics",
            )
        )

        assert len(events) == 1
        es_event, ending_offset, event_expanded_offset, s3_record_n = events[0]

        assert "@timestamp" in es_event
        assert es_event["fields"]["aws"]["ec2"]["metrics"]["CPUUtilization"]["avg"] == 50.0
        assert es_event["fields"]["cloud"]["provider"] == "aws"
        assert es_event["fields"]["cloud"]["region"] == "us-east-1"
        assert es_event["meta"]["data_stream"] == "metrics-aws.ec2_metrics-default"
        assert es_event["meta"]["pipeline"] == "metrics-aws.ec2_metrics"
        assert len(es_event["fields"]["message"]) > 0
        assert s3_record_n == 0

    @patch("handlers.aws.cloudwatch_metrics_trigger.StorageFactory")
    def test_unknown_namespace_uses_base_mapper(self, mock_storage_factory: MagicMock) -> None:
        from handlers.aws.cloudwatch_metrics_trigger import _handle_cloudwatch_metrics_event

        unknown_line = b'{"metric_stream_name":"test","account_id":"123","region":"us-east-1","namespace":"AWS/Unknown","metric_name":"Foo","dimensions":{},"timestamp":1705312200000,"value":{"max":1.0,"min":0.0,"sum":5.0,"count":10.0},"unit":"None"}'
        mock_storage = MagicMock()
        mock_storage.get_by_lines.return_value = iter([(unknown_line, 0, len(unknown_line), None)])
        mock_storage_factory.create.return_value = mock_storage

        sqs_record_body = _make_sqs_record_body()
        events = list(
            _handle_cloudwatch_metrics_event(
                sqs_record_body=sqs_record_body,
                input_id="arn:aws:sqs:us-east-1:123456789012:cw-metrics",
            )
        )

        assert len(events) == 1
        es_event = events[0][0]
        assert es_event["meta"]["data_stream"] == "metrics-aws.cloudwatch_metrics-default"

    @patch("handlers.aws.cloudwatch_metrics_trigger.StorageFactory")
    def test_yield_tuple_shape(self, mock_storage_factory: MagicMock) -> None:
        from handlers.aws.cloudwatch_metrics_trigger import _handle_cloudwatch_metrics_event

        line = b'{"metric_stream_name":"test","account_id":"123","region":"us-east-1","namespace":"AWS/EC2","metric_name":"CPUUtilization","dimensions":{},"timestamp":1705312200000,"value":{"max":1.0,"min":0.0,"sum":1.0,"count":1.0},"unit":"Percent"}'
        mock_storage = MagicMock()
        mock_storage.get_by_lines.return_value = iter([(line, 0, 100, None)])
        mock_storage_factory.create.return_value = mock_storage

        sqs_record_body = _make_sqs_record_body()
        events = list(
            _handle_cloudwatch_metrics_event(
                sqs_record_body=sqs_record_body,
                input_id="arn:aws:sqs:us-east-1:123456789012:cw-metrics",
            )
        )

        assert len(events) == 1
        assert len(events[0]) == 4
        es_event, ending_offset, event_expanded_offset, s3_record_n = events[0]
        assert isinstance(es_event, dict)
        assert ending_offset == 100
        assert event_expanded_offset is None
        assert s3_record_n == 0
