# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from unittest import TestCase

import pytest


@pytest.mark.unit
class TestEC2Mapper(TestCase):
    def _make_metric_record(self, **overrides):
        record = {
            "metric_stream_name": "test-stream",
            "account_id": "123456789012",
            "region": "us-east-1",
            "namespace": "AWS/EC2",
            "metric_name": "CPUUtilization",
            "dimensions": {"InstanceId": "i-1234567890abcdef0"},
            "timestamp": 1705312200000,
            "value": {"max": 100.0, "min": 10.0, "sum": 550.0, "count": 10.0},
            "unit": "Percent",
        }
        record.update(overrides)
        return record

    def test_service_key(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.ec2 import EC2Mapper

        mapper = EC2Mapper()
        assert mapper.get_service_key() == "ec2"

    def test_data_stream_name(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.ec2 import EC2Mapper

        mapper = EC2Mapper()
        assert mapper.get_data_stream_name() == "metrics-aws.ec2_metrics-default"

    def test_registry_lookup(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers import get_mapper
        from handlers.aws.cloudwatch_metrics_mappers.ec2 import EC2Mapper

        mapper = get_mapper("AWS/EC2")
        assert isinstance(mapper, EC2Mapper)

    def test_transform(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.ec2 import EC2Mapper

        mapper = EC2Mapper()
        record = self._make_metric_record()
        event = mapper.transform(record, s3_file_path="s3://bucket/key", byte_offset=0)

        assert event["@timestamp"] == "2024-01-15T09:50:00.000Z"
        assert event["fields"]["aws"]["ec2"]["metrics"]["CPUUtilization"]["max"] == 100.0
        assert event["fields"]["aws"]["ec2"]["metrics"]["CPUUtilization"]["avg"] == 55.0
        assert event["fields"]["aws"]["cloudwatch"]["namespace"] == "AWS/EC2"
        assert event["meta"]["data_stream"] == "metrics-aws.ec2_metrics-default"
        assert event["meta"]["pipeline"] == "metrics-aws.ec2_metrics"
        assert event["meta"]["data_stream_dataset"] == "aws.ec2_metrics"
