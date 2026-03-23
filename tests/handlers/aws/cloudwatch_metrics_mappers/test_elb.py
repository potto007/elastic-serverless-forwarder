# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from unittest import TestCase

import pytest


@pytest.mark.unit
class TestELBMapper(TestCase):
    def _make_metric_record(self, **overrides):
        record = {
            "metric_stream_name": "test-stream",
            "account_id": "123456789012",
            "region": "us-east-1",
            "namespace": "AWS/ApplicationELB",
            "metric_name": "RequestCount",
            "dimensions": {"LoadBalancer": "app/my-alb/1234567890"},
            "timestamp": 1705312200000,
            "value": {"max": 100.0, "min": 10.0, "sum": 550.0, "count": 10.0},
            "unit": "Count",
        }
        record.update(overrides)
        return record

    def test_service_key(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.elb import ELBMapper

        mapper = ELBMapper()
        assert mapper.get_service_key() == "elb"

    def test_data_stream_name(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.elb import ELBMapper

        mapper = ELBMapper()
        assert mapper.get_data_stream_name() == "metrics-aws.elb_metrics-default"

    def test_registry_lookup(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers import get_mapper
        from handlers.aws.cloudwatch_metrics_mappers.elb import ELBMapper

        mapper = get_mapper("AWS/ApplicationELB")
        assert isinstance(mapper, ELBMapper)

    def test_transform(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.elb import ELBMapper

        mapper = ELBMapper()
        record = self._make_metric_record()
        event = mapper.transform(record, s3_file_path="s3://bucket/key", byte_offset=0)

        assert event["@timestamp"] == "2024-01-15T09:50:00.000Z"
        assert event["fields"]["aws"]["elb"]["metrics"]["RequestCount"]["max"] == 100.0
        assert event["fields"]["aws"]["elb"]["metrics"]["RequestCount"]["avg"] == 55.0
        assert event["fields"]["aws"]["cloudwatch"]["namespace"] == "AWS/ApplicationELB"
        assert event["meta"]["data_stream"] == "metrics-aws.elb_metrics-default"
        assert event["meta"]["pipeline"] == "metrics-aws.elb_metrics"
        assert event["meta"]["data_stream_dataset"] == "aws.elb_metrics"
