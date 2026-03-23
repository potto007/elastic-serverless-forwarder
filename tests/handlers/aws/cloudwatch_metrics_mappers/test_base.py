# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from unittest import TestCase

import pytest


@pytest.mark.unit
class TestBaseMapper(TestCase):
    def _make_metric_record(self, **overrides):
        record = {
            "metric_stream_name": "test-stream",
            "account_id": "123456789012",
            "region": "us-east-1",
            "namespace": "AWS/Unknown",
            "metric_name": "TestMetric",
            "dimensions": {"DimKey": "DimValue"},
            "timestamp": 1705312200000,
            "value": {"max": 100.0, "min": 10.0, "sum": 550.0, "count": 10.0},
            "unit": "Count",
        }
        record.update(overrides)
        return record

    def test_transform_timestamp(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.base import BaseMapper
        mapper = BaseMapper()
        record = self._make_metric_record()
        event = mapper.transform(record, s3_file_path="s3://bucket/key", byte_offset=0)
        assert event["@timestamp"] == "2024-01-15T09:50:00.000Z"

    def test_transform_cloud_fields(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.base import BaseMapper
        mapper = BaseMapper()
        record = self._make_metric_record()
        event = mapper.transform(record, s3_file_path="s3://bucket/key", byte_offset=0)
        cloud = event["fields"]["cloud"]
        assert cloud["provider"] == "aws"
        assert cloud["region"] == "us-east-1"
        assert cloud["account"]["id"] == "123456789012"

    def test_transform_dimensions(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.base import BaseMapper
        mapper = BaseMapper()
        record = self._make_metric_record(dimensions={"InstanceId": "i-1234", "ImageId": "ami-5678"})
        event = mapper.transform(record, s3_file_path="s3://bucket/key", byte_offset=0)
        dims = event["fields"]["aws"]["dimensions"]
        assert dims["InstanceId"] == "i-1234"
        assert dims["ImageId"] == "ami-5678"

    def test_transform_namespace(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.base import BaseMapper
        mapper = BaseMapper()
        record = self._make_metric_record(namespace="AWS/EC2")
        event = mapper.transform(record, s3_file_path="s3://bucket/key", byte_offset=0)
        assert event["fields"]["aws"]["cloudwatch"]["namespace"] == "AWS/EC2"

    def test_transform_metric_values(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.base import BaseMapper
        mapper = BaseMapper()
        record = self._make_metric_record(value={"max": 100.0, "min": 10.0, "sum": 550.0, "count": 10.0})
        event = mapper.transform(record, s3_file_path="s3://bucket/key", byte_offset=0)
        metrics = event["fields"]["aws"]["cloudwatch"]["metrics"]["TestMetric"]
        assert metrics["max"] == 100.0
        assert metrics["min"] == 10.0
        assert metrics["sum"] == 550.0
        assert metrics["avg"] == 55.0

    def test_transform_avg_zero_count(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.base import BaseMapper
        mapper = BaseMapper()
        record = self._make_metric_record(value={"max": 0.0, "min": 0.0, "sum": 0.0, "count": 0.0})
        event = mapper.transform(record, s3_file_path="s3://bucket/key", byte_offset=0)
        metrics = event["fields"]["aws"]["cloudwatch"]["metrics"]["TestMetric"]
        assert metrics["avg"] == 0.0

    def test_transform_message_field(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.base import BaseMapper
        mapper = BaseMapper()
        record = self._make_metric_record()
        event = mapper.transform(record, s3_file_path="s3://bucket/key", byte_offset=0)
        assert len(event["fields"]["message"]) > 0
        assert "TestMetric" in event["fields"]["message"]

    def test_transform_log_offset(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.base import BaseMapper
        mapper = BaseMapper()
        record = self._make_metric_record()
        event = mapper.transform(record, s3_file_path="s3://bucket/key", byte_offset=42)
        assert event["fields"]["log"]["offset"] == 42
        assert event["fields"]["log"]["file"]["path"] == "s3://bucket/key"

    def test_transform_meta_data_stream(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.base import BaseMapper
        mapper = BaseMapper()
        record = self._make_metric_record()
        event = mapper.transform(record, s3_file_path="s3://bucket/key", byte_offset=0)
        meta = event["meta"]
        assert meta["data_stream"] == "metrics-aws.cloudwatch_metrics-default"
        assert meta["data_stream_type"] == "metrics"
        assert meta["data_stream_dataset"] == "aws.cloudwatch_metrics"
        assert meta["data_stream_namespace"] == "default"

    def test_get_data_stream_name(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.base import BaseMapper
        mapper = BaseMapper()
        assert mapper.get_data_stream_name() == "metrics-aws.cloudwatch_metrics-default"

    def test_get_pipeline_name(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.base import BaseMapper
        mapper = BaseMapper()
        assert mapper.get_pipeline_name() == "metrics-aws.cloudwatch_metrics"

    def test_get_service_key(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.base import BaseMapper
        mapper = BaseMapper()
        assert mapper.get_service_key() == "cloudwatch"
