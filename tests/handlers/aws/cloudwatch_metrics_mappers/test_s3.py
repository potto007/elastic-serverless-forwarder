# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from unittest import TestCase

import pytest


@pytest.mark.unit
class TestS3Mapper(TestCase):
    def _make_metric_record(self, **overrides):
        record = {
            "metric_stream_name": "test-stream",
            "account_id": "123456789012",
            "region": "us-east-1",
            "namespace": "AWS/S3",
            "metric_name": "AllRequests",
            "dimensions": {"BucketName": "my-bucket", "FilterId": "EntireBucket"},
            "timestamp": 1705312200000,
            "value": {"max": 100.0, "min": 10.0, "sum": 550.0, "count": 10.0},
            "unit": "Count",
        }
        record.update(overrides)
        return record

    def test_service_key(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.s3 import S3Mapper

        mapper = S3Mapper()
        assert mapper.get_service_key() == "s3"

    def test_data_stream_name(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.s3 import S3Mapper

        mapper = S3Mapper()
        assert mapper.get_data_stream_name() == "metrics-aws.s3_request-default"

    def test_registry_lookup(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers import get_mapper
        from handlers.aws.cloudwatch_metrics_mappers.s3 import S3Mapper

        mapper = get_mapper("AWS/S3")
        assert isinstance(mapper, S3Mapper)

    def test_transform(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.s3 import S3Mapper

        mapper = S3Mapper()
        record = self._make_metric_record()
        event = mapper.transform(record, s3_file_path="s3://bucket/key", byte_offset=0)

        assert event["@timestamp"] == "2024-01-15T09:50:00.000Z"
        assert event["fields"]["aws"]["s3"]["metrics"]["AllRequests"]["max"] == 100.0
        assert event["fields"]["aws"]["s3"]["metrics"]["AllRequests"]["avg"] == 55.0
        assert event["fields"]["aws"]["cloudwatch"]["namespace"] == "AWS/S3"
        assert event["meta"]["data_stream"] == "metrics-aws.s3_request-default"
        assert event["meta"]["pipeline"] == "metrics-aws.s3_request"
        assert event["meta"]["data_stream_dataset"] == "aws.s3_request"
