# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from unittest import TestCase

import pytest


@pytest.mark.unit
class TestLambdaMapper(TestCase):
    def _make_metric_record(self, **overrides):
        record = {
            "metric_stream_name": "test-stream",
            "account_id": "123456789012",
            "region": "us-east-1",
            "namespace": "AWS/Lambda",
            "metric_name": "Invocations",
            "dimensions": {"FunctionName": "my-function"},
            "timestamp": 1705312200000,
            "value": {"max": 100.0, "min": 10.0, "sum": 550.0, "count": 10.0},
            "unit": "Count",
        }
        record.update(overrides)
        return record

    def test_service_key(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.lambda_ import LambdaMapper

        mapper = LambdaMapper()
        assert mapper.get_service_key() == "lambda"

    def test_data_stream_name(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.lambda_ import LambdaMapper

        mapper = LambdaMapper()
        assert mapper.get_data_stream_name() == "metrics-aws.lambda_metrics-default"

    def test_registry_lookup(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers import get_mapper
        from handlers.aws.cloudwatch_metrics_mappers.lambda_ import LambdaMapper

        mapper = get_mapper("AWS/Lambda")
        assert isinstance(mapper, LambdaMapper)

    def test_transform(self) -> None:
        from handlers.aws.cloudwatch_metrics_mappers.lambda_ import LambdaMapper

        mapper = LambdaMapper()
        record = self._make_metric_record()
        event = mapper.transform(record, s3_file_path="s3://bucket/key", byte_offset=0)

        assert event["@timestamp"] == "2024-01-15T09:50:00.000Z"
        assert event["fields"]["aws"]["lambda"]["metrics"]["Invocations"]["max"] == 100.0
        assert event["fields"]["aws"]["lambda"]["metrics"]["Invocations"]["avg"] == 55.0
        assert event["fields"]["aws"]["cloudwatch"]["namespace"] == "AWS/Lambda"
        assert event["meta"]["data_stream"] == "metrics-aws.lambda_metrics-default"
        assert event["meta"]["pipeline"] == "metrics-aws.lambda_metrics"
        assert event["meta"]["data_stream_dataset"] == "aws.lambda_metrics"
