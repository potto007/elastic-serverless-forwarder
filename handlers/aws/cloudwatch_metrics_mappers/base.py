# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import datetime
from typing import Any

from share import json_dumper


class BaseMapper:
    """Base mapper for CloudWatch Metric Stream records."""

    def get_service_key(self) -> str:
        return "cloudwatch"

    def get_data_stream_name(self) -> str:
        return f"metrics-aws.{self.get_service_key()}_metrics-default"

    def get_dataset(self) -> str:
        return f"aws.{self.get_service_key()}_metrics"

    def get_pipeline_name(self) -> str:
        return f"metrics-aws.{self.get_service_key()}_metrics"

    def _build_aws_fields(
        self, record: dict[str, Any], service_key: str, metric_values: dict[str, Any]
    ) -> dict[str, Any]:
        service_data: dict[str, Any] = {
            "metrics": {
                record["metric_name"]: metric_values,
            },
        }
        aws: dict[str, Any] = {
            "cloudwatch": {"namespace": record.get("namespace", "")},
            "dimensions": record.get("dimensions", {}),
        }
        if service_key == "cloudwatch":
            aws["cloudwatch"]["metrics"] = service_data["metrics"]
        else:
            aws[service_key] = service_data
        return aws

    def transform(self, record: dict[str, Any], s3_file_path: str, byte_offset: int) -> dict[str, Any]:
        timestamp_ms = record["timestamp"]
        timestamp_dt = datetime.datetime.fromtimestamp(timestamp_ms / 1000.0, tz=datetime.UTC)
        timestamp_str = timestamp_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        value = record.get("value", {})
        count = value.get("count", 0.0)
        avg = value.get("sum", 0.0) / count if count > 0 else 0.0

        metric_values: dict[str, Any] = {
            "max": value.get("max", 0.0),
            "min": value.get("min", 0.0),
            "sum": value.get("sum", 0.0),
            "avg": avg,
        }

        service_key = self.get_service_key()
        dataset = self.get_dataset()
        data_stream_name = self.get_data_stream_name()

        return {
            "@timestamp": timestamp_str,
            "fields": {
                "message": json_dumper(record),
                "log": {
                    "offset": byte_offset,
                    "file": {"path": s3_file_path},
                },
                "aws": self._build_aws_fields(record, service_key, metric_values),
                "cloud": {
                    "provider": "aws",
                    "region": record.get("region", ""),
                    "account": {"id": record.get("account_id", "")},
                },
            },
            "meta": {
                "event_time": timestamp_ms,
                "metric_name": record["metric_name"],
                "data_stream": data_stream_name,
                "pipeline": self.get_pipeline_name(),
                "data_stream_type": "metrics",
                "data_stream_dataset": dataset,
                "data_stream_namespace": "default",
            },
        }
