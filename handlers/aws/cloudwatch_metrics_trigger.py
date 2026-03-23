# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any, Iterator, Optional
from urllib.parse import unquote_plus

from share import json_parser, shared_logger
from storage import StorageFactory

from .cloudwatch_metrics_mappers import get_mapper
from .utils import get_bucket_name_from_arn


def _handle_cloudwatch_metrics_event(
    sqs_record_body: dict[str, Any],
    input_id: str,
) -> Iterator[tuple[dict[str, Any], int, Optional[int], int]]:
    """
    Handler for cloudwatch-metrics input.
    Reads S3 objects containing CloudWatch Metric Stream JSON (newline-delimited),
    transforms each metric record using the mapper registry, and yields events.

    Yields the same tuple shape as _handle_s3_sqs_event:
    (es_event, ending_offset, event_expanded_offset, s3_record_n)
    """
    for s3_record_n, s3_record in enumerate(sqs_record_body["Records"]):
        aws_region = s3_record["awsRegion"]
        bucket_arn = unquote_plus(s3_record["s3"]["bucket"]["arn"], "utf-8")
        object_key = unquote_plus(s3_record["s3"]["object"]["key"], "utf-8")
        last_ending_offset = s3_record.get("last_ending_offset", 0)

        bucket_name = get_bucket_name_from_arn(bucket_arn)
        s3_file_path = f"https://{bucket_name}.s3.{aws_region}.amazonaws.com/{object_key}"

        storage = StorageFactory.create(
            storage_type="s3",
            bucket_name=bucket_name,
            object_key=object_key,
        )

        events = storage.get_by_lines(range_start=last_ending_offset)

        for line_bytes, starting_offset, ending_offset, event_expanded_offset in events:
            assert isinstance(line_bytes, bytes)
            line_str = line_bytes.decode("utf-8").strip()

            if not line_str:
                continue

            try:
                record = json_parser(line_str)
            except Exception:
                shared_logger.warning(
                    "skipping malformed metric record",
                    extra={"offset": starting_offset, "s3_path": s3_file_path},
                )
                continue

            if not isinstance(record, dict) or "namespace" not in record or "metric_name" not in record:
                shared_logger.warning(
                    "skipping metric record missing required fields",
                    extra={"offset": starting_offset, "s3_path": s3_file_path},
                )
                continue

            mapper = get_mapper(record["namespace"])
            es_event = mapper.transform(record, s3_file_path=s3_file_path, byte_offset=starting_offset)

            yield es_event, ending_offset, event_expanded_offset, s3_record_n
