# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import datetime
from typing import Any, Iterator, Optional

from botocore.client import BaseClient as BotoBaseClient

from share import ExpandEventListFromField, ProtocolMultiline, json_dumper, json_parser, shared_logger
from storage import ProtocolStorage, StorageFactory

from .utils import GZIP_ENCODING, PAYLOAD_ENCODING_KEY, get_account_id_from_arn, gzip_base64_encoded

_LAMBDA_PLATFORM_START = "platform.start"
_LAMBDA_PLATFORM_REPORT = "platform.report"


def _extract_lambda_platform_context(message: str) -> tuple[Optional[str], Optional[dict[str, Any]]]:
    """
    Parse a logEvent message for Lambda platform event type and context.

    Returns:
        ("platform.start", record_dict) for start events - keys in original camelCase
        ("platform.report", None) for report events
        (None, None) for function logs or parse failures
    """
    try:
        parsed = json_parser(message)
    except (ValueError, TypeError):
        return None, None

    if not isinstance(parsed, dict):
        return None, None

    event_type = parsed.get("type")
    if event_type == _LAMBDA_PLATFORM_START:
        record = parsed.get("record", {})
        return _LAMBDA_PLATFORM_START, dict(record) if record else None

    if event_type == _LAMBDA_PLATFORM_REPORT:
        return _LAMBDA_PLATFORM_REPORT, None

    return None, None


def _wrap_function_log_message(
    message: str,
    lambda_context: Optional[dict[str, Any]],
) -> str:
    """
    Rewrite a Lambda function log message into the platform event structure
    that the Lambda ingest pipeline expects.

    Transforms: {"time":"...","level":"INFO","msg":"hello",...}
    Into:        {"time":"...","type":"function","record":{"requestId":"...","message":"hello","level":"INFO",...}}
    """
    try:
        parsed = json_parser(message)
    except (ValueError, TypeError):
        parsed = None

    record: dict[str, Any] = {}
    original_time: Optional[str] = None

    # Merge platform.start context into record
    if lambda_context is not None:
        record.update(lambda_context)

    if isinstance(parsed, dict):
        original_time = parsed.get("time")
        # Move all fields except "time" into record, renaming "msg" to "message"
        for key, value in parsed.items():
            if key == "time":
                continue
            elif key == "msg":
                record["message"] = value
            else:
                record[key] = value

    wrapped: dict[str, Any] = {"type": "function", "record": record}
    if original_time is not None:
        wrapped["time"] = original_time

    return json_dumper(wrapped)


def _from_awslogs_data_to_event(awslogs_data: str) -> Any:
    """
    Returns cloudwatch logs event from base64 encoded and gzipped payload
    """
    storage: ProtocolStorage = StorageFactory.create(storage_type="payload", payload=awslogs_data)
    cloudwatch_logs_payload_plain = storage.get_as_string()
    return json_parser(cloudwatch_logs_payload_plain)


def _handle_cloudwatch_logs_move(
    sqs_client: BotoBaseClient,
    sqs_destination_queue: str,
    cloudwatch_logs_event: dict[str, Any],
    input_id: str,
    config_yaml: str,
    continuing_queue: bool = True,
    current_log_event: int = 0,
    last_ending_offset: Optional[int] = None,
    last_event_expanded_offset: Optional[int] = None,
) -> None:
    """
    Handler of the continuation queue for cloudwatch logs inputs
    If a cloudwatch logs data payload cannot be fully processed before the
    timeout of the lambda this handler will be called: it will
    send new sqs messages for the unprocessed payload to the
    internal continuing sqs queue
    """

    log_group_name = cloudwatch_logs_event["logGroup"]
    log_stream_name = cloudwatch_logs_event["logStream"]
    logs_events = cloudwatch_logs_event["logEvents"][current_log_event:]

    for current_log_event, log_event in enumerate(logs_events):
        if current_log_event > 0:
            last_ending_offset = None

        message_attributes = {
            "config": {"StringValue": config_yaml, "DataType": "String"},
            "originalEventId": {"StringValue": log_event["id"], "DataType": "String"},
            "originalEventSourceARN": {"StringValue": input_id, "DataType": "String"},
            "originalLogGroup": {"StringValue": log_group_name, "DataType": "String"},
            "originalLogStream": {"StringValue": log_stream_name, "DataType": "String"},
            "originalEventTimestamp": {"StringValue": str(log_event["timestamp"]), "DataType": "Number"},
            PAYLOAD_ENCODING_KEY: {"StringValue": GZIP_ENCODING, "DataType": "String"},
        }

        if last_ending_offset is not None:
            message_attributes["originalLastEndingOffset"] = {
                "StringValue": str(last_ending_offset),
                "DataType": "Number",
            }

        if last_event_expanded_offset is not None:
            message_attributes["originalLastEventExpandedOffset"] = {
                "StringValue": str(last_event_expanded_offset),
                "DataType": "Number",
            }

        # forward compressed message to sqs queue
        sqs_client.send_message(
            QueueUrl=sqs_destination_queue,
            MessageBody=gzip_base64_encoded(log_event["message"]),
            MessageAttributes=message_attributes,
        )

        if continuing_queue:
            shared_logger.debug(
                "continuing",
                extra={
                    "sqs_continuing_queue": sqs_destination_queue,
                    "last_ending_offset": last_ending_offset,
                    "last_event_expanded_offset": last_event_expanded_offset,
                    "event_id": log_event["id"],
                    "event_timestamp": log_event["timestamp"],
                },
            )
        else:
            shared_logger.debug(
                "replaying",
                extra={
                    "sqs_replaying_queue": sqs_destination_queue,
                    "event_id": log_event["id"],
                    "event_timestamp": log_event["timestamp"],
                },
            )


def _handle_cloudwatch_logs_event(
    event: dict[str, Any],
    aws_region: str,
    input_id: str,
    event_list_from_field_expander: ExpandEventListFromField,
    json_content_type: Optional[str],
    multiline_processor: Optional[ProtocolMultiline],
) -> Iterator[tuple[dict[str, Any], int, Optional[int], int]]:
    """
    Handler for cloudwatch logs inputs.
    It iterates through the logEvents in cloudwatch logs trigger payload and process
    content of body payload in the log event.
    If a log event cannot be fully processed before the
    timeout of the lambda it will call the sqs continuing handler
    """

    account_id = get_account_id_from_arn(input_id)

    log_group_name = event["logGroup"]
    log_stream_name = event["logStream"]

    lambda_context: Optional[dict[str, Any]] = None

    for cloudwatch_log_event_n, cloudwatch_log_event in enumerate(event["logEvents"]):
        event_id = cloudwatch_log_event["id"]
        event_timestamp = cloudwatch_log_event["timestamp"]
        message = cloudwatch_log_event["message"]

        # Track Lambda platform context across log events
        platform_type, platform_context = _extract_lambda_platform_context(message)
        if platform_type == _LAMBDA_PLATFORM_START:
            lambda_context = platform_context

        # Wrap function logs in platform event structure for ingest pipeline
        if platform_type is None:
            message = _wrap_function_log_message(message, lambda_context)

        storage_message: ProtocolStorage = StorageFactory.create(
            storage_type="payload",
            payload=message,
            json_content_type=json_content_type,
            event_list_from_field_expander=event_list_from_field_expander,
            multiline_processor=multiline_processor,
        )

        events = storage_message.get_by_lines(range_start=0)

        for log_event, starting_offset, ending_offset, event_expanded_offset in events:
            assert isinstance(log_event, bytes)

            es_event: dict[str, Any] = {
                "@timestamp": datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "fields": {
                    "message": log_event.decode("utf-8"),
                    "log": {
                        "offset": starting_offset,
                        "file": {
                            "path": f"{log_group_name}/{log_stream_name}",
                        },
                    },
                    "aws": {
                        "cloudwatch": {
                            "log_group": log_group_name,
                            "log_stream": log_stream_name,
                            "event_id": event_id,
                        },
                    },
                    "cloud": {
                        "provider": "aws",
                        "region": aws_region,
                        "account": {"id": account_id},
                    },
                },
                "meta": {"event_timestamp": event_timestamp},
            }

            yield es_event, ending_offset, event_expanded_offset, cloudwatch_log_event_n

        # Clear context after platform.report
        if platform_type == _LAMBDA_PLATFORM_REPORT:
            lambda_context = None
