# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any, Iterator, Optional

from botocore.client import BaseClient as BotoBaseClient

from share import ExpandEventListFromField, ProtocolMultiline, shared_logger

from .cloudwatch_logs_trigger import _from_awslogs_data_to_event, _handle_cloudwatch_logs_event, _handle_cloudwatch_logs_move
from .utils import get_kinesis_stream_name_type_and_region_from_arn

_REQUIRED_CW_KEYS = {"logGroup", "logStream", "logEvents"}


def _handle_kinesis_cloudwatch_logs_record(
    event: dict[str, Any],
    input_id: str,
    event_list_from_field_expander: ExpandEventListFromField,
    json_content_type: Optional[str],
    multiline_processor: Optional[ProtocolMultiline],
) -> Iterator[tuple[Optional[dict[str, Any]], int, Optional[int], int, int, Optional[dict[str, Any]]]]:
    """
    Handler for kinesis-cloudwatch-logs inputs.
    Iterates through Kinesis Records[], decodes each record's kinesis.data
    as a CloudWatch Logs payload, validates required keys, and delegates
    to _handle_cloudwatch_logs_event() for valid records.

    Yields tuples of:
        (es_event, ending_offset, event_expanded_offset, kinesis_record_n,
         cloudwatch_log_event_n, invalid_kinesis_record)
    """

    for kinesis_record_n, kinesis_record in enumerate(event["Records"]):
        kinesis_data: str = kinesis_record["kinesis"]["data"]

        # Try to decode the CW Logs payload from the Kinesis record
        try:
            cw_event = _from_awslogs_data_to_event(kinesis_data)
        except Exception:
            shared_logger.debug(
                "invalid kinesis record: failed to decode cloudwatch logs payload",
                extra={"kinesis_record_n": kinesis_record_n},
            )
            yield None, 0, None, kinesis_record_n, 0, kinesis_record
            continue

        # Validate that the decoded event has the required keys
        if not isinstance(cw_event, dict) or not _REQUIRED_CW_KEYS.issubset(cw_event.keys()):
            shared_logger.debug(
                "invalid kinesis record: missing required cloudwatch logs keys",
                extra={
                    "kinesis_record_n": kinesis_record_n,
                    "missing_keys": list(_REQUIRED_CW_KEYS - set(cw_event.keys() if isinstance(cw_event, dict) else [])),
                },
            )
            yield None, 0, None, kinesis_record_n, 0, kinesis_record
            continue

        # Extract region from the Kinesis stream ARN
        _, _, aws_region = get_kinesis_stream_name_type_and_region_from_arn(
            kinesis_record["eventSourceARN"]
        )

        # Delegate to the CW Logs event handler
        for es_event, ending_offset, event_expanded_offset, cw_log_event_n in _handle_cloudwatch_logs_event(
            event=cw_event,
            aws_region=aws_region,
            input_id=input_id,
            event_list_from_field_expander=event_list_from_field_expander,
            json_content_type=json_content_type,
            multiline_processor=multiline_processor,
        ):
            yield es_event, ending_offset, event_expanded_offset, kinesis_record_n, cw_log_event_n, None


def _handle_kinesis_cloudwatch_logs_invalid_record(
    sqs_client: BotoBaseClient,
    sqs_destination_queue: str,
    kinesis_record: dict[str, Any],
    event_input_id: str,
    config_yaml: str,
) -> None:
    """
    Sends an invalid Kinesis record to the replay SQS queue.

    An invalid record is one whose kinesis.data could not be decoded as a
    valid CloudWatch Logs payload.
    """
    kinesis_data: str = kinesis_record["kinesis"]["data"]
    partition_key: str = kinesis_record["kinesis"]["partitionKey"]
    sequence_number: str = kinesis_record["kinesis"]["sequenceNumber"]

    message_attributes = {
        "config": {"StringValue": config_yaml, "DataType": "String"},
        "originalEventSourceARN": {"StringValue": event_input_id, "DataType": "String"},
        "originalPartitionKey": {"StringValue": partition_key, "DataType": "String"},
        "originalSequenceNumber": {"StringValue": sequence_number, "DataType": "String"},
    }

    sqs_client.send_message(
        QueueUrl=sqs_destination_queue,
        MessageBody=kinesis_data,
        MessageAttributes=message_attributes,
    )

    shared_logger.debug(
        "sent invalid kinesis-cloudwatch-logs record to replay queue",
        extra={
            "sqs_replay_queue": sqs_destination_queue,
            "partition_key": partition_key,
            "sequence_number": sequence_number,
        },
    )


class _SqsClientWithInputType:
    """Wraps SQS client to inject originalInputType attribute into messages."""

    def __init__(self, sqs_client: BotoBaseClient, input_type: str):
        self._sqs_client = sqs_client
        self._input_type = input_type

    def send_message(self, **kwargs: Any) -> Any:
        if "MessageAttributes" in kwargs:
            kwargs["MessageAttributes"]["originalInputType"] = {
                "StringValue": self._input_type,
                "DataType": "String",
            }
        return self._sqs_client.send_message(**kwargs)


def _handle_kinesis_cloudwatch_logs_move(
    sqs_client: BotoBaseClient,
    sqs_destination_queue: str,
    kinesis_records: list[dict[str, Any]],
    current_kinesis_record: int,
    input_id: str,
    config_yaml: str,
    continuing_queue: bool = True,
    current_log_event: int = 0,
    last_ending_offset: Optional[int] = None,
    last_event_expanded_offset: Optional[int] = None,
) -> None:
    """
    Continuation handler for kinesis-cloudwatch-logs inputs.
    Processes remaining Kinesis records when Lambda times out by decoding
    each record and delegating to _handle_cloudwatch_logs_move().
    """
    remaining_records = kinesis_records[current_kinesis_record:]
    wrapped_client = _SqsClientWithInputType(sqs_client, "kinesis-cloudwatch-logs")

    for record_idx, kinesis_record in enumerate(remaining_records):
        kinesis_data: str = kinesis_record["kinesis"]["data"]

        # Try to decode the CW Logs payload from the Kinesis record
        try:
            cw_event = _from_awslogs_data_to_event(kinesis_data)
        except Exception:
            shared_logger.debug(
                "invalid kinesis record during continuation: failed to decode cloudwatch logs payload",
                extra={"record_idx": record_idx},
            )
            _handle_kinesis_cloudwatch_logs_invalid_record(
                sqs_client=sqs_client,
                sqs_destination_queue=sqs_destination_queue,
                kinesis_record=kinesis_record,
                event_input_id=input_id,
                config_yaml=config_yaml,
            )
            continue

        # Validate that the decoded event has the required keys
        if not isinstance(cw_event, dict) or not _REQUIRED_CW_KEYS.issubset(cw_event.keys()):
            shared_logger.debug(
                "invalid kinesis record during continuation: missing required cloudwatch logs keys",
                extra={"record_idx": record_idx},
            )
            _handle_kinesis_cloudwatch_logs_invalid_record(
                sqs_client=sqs_client,
                sqs_destination_queue=sqs_destination_queue,
                kinesis_record=kinesis_record,
                event_input_id=input_id,
                config_yaml=config_yaml,
            )
            continue

        # For the first record, pass through offsets; for subsequent records, reset them
        if record_idx == 0:
            record_log_event = current_log_event
            record_ending_offset = last_ending_offset
            record_event_expanded_offset = last_event_expanded_offset
        else:
            record_log_event = 0
            record_ending_offset = None
            record_event_expanded_offset = None

        _handle_cloudwatch_logs_move(
            sqs_client=wrapped_client,  # type: ignore[arg-type]
            sqs_destination_queue=sqs_destination_queue,
            cloudwatch_logs_event=cw_event,
            input_id=input_id,
            config_yaml=config_yaml,
            continuing_queue=continuing_queue,
            current_log_event=record_log_event,
            last_ending_offset=record_ending_offset,
            last_event_expanded_offset=record_event_expanded_offset,
        )
