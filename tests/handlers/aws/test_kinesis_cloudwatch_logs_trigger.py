# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import base64
import gzip
from typing import Any, Optional
from unittest.mock import MagicMock, call, patch

import pytest

from share import ExpandEventListFromField, json_dumper


_STREAM_ARN = "arn:aws:kinesis:us-east-1:123456789:stream/test-stream"


def _noop_field_resolver(integration_scope: str, field: str) -> str:
    return field


def _make_expander() -> ExpandEventListFromField:
    return ExpandEventListFromField("", "", _noop_field_resolver)


def _make_cw_payload(
    log_group: str = "test-group",
    log_stream: str = "test-stream",
    log_events: Optional[list[dict[str, Any]]] = None,
    extra_keys: Optional[dict[str, Any]] = None,
    omit_keys: Optional[list[str]] = None,
) -> str:
    """Build a base64-encoded gzip-compressed CW Logs JSON payload."""
    payload: dict[str, Any] = {
        "messageType": "DATA_MESSAGE",
        "owner": "123456789",
        "logGroup": log_group,
        "logStream": log_stream,
        "subscriptionFilters": ["filter"],
        "logEvents": log_events
        or [{"id": "evt1", "timestamp": 1234567890, "message": "hello"}],
    }
    if extra_keys:
        payload.update(extra_keys)
    if omit_keys:
        for k in omit_keys:
            payload.pop(k, None)
    compressed = gzip.compress(json_dumper(payload).encode("utf-8"))
    return base64.b64encode(compressed).decode("utf-8")


def _make_kinesis_event(records_data: list[str]) -> dict[str, Any]:
    """Wrap base64 data strings into a Kinesis event structure."""
    records = []
    for i, data in enumerate(records_data):
        records.append(
            {
                "eventSourceARN": _STREAM_ARN,
                "kinesis": {
                    "data": data,
                    "partitionKey": f"pk-{i}",
                    "sequenceNumber": f"seq-{i}",
                    "approximateArrivalTimestamp": 1234567890.0,
                },
            }
        )
    return {"Records": records}


class TestHandleKinesisCloudwatchLogsRecord:
    """Tests for _handle_kinesis_cloudwatch_logs_record."""

    def test_valid_single_record(self) -> None:
        from handlers.aws.kinesis_cloudwatch_logs_trigger import (
            _handle_kinesis_cloudwatch_logs_record,
        )

        encoded = _make_cw_payload(
            log_events=[
                {"id": "evt1", "timestamp": 1234567890, "message": "hello"},
                {"id": "evt2", "timestamp": 1234567891, "message": "world"},
            ]
        )
        event = _make_kinesis_event([encoded])

        results = list(
            _handle_kinesis_cloudwatch_logs_record(
                event=event,
                input_id=_STREAM_ARN,
                event_list_from_field_expander=_make_expander(),
                json_content_type=None,
                multiline_processor=None,
            )
        )

        # Should yield one tuple per log line produced by _handle_cloudwatch_logs_event
        assert len(results) >= 2
        for es_event, ending_offset, event_expanded_offset, kinesis_n, cw_n, invalid in results:
            assert es_event is not None
            assert invalid is None
            assert kinesis_n == 0
            assert "fields" in es_event
            assert "aws" in es_event["fields"]
            assert "cloudwatch" in es_event["fields"]["aws"]

    def test_invalid_record_non_gzip(self) -> None:
        """A record whose kinesis.data is plain text (not gzip) should yield an invalid tuple."""
        from handlers.aws.kinesis_cloudwatch_logs_trigger import (
            _handle_kinesis_cloudwatch_logs_record,
        )

        plain_text = base64.b64encode(b"this is not gzipped").decode("utf-8")
        event = _make_kinesis_event([plain_text])

        results = list(
            _handle_kinesis_cloudwatch_logs_record(
                event=event,
                input_id=_STREAM_ARN,
                event_list_from_field_expander=_make_expander(),
                json_content_type=None,
                multiline_processor=None,
            )
        )

        assert len(results) == 1
        es_event, ending_offset, event_expanded_offset, kinesis_n, cw_n, invalid = results[0]
        assert es_event is None
        assert ending_offset == 0
        assert event_expanded_offset is None
        assert kinesis_n == 0
        assert cw_n == 0
        assert invalid is not None
        assert invalid["kinesis"]["data"] == plain_text

    def test_invalid_record_missing_log_events_key(self) -> None:
        """Valid gzip JSON but missing logEvents key should yield an invalid tuple."""
        from handlers.aws.kinesis_cloudwatch_logs_trigger import (
            _handle_kinesis_cloudwatch_logs_record,
        )

        # Build a gzipped JSON that is missing logEvents
        payload = json_dumper(
            {
                "messageType": "DATA_MESSAGE",
                "owner": "123456789",
                "logGroup": "test-group",
                "logStream": "test-stream",
                "subscriptionFilters": ["filter"],
                # no logEvents key
            }
        )
        compressed = gzip.compress(payload.encode("utf-8"))
        encoded = base64.b64encode(compressed).decode("utf-8")
        event = _make_kinesis_event([encoded])

        results = list(
            _handle_kinesis_cloudwatch_logs_record(
                event=event,
                input_id=_STREAM_ARN,
                event_list_from_field_expander=_make_expander(),
                json_content_type=None,
                multiline_processor=None,
            )
        )

        assert len(results) == 1
        es_event, ending_offset, event_expanded_offset, kinesis_n, cw_n, invalid = results[0]
        assert es_event is None
        assert ending_offset == 0
        assert event_expanded_offset is None
        assert kinesis_n == 0
        assert cw_n == 0
        assert invalid is not None

    def test_invalid_record_missing_log_group_key(self) -> None:
        """Valid gzip JSON but missing logGroup key should yield an invalid tuple."""
        from handlers.aws.kinesis_cloudwatch_logs_trigger import (
            _handle_kinesis_cloudwatch_logs_record,
        )

        encoded = _make_cw_payload(omit_keys=["logGroup"])
        event = _make_kinesis_event([encoded])

        results = list(
            _handle_kinesis_cloudwatch_logs_record(
                event=event,
                input_id=_STREAM_ARN,
                event_list_from_field_expander=_make_expander(),
                json_content_type=None,
                multiline_processor=None,
            )
        )

        assert len(results) == 1
        es_event, _, _, _, _, invalid = results[0]
        assert es_event is None
        assert invalid is not None

    def test_invalid_record_missing_log_stream_key(self) -> None:
        """Valid gzip JSON but missing logStream key should yield an invalid tuple."""
        from handlers.aws.kinesis_cloudwatch_logs_trigger import (
            _handle_kinesis_cloudwatch_logs_record,
        )

        encoded = _make_cw_payload(omit_keys=["logStream"])
        event = _make_kinesis_event([encoded])

        results = list(
            _handle_kinesis_cloudwatch_logs_record(
                event=event,
                input_id=_STREAM_ARN,
                event_list_from_field_expander=_make_expander(),
                json_content_type=None,
                multiline_processor=None,
            )
        )

        assert len(results) == 1
        es_event, _, _, _, _, invalid = results[0]
        assert es_event is None
        assert invalid is not None

    def test_mixed_valid_and_invalid_records(self) -> None:
        """Mix of valid and invalid records should yield appropriate tuples."""
        from handlers.aws.kinesis_cloudwatch_logs_trigger import (
            _handle_kinesis_cloudwatch_logs_record,
        )

        valid_encoded = _make_cw_payload()
        invalid_encoded = base64.b64encode(b"not gzip").decode("utf-8")
        event = _make_kinesis_event([valid_encoded, invalid_encoded])

        results = list(
            _handle_kinesis_cloudwatch_logs_record(
                event=event,
                input_id=_STREAM_ARN,
                event_list_from_field_expander=_make_expander(),
                json_content_type=None,
                multiline_processor=None,
            )
        )

        # At least 2 results: 1 valid event + 1 invalid
        valid_results = [r for r in results if r[0] is not None]
        invalid_results = [r for r in results if r[0] is None]

        assert len(valid_results) >= 1
        assert len(invalid_results) == 1
        assert invalid_results[0][3] == 1  # kinesis_record_n = 1
        assert invalid_results[0][5] is not None  # invalid_kinesis_record

    def test_kinesis_record_n_increments(self) -> None:
        """kinesis_record_n should match the record index."""
        from handlers.aws.kinesis_cloudwatch_logs_trigger import (
            _handle_kinesis_cloudwatch_logs_record,
        )

        encoded1 = _make_cw_payload(
            log_events=[{"id": "evt1", "timestamp": 111, "message": "a"}]
        )
        encoded2 = _make_cw_payload(
            log_events=[{"id": "evt2", "timestamp": 222, "message": "b"}]
        )
        event = _make_kinesis_event([encoded1, encoded2])

        results = list(
            _handle_kinesis_cloudwatch_logs_record(
                event=event,
                input_id=_STREAM_ARN,
                event_list_from_field_expander=_make_expander(),
                json_content_type=None,
                multiline_processor=None,
            )
        )

        kinesis_ns = [r[3] for r in results]
        assert 0 in kinesis_ns
        assert 1 in kinesis_ns


class TestHandleKinesisCloudwatchLogsInvalidRecord:
    """Tests for _handle_kinesis_cloudwatch_logs_invalid_record."""

    def test_sends_to_replay_queue(self) -> None:
        from handlers.aws.kinesis_cloudwatch_logs_trigger import (
            _handle_kinesis_cloudwatch_logs_invalid_record,
        )

        sqs_client = MagicMock()
        kinesis_record: dict[str, Any] = {
            "eventSourceARN": _STREAM_ARN,
            "kinesis": {
                "data": "some-base64-data",
                "partitionKey": "pk-0",
                "sequenceNumber": "seq-123",
                "approximateArrivalTimestamp": 1234567890.0,
            },
        }

        _handle_kinesis_cloudwatch_logs_invalid_record(
            sqs_client=sqs_client,
            sqs_destination_queue="https://sqs.us-east-1.amazonaws.com/123456789/replay-queue",
            kinesis_record=kinesis_record,
            event_input_id=_STREAM_ARN,
            config_yaml="inputs: []",
        )

        sqs_client.send_message.assert_called_once()
        call_kwargs = sqs_client.send_message.call_args[1]

        assert call_kwargs["QueueUrl"] == "https://sqs.us-east-1.amazonaws.com/123456789/replay-queue"
        assert call_kwargs["MessageBody"] == "some-base64-data"

        attrs = call_kwargs["MessageAttributes"]
        assert attrs["config"]["StringValue"] == "inputs: []"
        assert attrs["originalEventSourceARN"]["StringValue"] == _STREAM_ARN
        assert attrs["originalPartitionKey"]["StringValue"] == "pk-0"
        assert attrs["originalSequenceNumber"]["StringValue"] == "seq-123"

    def test_different_config_yaml(self) -> None:
        from handlers.aws.kinesis_cloudwatch_logs_trigger import (
            _handle_kinesis_cloudwatch_logs_invalid_record,
        )

        sqs_client = MagicMock()
        kinesis_record: dict[str, Any] = {
            "eventSourceARN": _STREAM_ARN,
            "kinesis": {
                "data": "data123",
                "partitionKey": "pk-1",
                "sequenceNumber": "seq-456",
                "approximateArrivalTimestamp": 9999.0,
            },
        }

        _handle_kinesis_cloudwatch_logs_invalid_record(
            sqs_client=sqs_client,
            sqs_destination_queue="https://sqs.example.com/queue",
            kinesis_record=kinesis_record,
            event_input_id="arn:aws:kinesis:eu-west-1:999:stream/other",
            config_yaml="custom-config",
        )

        call_kwargs = sqs_client.send_message.call_args[1]
        assert call_kwargs["MessageAttributes"]["config"]["StringValue"] == "custom-config"
        assert (
            call_kwargs["MessageAttributes"]["originalEventSourceARN"]["StringValue"]
            == "arn:aws:kinesis:eu-west-1:999:stream/other"
        )


class TestSqsClientWithInputType:
    """Tests for _SqsClientWithInputType wrapper."""

    def test_injects_original_input_type(self) -> None:
        from handlers.aws.kinesis_cloudwatch_logs_trigger import _SqsClientWithInputType

        mock_client = MagicMock()
        wrapper = _SqsClientWithInputType(mock_client, "kinesis-cloudwatch-logs")

        wrapper.send_message(
            QueueUrl="https://sqs.example.com/queue",
            MessageBody="test",
            MessageAttributes={
                "config": {"StringValue": "cfg", "DataType": "String"},
            },
        )

        mock_client.send_message.assert_called_once()
        call_kwargs = mock_client.send_message.call_args[1]
        assert "originalInputType" in call_kwargs["MessageAttributes"]
        assert call_kwargs["MessageAttributes"]["originalInputType"] == {
            "StringValue": "kinesis-cloudwatch-logs",
            "DataType": "String",
        }

    def test_does_not_add_when_no_message_attributes(self) -> None:
        from handlers.aws.kinesis_cloudwatch_logs_trigger import _SqsClientWithInputType

        mock_client = MagicMock()
        wrapper = _SqsClientWithInputType(mock_client, "kinesis-cloudwatch-logs")

        wrapper.send_message(QueueUrl="https://sqs.example.com/queue", MessageBody="test")

        mock_client.send_message.assert_called_once()
        call_kwargs = mock_client.send_message.call_args[1]
        assert "MessageAttributes" not in call_kwargs


class TestHandleKinesisCloudwatchLogsMove:
    """Tests for _handle_kinesis_cloudwatch_logs_move."""

    def _make_kinesis_records(self, records_data: list[str]) -> list[dict[str, Any]]:
        """Build a list of kinesis record dicts."""
        records = []
        for i, data in enumerate(records_data):
            records.append(
                {
                    "eventSourceARN": _STREAM_ARN,
                    "kinesis": {
                        "data": data,
                        "partitionKey": f"pk-{i}",
                        "sequenceNumber": f"seq-{i}",
                        "approximateArrivalTimestamp": 1234567890.0,
                    },
                }
            )
        return records

    @patch("handlers.aws.kinesis_cloudwatch_logs_trigger._handle_cloudwatch_logs_move")
    def test_delegates_to_handle_cloudwatch_logs_move(self, mock_cw_move: MagicMock) -> None:
        from handlers.aws.kinesis_cloudwatch_logs_trigger import _handle_kinesis_cloudwatch_logs_move

        encoded = _make_cw_payload(
            log_events=[{"id": "evt1", "timestamp": 111, "message": "hello"}]
        )
        records = self._make_kinesis_records([encoded])
        sqs_client = MagicMock()

        _handle_kinesis_cloudwatch_logs_move(
            sqs_client=sqs_client,
            sqs_destination_queue="https://sqs.example.com/queue",
            kinesis_records=records,
            current_kinesis_record=0,
            input_id=_STREAM_ARN,
            config_yaml="inputs: []",
        )

        mock_cw_move.assert_called_once()
        call_kwargs = mock_cw_move.call_args[1]
        assert call_kwargs["sqs_destination_queue"] == "https://sqs.example.com/queue"
        assert call_kwargs["input_id"] == _STREAM_ARN
        assert call_kwargs["config_yaml"] == "inputs: []"
        assert call_kwargs["cloudwatch_logs_event"]["logGroup"] == "test-group"
        assert call_kwargs["cloudwatch_logs_event"]["logStream"] == "test-stream"
        assert call_kwargs["current_log_event"] == 0
        assert call_kwargs["last_ending_offset"] is None
        assert call_kwargs["last_event_expanded_offset"] is None

    @patch("handlers.aws.kinesis_cloudwatch_logs_trigger._handle_cloudwatch_logs_move")
    def test_passes_offsets_for_first_record_only(self, mock_cw_move: MagicMock) -> None:
        from handlers.aws.kinesis_cloudwatch_logs_trigger import _handle_kinesis_cloudwatch_logs_move

        encoded1 = _make_cw_payload(
            log_events=[{"id": "evt1", "timestamp": 111, "message": "a"}]
        )
        encoded2 = _make_cw_payload(
            log_events=[{"id": "evt2", "timestamp": 222, "message": "b"}]
        )
        records = self._make_kinesis_records([encoded1, encoded2])
        sqs_client = MagicMock()

        _handle_kinesis_cloudwatch_logs_move(
            sqs_client=sqs_client,
            sqs_destination_queue="https://sqs.example.com/queue",
            kinesis_records=records,
            current_kinesis_record=0,
            input_id=_STREAM_ARN,
            config_yaml="inputs: []",
            current_log_event=5,
            last_ending_offset=42,
            last_event_expanded_offset=10,
        )

        assert mock_cw_move.call_count == 2

        # First record should get the passed-through offsets
        first_call_kwargs = mock_cw_move.call_args_list[0][1]
        assert first_call_kwargs["current_log_event"] == 5
        assert first_call_kwargs["last_ending_offset"] == 42
        assert first_call_kwargs["last_event_expanded_offset"] == 10

        # Second record should get reset offsets
        second_call_kwargs = mock_cw_move.call_args_list[1][1]
        assert second_call_kwargs["current_log_event"] == 0
        assert second_call_kwargs["last_ending_offset"] is None
        assert second_call_kwargs["last_event_expanded_offset"] is None

    @patch("handlers.aws.kinesis_cloudwatch_logs_trigger._handle_cloudwatch_logs_move")
    @patch("handlers.aws.kinesis_cloudwatch_logs_trigger._handle_kinesis_cloudwatch_logs_invalid_record")
    def test_handles_invalid_records_in_remaining_batch(
        self, mock_invalid: MagicMock, mock_cw_move: MagicMock
    ) -> None:
        from handlers.aws.kinesis_cloudwatch_logs_trigger import _handle_kinesis_cloudwatch_logs_move

        valid_encoded = _make_cw_payload()
        invalid_encoded = base64.b64encode(b"not gzip").decode("utf-8")
        records = self._make_kinesis_records([valid_encoded, invalid_encoded])
        sqs_client = MagicMock()

        _handle_kinesis_cloudwatch_logs_move(
            sqs_client=sqs_client,
            sqs_destination_queue="https://sqs.example.com/queue",
            kinesis_records=records,
            current_kinesis_record=0,
            input_id=_STREAM_ARN,
            config_yaml="inputs: []",
        )

        # Valid record delegates to _handle_cloudwatch_logs_move
        mock_cw_move.assert_called_once()

        # Invalid record delegates to _handle_kinesis_cloudwatch_logs_invalid_record
        mock_invalid.assert_called_once()
        invalid_kwargs = mock_invalid.call_args[1]
        assert invalid_kwargs["kinesis_record"] == records[1]

    @patch("handlers.aws.kinesis_cloudwatch_logs_trigger._handle_cloudwatch_logs_move")
    def test_injects_original_input_type_via_wrapper(self, mock_cw_move: MagicMock) -> None:
        from handlers.aws.kinesis_cloudwatch_logs_trigger import (
            _SqsClientWithInputType,
            _handle_kinesis_cloudwatch_logs_move,
        )

        encoded = _make_cw_payload()
        records = self._make_kinesis_records([encoded])
        sqs_client = MagicMock()

        _handle_kinesis_cloudwatch_logs_move(
            sqs_client=sqs_client,
            sqs_destination_queue="https://sqs.example.com/queue",
            kinesis_records=records,
            current_kinesis_record=0,
            input_id=_STREAM_ARN,
            config_yaml="inputs: []",
        )

        mock_cw_move.assert_called_once()
        wrapped = mock_cw_move.call_args[1]["sqs_client"]
        assert isinstance(wrapped, _SqsClientWithInputType)

    @patch("handlers.aws.kinesis_cloudwatch_logs_trigger._handle_cloudwatch_logs_move")
    def test_slices_from_current_kinesis_record(self, mock_cw_move: MagicMock) -> None:
        from handlers.aws.kinesis_cloudwatch_logs_trigger import _handle_kinesis_cloudwatch_logs_move

        encoded1 = _make_cw_payload(
            log_events=[{"id": "evt1", "timestamp": 111, "message": "a"}]
        )
        encoded2 = _make_cw_payload(
            log_events=[{"id": "evt2", "timestamp": 222, "message": "b"}]
        )
        encoded3 = _make_cw_payload(
            log_events=[{"id": "evt3", "timestamp": 333, "message": "c"}]
        )
        records = self._make_kinesis_records([encoded1, encoded2, encoded3])
        sqs_client = MagicMock()

        # Start from record 1 (skip record 0)
        _handle_kinesis_cloudwatch_logs_move(
            sqs_client=sqs_client,
            sqs_destination_queue="https://sqs.example.com/queue",
            kinesis_records=records,
            current_kinesis_record=1,
            input_id=_STREAM_ARN,
            config_yaml="inputs: []",
        )

        # Should only process records 1 and 2
        assert mock_cw_move.call_count == 2
