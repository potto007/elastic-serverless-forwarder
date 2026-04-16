# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any, Optional

import pytest

from share import ExpandEventListFromField, json_dumper, json_parser


_INPUT_ID = "arn:aws:logs:us-east-1:123456789012:log-group:test-group"
_AWS_REGION = "us-east-1"


def _noop_field_resolver(integration_scope: str, field: str) -> str:
    return field


def _make_expander() -> ExpandEventListFromField:
    return ExpandEventListFromField("", "", _noop_field_resolver)


def _make_cw_event(
    log_events: list[dict[str, Any]],
    log_group: str = "/aws/lambda/my-function",
    log_stream: str = "2026/04/14/[$LATEST]abc123",
) -> dict[str, Any]:
    return {
        "messageType": "DATA_MESSAGE",
        "owner": "123456789012",
        "logGroup": log_group,
        "logStream": log_stream,
        "subscriptionFilters": ["filter"],
        "logEvents": log_events,
    }


def _platform_start_message(
    request_id: str = "req-123",
    function_arn: str = "arn:aws:lambda:us-east-1:123456789012:function:my-function",
    version: str = "$LATEST",
) -> str:
    return json_dumper({
        "time": "2026-04-08T19:12:49.468Z",
        "type": "platform.start",
        "record": {
            "requestId": request_id,
            "functionArn": function_arn,
            "version": version,
            "tracing": {
                "spanId": "abc123",
                "type": "X-Amzn-Trace-Id",
                "value": "Root=1-abc-def;Parent=ghi;Sampled=1",
            },
        },
    })


def _platform_report_message(request_id: str = "req-123") -> str:
    return json_dumper({
        "time": "2026-04-08T19:12:49.963Z",
        "type": "platform.report",
        "record": {
            "requestId": request_id,
            "metrics": {
                "durationMs": 495.138,
                "billedDurationMs": 654,
                "memorySizeMB": 512,
                "maxMemoryUsedMB": 64,
            },
            "status": "success",
        },
    })


def _function_log_message(msg: str = "hello world") -> str:
    return json_dumper({
        "time": "2026-04-08T19:12:49.738Z",
        "level": "INFO",
        "msg": msg,
    })


def _collect_results(
    cw_event: dict[str, Any],
    json_content_type: Optional[str] = None,
) -> list[dict[str, Any]]:
    from handlers.aws.cloudwatch_logs_trigger import _handle_cloudwatch_logs_event

    results = []
    for es_event, _, _, _ in _handle_cloudwatch_logs_event(
        event=cw_event,
        aws_region=_AWS_REGION,
        input_id=_INPUT_ID,
        event_list_from_field_expander=_make_expander(),
        json_content_type=json_content_type,
        multiline_processor=None,
    ):
        results.append(es_event)
    return results


def _parse_message(es_event: dict[str, Any]) -> dict[str, Any]:
    """Parse fields.message JSON from an es_event."""
    return json_parser(es_event["fields"]["message"])


class TestExtractLambdaPlatformContext:
    """Tests for _extract_lambda_platform_context."""

    def test_platform_start(self) -> None:
        from handlers.aws.cloudwatch_logs_trigger import _extract_lambda_platform_context

        event_type, context = _extract_lambda_platform_context(_platform_start_message())
        assert event_type == "platform.start"
        assert context is not None
        assert context["requestId"] == "req-123"
        assert context["functionArn"] == "arn:aws:lambda:us-east-1:123456789012:function:my-function"
        assert context["version"] == "$LATEST"
        assert context["tracing"]["spanId"] == "abc123"

    def test_platform_report(self) -> None:
        from handlers.aws.cloudwatch_logs_trigger import _extract_lambda_platform_context

        event_type, context = _extract_lambda_platform_context(_platform_report_message())
        assert event_type == "platform.report"
        assert context is None

    def test_function_log(self) -> None:
        from handlers.aws.cloudwatch_logs_trigger import _extract_lambda_platform_context

        event_type, context = _extract_lambda_platform_context(_function_log_message())
        assert event_type is None
        assert context is None

    def test_plain_text(self) -> None:
        from handlers.aws.cloudwatch_logs_trigger import _extract_lambda_platform_context

        event_type, context = _extract_lambda_platform_context("just a plain log line")
        assert event_type is None
        assert context is None

    def test_empty_record(self) -> None:
        from handlers.aws.cloudwatch_logs_trigger import _extract_lambda_platform_context

        msg = json_dumper({"type": "platform.start", "record": {}})
        event_type, context = _extract_lambda_platform_context(msg)
        assert event_type == "platform.start"
        assert context is None


class TestWrapFunctionLogMessage:
    """Tests for _wrap_function_log_message."""

    def test_wraps_with_context(self) -> None:
        from handlers.aws.cloudwatch_logs_trigger import _wrap_function_log_message

        context = {"requestId": "req-123", "functionArn": "arn:aws:lambda:..."}
        result = json_parser(_wrap_function_log_message(_function_log_message("hello"), context))

        assert result["type"] == "platform"
        assert result["time"] == "2026-04-08T19:12:49.738Z"
        assert result["level"] == "INFO"
        assert result["record"]["requestId"] == "req-123"
        assert result["record"]["functionArn"] == "arn:aws:lambda:..."
        assert result["record"]["message"] == "hello"
        assert "msg" not in result["record"]

    def test_wraps_without_context(self) -> None:
        from handlers.aws.cloudwatch_logs_trigger import _wrap_function_log_message

        result = json_parser(_wrap_function_log_message(_function_log_message("orphan"), None))

        assert result["type"] == "platform"
        assert result["time"] == "2026-04-08T19:12:49.738Z"
        assert result["level"] == "INFO"
        assert result["record"]["message"] == "orphan"
        assert "requestId" not in result["record"]

    def test_preserves_extra_fields_in_record(self) -> None:
        from handlers.aws.cloudwatch_logs_trigger import _wrap_function_log_message

        msg = json_dumper({
            "time": "2026-04-08T19:12:49.738Z",
            "level": "INFO",
            "msg": "test",
            "bucket": "my-bucket",
            "stargate": {"team": {"name": "tetris"}},
        })
        result = json_parser(_wrap_function_log_message(msg, None))

        assert result["record"]["bucket"] == "my-bucket"
        assert result["record"]["stargate"]["team"]["name"] == "tetris"
        assert result["record"]["message"] == "test"
        assert "msg" not in result["record"]
        assert "time" not in result["record"]
        assert "level" not in result["record"]

    def test_plain_text_message(self) -> None:
        from handlers.aws.cloudwatch_logs_trigger import _wrap_function_log_message

        result = json_parser(_wrap_function_log_message("plain text log", None))

        assert result["type"] == "platform"
        assert result["record"] == {}
        assert "time" not in result
        assert "level" not in result

    def test_time_and_level_stay_top_level(self) -> None:
        from handlers.aws.cloudwatch_logs_trigger import _wrap_function_log_message

        result = json_parser(_wrap_function_log_message(_function_log_message("hello"), None))

        assert result["time"] == "2026-04-08T19:12:49.738Z"
        assert result["level"] == "INFO"
        assert "time" not in result["record"]
        assert "level" not in result["record"]


class TestLambdaFunctionLogWrapping:
    """Tests for function log wrapping in _handle_cloudwatch_logs_event."""

    def test_function_logs_wrapped_with_context(self) -> None:
        log_events = [
            {"id": "1", "timestamp": 1000, "message": _platform_start_message()},
            {"id": "2", "timestamp": 1001, "message": _function_log_message("log1")},
            {"id": "3", "timestamp": 1002, "message": _function_log_message("log2")},
            {"id": "4", "timestamp": 1003, "message": _platform_report_message()},
        ]
        cw_event = _make_cw_event(log_events)
        results = _collect_results(cw_event)

        assert len(results) == 4

        for r in results[1:3]:
            parsed = _parse_message(r)
            assert parsed["type"] == "platform"
            assert parsed["record"]["requestId"] == "req-123"
            assert parsed["record"]["functionArn"] == "arn:aws:lambda:us-east-1:123456789012:function:my-function"

        assert _parse_message(results[1])["record"]["message"] == "log1"
        assert _parse_message(results[2])["record"]["message"] == "log2"

    def test_platform_events_not_wrapped(self) -> None:
        log_events = [
            {"id": "1", "timestamp": 1000, "message": _platform_start_message()},
            {"id": "2", "timestamp": 1001, "message": _platform_report_message()},
        ]
        cw_event = _make_cw_event(log_events)
        results = _collect_results(cw_event)

        assert len(results) == 2
        assert _parse_message(results[0])["type"] == "platform.start"
        assert _parse_message(results[1])["type"] == "platform.report"

    def test_orphan_logs_wrapped_without_context(self) -> None:
        log_events = [
            {"id": "1", "timestamp": 1000, "message": _function_log_message("orphan")},
            {"id": "2", "timestamp": 1001, "message": _platform_start_message()},
        ]
        cw_event = _make_cw_event(log_events)
        results = _collect_results(cw_event)

        assert len(results) == 2
        parsed = _parse_message(results[0])
        assert parsed["type"] == "platform"
        assert parsed["record"]["message"] == "orphan"
        assert "requestId" not in parsed["record"]

    def test_context_cleared_after_report(self) -> None:
        log_events = [
            {"id": "1", "timestamp": 1000, "message": _platform_start_message(request_id="req-1")},
            {"id": "2", "timestamp": 1001, "message": _function_log_message("during")},
            {"id": "3", "timestamp": 1002, "message": _platform_report_message(request_id="req-1")},
            {"id": "4", "timestamp": 1003, "message": _function_log_message("after")},
        ]
        cw_event = _make_cw_event(log_events)
        results = _collect_results(cw_event)

        assert len(results) == 4
        assert _parse_message(results[1])["record"]["requestId"] == "req-1"
        assert "requestId" not in _parse_message(results[3])["record"]

    def test_multiple_invocations_in_payload(self) -> None:
        log_events = [
            {"id": "1", "timestamp": 1000, "message": _platform_start_message(request_id="req-1")},
            {"id": "2", "timestamp": 1001, "message": _function_log_message("first")},
            {"id": "3", "timestamp": 1002, "message": _platform_report_message(request_id="req-1")},
            {"id": "4", "timestamp": 1003, "message": _platform_start_message(request_id="req-2")},
            {"id": "5", "timestamp": 1004, "message": _function_log_message("second")},
            {"id": "6", "timestamp": 1005, "message": _platform_report_message(request_id="req-2")},
        ]
        cw_event = _make_cw_event(log_events)
        results = _collect_results(cw_event)

        assert len(results) == 6
        assert _parse_message(results[1])["record"]["requestId"] == "req-1"
        assert _parse_message(results[4])["record"]["requestId"] == "req-2"

    def test_plain_text_logs_wrapped(self) -> None:
        log_events = [
            {"id": "1", "timestamp": 1000, "message": "START RequestId: req-123"},
            {"id": "2", "timestamp": 1001, "message": "plain text log"},
        ]
        cw_event = _make_cw_event(log_events)
        results = _collect_results(cw_event)

        assert len(results) == 2
        for r in results:
            parsed = _parse_message(r)
            assert parsed["type"] == "platform"
            assert parsed["record"] == {}

    def test_existing_cloudwatch_fields_preserved(self) -> None:
        log_events = [
            {"id": "1", "timestamp": 1000, "message": _platform_start_message()},
            {"id": "2", "timestamp": 1001, "message": _function_log_message("test")},
        ]
        cw_event = _make_cw_event(log_events)
        results = _collect_results(cw_event)

        for r in results:
            cw = r["fields"]["aws"]["cloudwatch"]
            assert cw["log_group"] == "/aws/lambda/my-function"
            assert cw["log_stream"] == "2026/04/14/[$LATEST]abc123"
            assert r["fields"]["cloud"]["provider"] == "aws"

    def test_no_lambda_key_in_aws_fields(self) -> None:
        """Enrichment is in fields.message, not fields.aws.lambda."""
        log_events = [
            {"id": "1", "timestamp": 1000, "message": _platform_start_message()},
            {"id": "2", "timestamp": 1001, "message": _function_log_message("test")},
            {"id": "3", "timestamp": 1002, "message": _platform_report_message()},
        ]
        cw_event = _make_cw_event(log_events)
        results = _collect_results(cw_event)

        for r in results:
            assert "lambda" not in r["fields"]["aws"]

    def test_function_log_preserves_original_time(self) -> None:
        log_events = [
            {"id": "1", "timestamp": 1000, "message": _platform_start_message()},
            {"id": "2", "timestamp": 1001, "message": _function_log_message("test")},
        ]
        cw_event = _make_cw_event(log_events)
        results = _collect_results(cw_event)

        parsed = _parse_message(results[1])
        assert parsed["time"] == "2026-04-08T19:12:49.738Z"

    def test_function_log_level_top_level(self) -> None:
        log_events = [
            {"id": "1", "timestamp": 1000, "message": _platform_start_message()},
            {"id": "2", "timestamp": 1001, "message": _function_log_message("test")},
        ]
        cw_event = _make_cw_event(log_events)
        results = _collect_results(cw_event)

        parsed = _parse_message(results[1])
        assert parsed["level"] == "INFO"
        assert "level" not in parsed["record"]

    def test_function_log_extra_fields_in_record(self) -> None:
        msg = json_dumper({
            "time": "2026-04-08T19:12:49.738Z",
            "level": "INFO",
            "msg": "Identified K8s artifact",
            "bucket": "my-bucket",
            "key": "path/to/chart.tgz",
        })
        log_events = [
            {"id": "1", "timestamp": 1000, "message": _platform_start_message()},
            {"id": "2", "timestamp": 1001, "message": msg},
        ]
        cw_event = _make_cw_event(log_events)
        results = _collect_results(cw_event)

        parsed = _parse_message(results[1])
        assert parsed["record"]["message"] == "Identified K8s artifact"
        assert parsed["record"]["bucket"] == "my-bucket"
        assert parsed["record"]["key"] == "path/to/chart.tgz"
        assert parsed["record"]["requestId"] == "req-123"
        assert "msg" not in parsed["record"]
