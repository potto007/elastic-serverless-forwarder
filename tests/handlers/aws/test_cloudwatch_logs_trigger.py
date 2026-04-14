# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

import json
from typing import Any, Optional

import pytest

from share import ExpandEventListFromField, json_dumper


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


class TestExtractLambdaPlatformContext:
    """Tests for _extract_lambda_platform_context."""

    def test_platform_start(self) -> None:
        from handlers.aws.cloudwatch_logs_trigger import _extract_lambda_platform_context

        event_type, context = _extract_lambda_platform_context(_platform_start_message())
        assert event_type == "platform.start"
        assert context is not None
        assert context["request_id"] == "req-123"
        assert context["function_arn"] == "arn:aws:lambda:us-east-1:123456789012:function:my-function"
        assert context["version"] == "$LATEST"
        assert context["tracing"]["span_id"] == "abc123"
        assert context["tracing"]["type"] == "X-Amzn-Trace-Id"

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
        assert context is None  # empty context -> None


class TestLambdaEnrichment:
    """Tests for Lambda context enrichment in _handle_cloudwatch_logs_event."""

    def test_function_logs_enriched_between_start_and_report(self) -> None:
        log_events = [
            {"id": "1", "timestamp": 1000, "message": _platform_start_message()},
            {"id": "2", "timestamp": 1001, "message": _function_log_message("log1")},
            {"id": "3", "timestamp": 1002, "message": _function_log_message("log2")},
            {"id": "4", "timestamp": 1003, "message": _platform_report_message()},
        ]
        cw_event = _make_cw_event(log_events)
        results = _collect_results(cw_event)

        assert len(results) == 4

        # platform.start - no lambda enrichment
        assert "lambda" not in results[0]["fields"]["aws"]

        # function logs - enriched
        for r in results[1:3]:
            lambda_fields = r["fields"]["aws"]["lambda"]
            assert lambda_fields["request_id"] == "req-123"
            assert lambda_fields["function_arn"] == "arn:aws:lambda:us-east-1:123456789012:function:my-function"
            assert lambda_fields["version"] == "$LATEST"
            assert lambda_fields["tracing"]["span_id"] == "abc123"

        # platform.report - no lambda enrichment
        assert "lambda" not in results[3]["fields"]["aws"]

    def test_orphan_logs_get_error_marker(self) -> None:
        log_events = [
            {"id": "1", "timestamp": 1000, "message": _function_log_message("orphan")},
            {"id": "2", "timestamp": 1001, "message": _platform_start_message()},
        ]
        cw_event = _make_cw_event(log_events)
        results = _collect_results(cw_event)

        assert len(results) == 2

        # orphan - error marker
        lambda_fields = results[0]["fields"]["aws"]["lambda"]
        assert "_enrichment_error" in lambda_fields
        assert "no platform.start context" in lambda_fields["_enrichment_error"]

        # platform.start - no lambda enrichment
        assert "lambda" not in results[1]["fields"]["aws"]

    def test_platform_events_pass_through_unchanged(self) -> None:
        log_events = [
            {"id": "1", "timestamp": 1000, "message": _platform_start_message()},
            {"id": "2", "timestamp": 1001, "message": _platform_report_message()},
        ]
        cw_event = _make_cw_event(log_events)
        results = _collect_results(cw_event)

        assert len(results) == 2
        for r in results:
            assert "lambda" not in r["fields"]["aws"]
            assert "cloudwatch" in r["fields"]["aws"]

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

        # function log during invocation - enriched
        assert results[1]["fields"]["aws"]["lambda"]["request_id"] == "req-1"

        # function log after report - orphan error
        assert "_enrichment_error" in results[3]["fields"]["aws"]["lambda"]

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

        # first invocation function log
        assert results[1]["fields"]["aws"]["lambda"]["request_id"] == "req-1"

        # second invocation function log
        assert results[4]["fields"]["aws"]["lambda"]["request_id"] == "req-2"

    def test_plain_text_logs_no_enrichment_no_error(self) -> None:
        """Non-JSON messages should pass through without enrichment or error."""
        log_events = [
            {"id": "1", "timestamp": 1000, "message": "START RequestId: req-123"},
            {"id": "2", "timestamp": 1001, "message": "plain text log"},
        ]
        cw_event = _make_cw_event(log_events)
        results = _collect_results(cw_event)

        assert len(results) == 2
        # Plain text - _extract returns (None, None), so platform_type is None
        # but lambda_context is also None -> orphan error marker
        for r in results:
            assert "lambda" in r["fields"]["aws"]
            assert "_enrichment_error" in r["fields"]["aws"]["lambda"]

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
            assert "cloud" in r["fields"]
            assert r["fields"]["cloud"]["provider"] == "aws"
