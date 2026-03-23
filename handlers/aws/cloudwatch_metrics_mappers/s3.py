# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from .base import BaseMapper


class S3Mapper(BaseMapper):
    def get_service_key(self) -> str:
        return "s3"

    def get_data_stream_name(self) -> str:
        return "metrics-aws.s3_request-default"

    def get_dataset(self) -> str:
        return "aws.s3_request"

    def get_pipeline_name(self) -> str:
        return "metrics-aws.s3_request"
