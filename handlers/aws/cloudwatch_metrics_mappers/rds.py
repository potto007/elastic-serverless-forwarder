# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from .base import BaseMapper


class RDSMapper(BaseMapper):
    def get_service_key(self) -> str:
        return "rds"
