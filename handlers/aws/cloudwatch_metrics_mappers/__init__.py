# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from .base import BaseMapper
from .ec2 import EC2Mapper
from .elb import ELBMapper
from .lambda_ import LambdaMapper
from .rds import RDSMapper
from .s3 import S3Mapper


def get_mapper(namespace: str) -> BaseMapper:
    """Return the appropriate mapper for a CloudWatch namespace."""
    mapper = _NAMESPACE_MAPPERS.get(namespace)
    if mapper is not None:
        return mapper
    return BaseMapper()


_NAMESPACE_MAPPERS: dict[str, BaseMapper] = {
    "AWS/EC2": EC2Mapper(),
    "AWS/ApplicationELB": ELBMapper(),
    "AWS/RDS": RDSMapper(),
    "AWS/Lambda": LambdaMapper(),
    "AWS/S3": S3Mapper(),
}
