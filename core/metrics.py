import os
import boto3


class Metrics:
def __init__(self, namespace: str, dimensions: list[dict] | None = None, enabled: bool = True):
self.enabled = enabled
if enabled:
self.cw = boto3.client("cloudwatch")
self.namespace = namespace
self.dimensions = dimensions or []


def put(self, name: str, value: float, unit: str = "Count"):
if not self.enabled:
return
self.cw.put_metric_data(
Namespace=self.namespace,
MetricData=[{
"MetricName": name,
"Dimensions": self.dimensions,
"Value": value,
"Unit": unit,
}]
)