from awsglue.context import GlueContext
from pyspark.sql import DataFrame


class IO:
def __init__(self, glue_ctx: GlueContext):
self.glue_ctx = glue_ctx
self.spark = glue_ctx.spark_session


def read(self, fmt: str, path: str, options: dict | None = None) -> DataFrame:
options = options or {}
reader = self.spark.read.format(fmt)
for k, v in options.items():
reader = reader.option(k, v)
return reader.load(path)


def write(self, df: DataFrame, fmt: str, path: str, mode: str = "errorifexists", partitionBy: list[str] | None = None, options: dict | None = None):
options = options or {}
writer = df.write.mode(mode).format(fmt)
if partitionBy:
writer = writer.partitionBy(*partitionBy)
for k, v in options.items():
writer = writer.option(k, v)
writer.save(path)