import sys
from core.base import BaseJob


class TitanicIngest(BaseJob):
def extract(self):
cfg = self.conf["io"]["input"]
return self.io.read(cfg["format"], cfg["path"], cfg.get("options"))


def transform(self, df):
# Example: trim columns and cast a field if present
cols = [c.strip() for c in df.columns]
df = df.toDF(*cols)
if "Age" in df.columns:
from pyspark.sql.functions import col
df = df.withColumn("Age", col("Age").cast("double"))
return df


def load(self, df):
ocfg = self.conf["io"]["output"]
self.io.write(
df,
fmt=ocfg.get("format", "parquet"),
path=ocfg["path"],
mode=ocfg.get("mode", "errorifexists"),
partitionBy=ocfg.get("partitionBy", []),
options=ocfg.get("options", {}),
)


if __name__ == "__main__":
TitanicIngest().run()