import json, os, io, yaml
import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from .logging import Logger
from .metrics import Metrics
from .io import IO
from .dq import DataQuality


class BaseJob:
ARGUMENTS = [
"JOB_NAME",
"config_s3_uri", # optional; if absent falls back to conf/config.yaml in the bundle
"env", # dev|uat|prod overrides
]


def __init__(self):
args = getResolvedOptions(sys.argv, [a for a in self.ARGUMENTS if a])
self.sc = SparkContext()
self.glue_ctx = GlueContext(self.sc)
self.spark = self.glue_ctx.spark_session
self.job = Job(self.glue_ctx)
self.job.init(args["JOB_NAME"], {})


# Load config (S3 or packaged file)
self.conf = self._load_config(args.get("config_s3_uri"))
if args.get("env"):
self.conf["env"] = args["env"]


self.log = Logger({"job": args["JOB_NAME"], "env": self.conf.get("env", "dev")})
m = self.conf.get("metrics", {})
self.metrics = Metrics(m.get("namespace", "GlueFramework"), m.get("dimensions", []), m.get("enabled", True))
self.io = IO(self.glue_ctx)
dq_cfg = self.conf.get("dq", {"enabled": False})
self.dq = DataQuality(dq_cfg.get("enabled", False), dq_cfg.get("ruleset_s3_uri"))


rp = self.conf.get("runtime", {})
if rp.get("shuffle_partitions"):
self.spark.conf.set("spark.sql.shuffle.partitions", rp["shuffle_partitions"])


def _load_config(self, s3_uri: str | None):
if not s3_uri:
# local packaged file
with open("conf/config.yaml", "r", encoding="utf-8") as fh:
return yaml.safe_load(fh)
# read from S3
s3 = boto3.client("s3")
bucket_key = s3_uri.replace("s3://", "", 1)
bucket = bucket_key.split("/", 1)[0]
key = bucket_key.split("/", 1)[1]
obj = s3.get_object(Bucket=bucket, Key=key)
return yaml.safe_load(obj["Body"].read())


# hooks to override
def extract(self):
raise NotImplementedError


def transform(self, df):
return df


def load(self, df):
raise NotImplementedError


def run(self):
try:
self.log.info("Job started")
df = self.extract()
dq = self.dq.run(df)
self.log.info("DQ result", result=dq)
tdf = self.transform(df)
self.load(tdf)
self.metrics.put("RowsProcessed", tdf.count())
self.log.info("Job finished")
self.job.commit()
except Exception as e:
self.log.error("Job failed", error=str(e))
self.metrics.put("JobFailure", 1)
raise