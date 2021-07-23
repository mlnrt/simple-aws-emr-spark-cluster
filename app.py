#!/usr/bin/env python3
import json
from aws_cdk import core as cdk
from aws_emr_spark.aws_emr_spark_stack import AwsEmrSparkStack

app = cdk.App()

with open("./emr_steps/emr_steps_configuration.json") as steps_config:
    emr_steps = json.loads(steps_config.read())["steps"]

existing_data_buckets = [a_step["data_source_bucket"] for a_step in emr_steps if "data_source_bucket" in a_step]

emr_stack = AwsEmrSparkStack(
    scope=app,
    construct_id="AwsEmrSparkStack",
    naming_prefix="mads-siads-516",
    existing_data_buckets=existing_data_buckets)

for a_step in emr_steps:
    emr_stack.create_spark_step(a_step)

app.synth()

