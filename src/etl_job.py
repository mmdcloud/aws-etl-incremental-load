import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Retrieve job parameters
args = getResolvedOptions(sys.argv, ["JOB_NAME", "TempDir", "aws_iam_role"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Load data from S3 (as a CSV example)
datasource0 = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://your-bucket/path/to/source-data/"]},
    format="csv",
    format_options={"withHeader": True}
)

# Optional: Apply mapping or transformations here

# Write to Redshift
glueContext.write_dynamic_frame.from_options(
    frame=datasource0,
    connection_type="redshift",
    connection_options={
        "database": "your_redshift_db",
        "dbtable": "your_target_table",
        "aws_iam_role": args["aws_iam_role"],
        "redshiftTmpDir": args["TempDir"]  # Glue uses this S3 path as staging area
    }
)
