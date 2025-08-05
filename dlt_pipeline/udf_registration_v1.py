# 03_udf_registration.py (attach as first library notebook)
import mlflow.pyfunc
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Load champion model & register UDF
champion_uri = "models:/{CATALOG_NAME}.{SCHEMA_NAME}.customer_anomaly_detector@champion" # replace with your own model path
predict_udf = mlflow.pyfunc.spark_udf(spark, champion_uri)
spark.udf.register("predict_customer_anomaly", predict_udf)
