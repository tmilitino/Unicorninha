from pyspark.sql import SparkSession, Row
import pydeequ
from pyspark.sql.types import *
from pydeequ.checks import *
from pydeequ.verification import *
from pydeequ.repository import *
from pydeequ.analyzers import *


schema = StructType([
    StructField("vendor_id", IntegerType()),
    StructField("pickup_datetime", DateType()),
    StructField("dropoff_datetime", DateType()),
    StructField("passenger_count", IntegerType()),
    StructField("trip_distance", DecimalType()),
    StructField("rate_code_id", IntegerType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("pickup_location_id", IntegerType()),
    StructField("dropoff_location_id", IntegerType()),
    StructField("payment_type", DecimalType()),
    StructField("fare_amount", DecimalType()),
    StructField("extra", DecimalType()),
    StructField("mta_tax", DecimalType()),
    StructField("tip_amount", DecimalType()),
    StructField("tolls_amount", DecimalType()),
    StructField("improvement_surcharge", DecimalType()),
    StructField("total_amount", DecimalType()),
    StructField("congestion_surcharge", DecimalType()),])

spark_session = SparkSession \
    .builder \
    .config("spark.jars.packages", pydeequ.deequ_maven_coord)\
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord)\
    .appName("soda core scan") \
    .getOrCreate()
 
df = spark_session.read.csv("./sample/taxi_yellow_2019_02_without_errors.csv", schema=schema, header=True)

# okay
from pydeequ.analyzers import *

analysisResult = AnalysisRunner(spark_session) \
                    .onData(df) \
                    .addAnalyzer(Size()) \
                    .addAnalyzer(Completeness("vendor_id")) \
                    .addAnalyzer(Completeness("store_and_fwd_flag")) \
                    .addAnalyzer(Completeness("dropoff_datetime"))\
                    .addAnalyzer(Entropy("vendor_id"))\
                    .addAnalyzer(Mean("total_amount"))\
                    .addAnalyzer(Maximum("total_amount"))\
                    .addAnalyzer(Entropy("rate_code_id"))\
                    .addAnalyzer(Entropy("payment_type"))\
                    .run()

analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark_session, analysisResult)
analysisResult_df.show(100)


# okay

check = Check(spark_session, CheckLevel.Warning, "Review Check")
metrics_file = FileSystemMetricsRepository.helper_metrics_file(spark_session, 'metrics.json')
repository = FileSystemMetricsRepository(spark_session, metrics_file)
key_tags = {'tag': 'pydeequ hello world'}
resultKey = ResultKey(spark_session, ResultKey.current_milli_time(), key_tags)

checkResult = VerificationSuite(spark_session) \
    .onData(df) \
    .addCheck(
        check.hasSize(lambda x: x >= 3) \
        .hasMin("congestion_surcharge", lambda x: x == 0) \
        .isComplete("vendor_id")  \
        .isUnique("dropoff_datetime")  \
        .isContainedIn("store_and_fwd_flag", ["Y", "N"]) \
        .isContainedIn("rate_code_id", ['1','2','3','4','5']) \
        .isNonNegative("dropoff_location_id") \
        .isNonNegative("trip_distance")) \
  .useRepository(repository)\
  .saveOrAppendResult(resultKey)\
    .run()

checkResult_df = VerificationResult.checkResultsAsDataFrame(spark_session, checkResult)
checkResult_df.show()

spark_session.sparkContext._gateway.shutdown_callback_server()
spark_session.stop()
