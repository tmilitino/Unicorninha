from pyspark.sql import SparkSession, Row
import pydeequ
from pyspark.sql.types import *

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
                    .addAnalyzer(Completeness("dropoff_datetime")) \
                    .addAnalyzer(Completeness("total_amount")) \
                    .run()

analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark_session, analysisResult)
analysisResult_df.show()

## erro
# from pydeequ.profiles import *

# result = ColumnProfilerRunner(spark) \
#     .onData(df) \
#     .run()

# for col, profile in result.profiles.items():
#     print(profile)

## erro
# from pydeequ.suggestions import *

# suggestionResult = ConstraintSuggestionRunner(spark) \
#              .onData(df) \
#              .addConstraintRule(DEFAULT()) \
#              .run()

# # Constraint Suggestions in JSON format
# print(suggestionResult)

## okay
# from pydeequ.checks import *
# from pydeequ.verification import *

# check = Check(spark, CheckLevel.Warning, "Review Check")

# checkResult = VerificationSuite(spark) \
#     .onData(df) \
#     .addCheck(
#         check.hasSize(lambda x: x >= 3) \
#         .hasMin("b", lambda x: x == 0) \
#         .isComplete("c")  \
#         .isUnique("a")  \
#         .isContainedIn("a", ["foo", "bar", "baz"]) \
#         .isNonNegative("b")) \
#     .run()

# checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
# checkResult_df.show()

## erro
# from pydeequ.repository import *
# from pydeequ.analyzers import *

# metrics_file = FileSystemMetricsRepository.helper_metrics_file(spark, 'metrics.json')
# repository = FileSystemMetricsRepository(spark, metrics_file)
# key_tags = {'tag': 'pydeequ hello world'}
# resultKey = ResultKey(spark, ResultKey.current_milli_time(), key_tags)

# analysisResult = AnalysisRunner(spark) \
#     .onData(df) \
#     .addAnalyzer(ApproxCountDistinct('b')) \
#     .useRepository(repository) \
#     .saveOrAppendResult(resultKey) \
#     .run()

spark_session.sparkContext._gateway.shutdown_callback_server()
spark_session.stop()
