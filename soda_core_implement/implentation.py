from pyspark.sql import SparkSession
from soda.scan import Scan

table_name = "test_soda"

spark_session = SparkSession \
    .builder \
    .appName("soda core scan") \
    .getOrCreate()
 
df = spark_session.read.csv("./sample/taxi_yellow_2019_01_without_errors.csv")
df.createOrReplaceTempView(f"{table_name}")

scan = Scan()

scan.   set_scan_definition_name('SODA_CORE_SCAN')
scan.set_data_source_name("spark_df")

scan.add_configuration_yaml_file(file_path="./configuration.yaml")
scan.add_spark_session(spark_session)

# If you defined checks in a file accessible via Spark, you can use the scan.add_sodacl_yaml_file method to retrieve the checks
scan.add_sodacl_yaml_file("./check.yaml")

# Execute the scan
##################
scan.execute()

# Set logs to verbose mode, equivalent to CLI -V option
##################
scan.set_verbose(True)

# Set scan definition name, equivalent to CLI -s option;
# see Tips and best practices below
##################
scan.set_scan_definition_name("SODA_CORE_SCAN")


# Inspect the scan result
#########################
print(scan.get_scan_results())

# Inspect the scan logs
#######################
scan.get_logs_text()

# Typical log inspection
##################
scan.assert_no_error_logs()
scan.assert_no_checks_fail()

# Advanced methods to inspect scan execution logs 
#################################################
scan.has_error_logs()
scan.get_error_logs_text()

# Advanced methods to review check results details
########################################
scan.get_checks_fail()
scan.has_check_fails()
scan.get_checks_fail_text()
scan.assert_no_checks_warn_or_fail()
scan.get_checks_warn_or_fail()
scan.has_checks_warn_or_fail()
scan.get_checks_warn_or_fail_text()
scan.get_all_checks_text()