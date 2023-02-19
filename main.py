import pandas as pd 


df = pd.read_csv('/home/milithus/git/DataQualityBenchmarks/sample/taxi_yellow_2019_01_without_errors.csv')
df.to_csv("./teste.csv", sep="|", decimal=",")




#   "vendor_id"
#   "pickup_datetime"
#   "dropoff_datetime"
#   "passenger_count"
#   "rate_code_id"
#   "store_and_fwd_flag"
#   "pickup_location_id"
#   "dropoff_location_id"
#   "payment_type"
#   "fare_amount"
#   "extra"
#   "mta_tax"
#   "tip_amount"
#   "tolls_amount"
#   "improvement_surcharge"
#   "total_amount"
#   "congestion_surcharge"
