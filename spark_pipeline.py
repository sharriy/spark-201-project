import sys
sys.path.append("./utility_zip")
from pyspark.sql import SparkSession
from ingest import ingest_data
from transform import transform_data
from prediction import prediction

def run_pipeline(spark):
    try:
        print("run_pipeline method started. Running Pipeline....")
        df = ingest_data(spark)
        transformed_df = transform_data(spark,df)
        prediction(transformed_df)
        print("run_pipeline method ended.")
    except Exception as exp:
	print("An error occured while running the pipeline >"+str(exp))
    return

def create_spark_session():
    spark = SparkSession.builder\
	.appName("spark-201-project") \
	.config("spark.executor.instances","20") \
	.config("spark.sql.broadcastTimeout","36000") \
	.getOrCreate()
    return spark

def main():
    print("Application started.")
    spark = create_spark_session()
    print("Spark session created")
    run_pipeline(spark)
    print("Pipeline executed.")


if __name__ = '__main__':
    main()	