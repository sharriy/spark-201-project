def ingest_data(spark,filepath):
    print("ingest_data method started.")
    df = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(filepath)
    return df
