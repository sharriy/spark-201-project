def transform_data(spark,df):
    print("transform_data method started.")

    df = preprocess(spark,df)
    return df

def pre_year(data):    
    data = data.split("-")
    return data [0]
def pre_hour(data):
    data = data.split(":")
    return data [0]
def pre_age(data):
    age = 2021 - data
    return age
def pre_time(data):
    data = data.split(" ")
    return data[1]

def preprocess(spark,df):
    
    df = df.withColumn("amt", col("amt").cast("int"))
    df = df.withColumn("city_pop", col("city_pop").cast("int"))
    df = df.withColumn("year", pre_year(col("dob")).cast("int"))
    df = df.withColumn("age", pre_age(col("year")).cast("int"))
    df = df.withColumn("time_old", pre_time(col("trans_date_trans_time")))
    df = df.withColumn("time", pre_hour(col("time_old")).cast("int"))
    df = df.select(col("time"), col("amt"), col("city_pop"), col("age"))

    return df
