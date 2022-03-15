#set path
export PYSPARK_DRIVER_PYTHON=./environment/bin/python
export PYSPARK_PYTHON=./environment/bin/python

spark-submit \
--master local \
--deploy-mode client \
--py-files modules/pipeline/utility.zip \
spark_pipeline.py