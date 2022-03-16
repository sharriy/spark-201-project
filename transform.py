from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param, Params, TypeConverters
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col

class get_age_from_dob(Transformer, HasInputCol, HasOutputCol, DefaultParamsReadable, DefaultParamsWritable):
    
    input_col = Param(Params._dummy(), "input_col", "input column name.", typeConverter=TypeConverters.toString)
    output_col = Param(Params._dummy(), "output_col", "output column name.", typeConverter=TypeConverters.toString)
  
    @keyword_only
    def __init__(self, input_col: str = "input", output_col: str = "output"):
        super(get_age_from_dob, self).__init__()
        self._setDefault(input_col=None, output_col=None)
        kwargs = self._input_kwargs
        self.set_params(**kwargs)

    @keyword_only
    def set_params(self, input_col: str = "input", output_col: str = "output"):
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def get_input_col(self):
        return self.getOrDefault(self.input_col)

    def get_output_col(self):
        return self.getOrDefault(self.output_col)

    def _transform(self, df: DataFrame):
        input_col = self.get_input_col()
        output_col = self.get_output_col()
        
        def transform_fn(data):    
            data = data.split("-")
            age = 2021 - int(data[0])
            return age
        transform_udf = F.udf(transform_fn, IntegerType())
        return df.withColumn(output_col, transform_udf(input_col))


class get_time(Transformer, HasInputCol, HasOutputCol, DefaultParamsReadable, DefaultParamsWritable):
    
    input_col = Param(Params._dummy(), "input_col", "input column name.", typeConverter=TypeConverters.toString)
    output_col = Param(Params._dummy(), "output_col", "output column name.", typeConverter=TypeConverters.toString)
  
    @keyword_only
    def __init__(self, input_col: str = "input", output_col: str = "output"):
        super(get_time, self).__init__()
        self._setDefault(input_col=None, output_col=None)
        kwargs = self._input_kwargs
        self.set_params(**kwargs)

    @keyword_only
    def set_params(self, input_col: str = "input", output_col: str = "output"):
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def get_input_col(self):
        return self.getOrDefault(self.input_col)

    def get_output_col(self):
        return self.getOrDefault(self.output_col)

    def _transform(self, df: DataFrame):
        input_col = self.get_input_col()
        output_col = self.get_output_col()
        
        def transform_fn(data):    
            data = data.split(" ")
            time = data[1].split(":")
            return int(time[0])
        transform_udf = F.udf(transform_fn, IntegerType())
        return df.withColumn(output_col, transform_udf(input_col))


class cast_int(Transformer, HasInputCol, HasOutputCol, DefaultParamsReadable, DefaultParamsWritable):
    
    input_col = Param(Params._dummy(), "input_col", "input column name.", typeConverter=TypeConverters.toString)
    output_col = Param(Params._dummy(), "output_col", "output column name.", typeConverter=TypeConverters.toString)
  
    @keyword_only
    def __init__(self, input_col: str = "input", output_col: str = "output"):
        super(cast_int, self).__init__()
        self._setDefault(input_col=None, output_col=None)
        kwargs = self._input_kwargs
        self.set_params(**kwargs)

    @keyword_only
    def set_params(self, input_col: str = "input", output_col: str = "output"):
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def get_input_col(self):
        return self.getOrDefault(self.input_col)

    def get_output_col(self):
        return self.getOrDefault(self.output_col)

    def _transform(self, df: DataFrame):
        input_col = self.get_input_col()
        output_col = self.get_output_col()
        
        def transform_fn(data):    
            return int(float(data))
        transform_udf = F.udf(transform_fn, IntegerType())
        return df.withColumn(output_col, transform_udf(input_col))