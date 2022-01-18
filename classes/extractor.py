import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from pyspark.sql import Row
import pyspark.sql.functions as F
import pyspark.sql.types as T

class Extractor:
	def __init__(self,file,output,spark):
		self.file = file
		self.output = output
		self.spark = spark
		self.df = spark.read.option("header",True).csv(file)

#extract Fuel Consumption and CO2 Emission
	def extract(self):
	    df_extracted = self.df.withColumn('extracted-value-ConsumptionTotalText',\
	     F.split(self.df['ConsumptionTotalText'], ' ').getItem(0))\
	       .withColumn('extracted-unit-ConsumptionTotalText', F.split(self.df['ConsumptionTotalText'], ' ').getItem(1))\
	        .withColumn('extracted-value-Co2EmissionText', F.split(self.df['Co2EmissionText'], ' ').getItem(0))\
	       .withColumn('extracted-unit-Co2EmissionText', F.split(self.df['Co2EmissionText'], ' ').getItem(1))

	    df_extracted.toPandas().to_csv(self.output,index=False)

