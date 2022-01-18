
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from pyspark.sql import Row
import pyspark.sql.functions as F
import pyspark.sql.types as T

class Preprocessor:
	def __init__(self,file,output,spark):
		self.file = file
		self.output = output
		self.spark = spark
		self.df = self.spark.read.json(file)

	def preprocess(self):
		attribute_names = self.df.select('Attribute Names').distinct().collect()
		attribute_names = [attr[0] for attr in attribute_names]
		#Try to get text from the attribute if such attribute is missing return None
		def get_text_helper(x,text):
		    try:
		        return x.key_value[text]
		    except:
		        return None 
	    #Store the attribute names and values as a dictionary so we have 1 row per item
		non_attr_columns = self.df.columns[2:-1]
		df_dict = self.df.groupBy(non_attr_columns).agg(
		F.map_from_entries(
			F.collect_list(
		    F.struct("Attribute Names", "Attribute Values"))).alias("key_value"))

	    #get attribute names
		attribute_names = self.df.select('Attribute Names').distinct().collect()
		attribute_names = [attr[0] for attr in attribute_names]

		df_preprocessed = df_dict.rdd.map\
		(lambda x:((x.ID,x.MakeText,x.ModelText,x.ModelTypeText,x.TypeName,x.TypeNameFull)+\
		                                        tuple([get_text_helper(x,attr) for attr in attribute_names])) )\
		.toDF(non_attr_columns+attribute_names)
		df_preprocessed.toPandas().to_csv(self.output,index=False)

"""	df_preprocessed = pre_process(self.df)
	df_preprocessed.toPandas().to_csv('pre-processing.csv',index=False)"""