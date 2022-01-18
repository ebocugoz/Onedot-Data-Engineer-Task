
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from pyspark.sql import Row
import pyspark.sql.functions as F
import pyspark.sql.types as T

#Normalize values regarding to target df

class Normalizer:
	def __init__(self,file,output,spark):
		self.file = file
		self.output = output
		self.spark = spark
		self.df = self.spark.read.option("header",True).csv(file)


	def normalize(self):
	    

	    def make_normalizer(text):
	    	try:
	    		#If bmw or vw make all upper
		    	if text.lower() == 'bmw' or text.lower() == 'vw':
		    		return text.upper()
		    	else: # else make first letters upper
		        	return text.title()
	    	except:
	    		return None
	    make_normalizer_udf = F.udf(lambda x: make_normalizer(x),T.StringType())

	    #mal colors according to the target df
	    def color_normalizer(text):
	        color_dict = {'schwarz':'Black','orange':'Orange',"bordeaux":"Other","grün":'Green',"braun":"Brown",'rot':"Red",\
	                  'grau':"Gray", 'gelb':"Yellow",'weiss':"White",'gold':"Gold","violett":"Purple","beige":"Beige",\
	                   "anthrazit":"Other" , 'blau':"Blue" , 'rot':"Red" ,"silber":"Silver" }
	        try:
	        	return color_dict[text.split(" ")[0]]
	        except:
	        	return 'Other'
	    color_normalizer_udf = F.udf(lambda x: color_normalizer(x),T.StringType())

	    #It is possible to normalize body type however it is not complete
	    def body_type_normalizer(text):
	    	body_dict = {'SUV / Geländewagen':"SUV",'Cabriolet':'Convertible / Roadster','Coupé':'Coupé'}
	    	if text is None:
	    		return None
	    	try:
	    		return body_dict[text]
	    	except:
	    		return "Other"
	    body_type_normalizer_udf = F.udf(lambda x: body_type_normalizer(x),T.StringType())
	    

	    df_normalized = self.df.withColumn("MakeText",make_normalizer_udf("MakeText")).\
	    withColumn("BodyColorText",color_normalizer_udf("BodyColorText")).\
	    withColumn("BodyTypeText",body_type_normalizer_udf("BodyTypeText")).\
	    withColumn("ModelText",make_normalizer_udf("ModelText"))

	    df_normalized.toPandas().to_csv(self.output,index=False)

		


