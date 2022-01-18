import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from pyspark.sql import Row
import pyspark.sql.functions as F
import pyspark.sql.types as T


class Integrator: 

	def __init__(self,file,output,spark):
		self.file = file
		self.output = output
		self.spark = spark

		self.df = self.spark.read.option("header",True).csv(self.file)
		#Create column name mappings
		self.integration_map = {"BodyColorText":"color","MakeText":"make","ModelText":"model","TypeName":"model_variant","City":"city",
					"FirstRegYear":"manufacture_year","FirstRegMonth":"manufacture_month","ConditionTypeText":"condition",
					"Km":"mileage","BodyTypeText":"carType","extracted-unit-ConsumptionTotalText":"fuel_consumption_unit"}
		#Remove columns that are not in target
		self.columns_to_remove = ("ID","ModelTypeText","TypeNameFull","ConsumptionRatingText","Doors","InteriorColorText","Co2EmissionText"
			,"Seats","ConsumptionTotalText","Ccm","Properties","DriveTypeText","TransmissionTypeText","Hp","FuelTypeText")
		#There are some columns we need to add, we can assume some of their values
		self.columns_add_map = {"currency":F.lit("CHF"), "country":F.lit("CH"), 'mileage_unit':F.lit("kilometer"), 
		'type':F.lit('car'), "price_on_request":F.lit(None).cast(T.StringType()), 
		"drive":F.lit(None).cast(T.StringType()), "zip":F.lit(None).cast(T.StringType())}


	def integrate(self):

		# change column names that can be mapped to the target table
		def change_column_names(df,integration_map):
			for key in integration_map.keys():
				df = df.withColumnRenamed(key,integration_map[key])
			return df

		#remove some columns that are not in the target table
		def remove_columns(df,columns_to_remove):
			return df.drop(*columns_to_remove)

		#add columns that are in the target df but not in our df
		def add_columns(df,columns_add_map):
			for key in columns_add_map.keys():
				df = df.withColumn(key,columns_add_map[key])
			return df
		#arrange columns according to the target df
		def arrange_columns(df):
			targe_columns = ['carType', 'color', 'condition', 'currency', 'drive', 'city', 'country',
		       'make', 'manufacture_year', 'mileage', 'mileage_unit', 'model',
		       'model_variant', 'price_on_request', 'type', 'zip', 'manufacture_month',
		       'fuel_consumption_unit']
			return df.select(targe_columns)
		df_integrated = change_column_names(self.df,self.integration_map)
		df_integrated = remove_columns(df_integrated,self.columns_to_remove)
		df_integrated = add_columns(df_integrated,self.columns_add_map)
		df_integrated = arrange_columns(df_integrated)

		df_integrated.toPandas().to_csv(self.output,index=False)



"""#Create column name mappings
integration_map = {"BodyColorText":"color","MakeText":"make","ModelText":"model","TypeName":"model_variant","City":"city",
					"FirstRegYear":"manufacture_year","FirstRegMonth":"manufacture_month","ConditionTypeText":"condition",
					"Km":"mileage","BodyTypeText":"carType","extracted-unit-ConsumptionTotalText":"fuel_consumption_unit"}
#There are some columns we need to add, we can assume some of their values
columns_add_map = {"currency":F.lit("CHF"), "country":F.lit("CH"), 'mileage_unit':F.lit("kilometer"),
 'type':F.lit('car'), "price_on_request":F.lit(None).cast(T.StringType()), 
 "drive":F.lit(None).cast(T.StringType()), "zip":F.lit(None).cast(T.StringType())}



columns_to_remove = ("ID","ModelTypeText","TypeNameFull","ConsumptionRatingText","Doors","InteriorColorText","Co2EmissionText"
	,"Seats","ConsumptionTotalText","Ccm","Properties","DriveTypeText","TransmissionTypeText","Hp","FuelTypeText")

df_integrated= integrate(df_extracted,integration_map,columns_to_remove,columns_add_map)     
df_integrated.toPandas().to_csv('integration.csv',index=False)

"""

