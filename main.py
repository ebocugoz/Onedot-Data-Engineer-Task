
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from pyspark.sql import Row
import pyspark.sql.functions as F
import pyspark.sql.types as T

from classes.preprocessor import *
from classes.normalizer import *
from classes.extractor import *
from classes.integrator import *

spark = SparkSession.builder.appName('normalizer').getOrCreate()

#Preprocessing
print("Preprocessing...")
prp = Preprocessor("supplier_car.json",'pre-processing.csv',spark)
prp.preprocess()
print("Preprocessing done")


nrm = Normalizer('pre-processing.csv','normalisation.csv',spark)
nrm.normalize()

extr = Extractor('normalisation.csv','extraction.csv',spark)
extr.extract()

intg = Integrator('extraction.csv','integration.csv',spark)
intg.integrate()