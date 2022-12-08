from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext

def spark_context_creator():
    
    conf = (
        SparkConf()
        .setAppName("ETLPipeline")
        .setMaster("local")
        .set("spark.driver.extraClassPath", "c:/pyspark/*")
    )
   
    sc = SparkContext(conf=conf)
   
    return sc

sc = spark_context_creator()
#To avoid unncessary logs
sc.setLogLevel("WARN")

etl = SparkSession(sc)




