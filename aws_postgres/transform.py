import json
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from sqlalchemy import create_engine
from config.spark_conf import etl


with open("config/config.json") as f:
    config = json.load(f)

 
target_driver = config["target_driver"]
target_db = config["target_db"]
server = config["server"]
pwd = os.environ["PGPASS"]
uid = os.environ["PGUID"]
target_url = f"jdbc:postgresql://{server}:5432/{target_db}?user={uid}&password={pwd}"


# decide where to transform data into warehouse


def transform_product():
    """
    Transform Product Table
    """
    df = (
        etl.read.format("jdbc")
        .option("url", target_url)
        .option("driver", target_driver)
        .option("user", uid)
        .option("password", pwd)
        .option("dbtable", "src_dimproduct")
        .load()
    )
    # cleaning table
    revised = df.select(
        "ProductKey",
        "ProductAlternateKey",
        "ProductSubcategoryKey",
        "WeightUnitMeasureCode",
        "SizeUnitMeasureCode",
        "EnglishProductName",
        "StandardCost",
        "FinishedGoodsFlag",
        "Color",
        "SafetyStockLevel",
        "ReorderPoint",
        "ListPrice",
        "Size",
        "SizeRange",
        "Weight",
        "DaysToManufacture",
        "ProductLine",
        "DealerPrice",
        "Class",
        "Style",
        "ModelName",
        "EnglishDescription",
        "StartDate",
        "EndDate",
        "Status",
    ).fillna(
        {
            "ProductSubcategoryKey": 0,
            "SizeUnitMeasureCode": "",
            "StandardCost": 0,
            "ListPrice": 0,
            "ProductLine": 0,
            "Class": "N/A",
            "Style": "N/A",
            "Size": "N/A",
            "ModelName": "N/A",
            "EnglishDescription": "N/A",
            "DealerPrice": 0,
            "Weight": 0,
            "EndDate":"N/A"
        }
    ).withColumnRenamed("EnglishDescription","Description").withColumnRenamed("EnglishProductName", "ProductName")
    #save data to table
    revised.write.format("jdbc").option("url", target_url).option(
        "driver", target_driver
    ).option("dbtable", "src_dimproducts").mode("overwrite").save()



def transform_ProductCategory():
    """
    Transform Product Category table
    """
    df = (
            etl.read.format("jdbc")
            .option("url", target_url)
            .option("driver", target_driver)
            .option("user", uid)
            .option("password", pwd)
            .option("dbtable", "src_DimProductCategory")
            .load()
        )
    # cleaning table
    revised = df.select(
        'ProductCategoryKey', 'ProductCategoryAlternateKey','EnglishProductCategoryName').withColumnRenamed("EnglishProductCategoryName", "ProductSubcategoryName")
            # .withColumn("isGraduated",col("isGraduated").cast(IntegerType()))
    # revised = revised.withColumn("ProductCategoryKey", revised.ProductCategoryKey.cast('float'))
    print("transforming stg_DimProductCategory")

    # save dataframe to table
    revised.write.format("jdbc").option("url", target_url).option(
        "driver", target_driver
    ).option("dbtable", "stg_DimProductCategory").mode("overwrite").save()
    print("Done transforming")

def transform_SubProductCategory():
    """
    Transform Product Sub category table
    """
    print("Loading data: stg_DimProductCategory")
    df = (
            etl.read.format("jdbc")
            .option("url", target_url)
            .option("driver", target_driver)
            .option("user", uid)
            .option("password", pwd)
            .option("dbtable", "src_DimSubProductCategory")
            .load()
        )
    # cleaning table
    revised = df.select('ProductSubcategoryKey','EnglishProductSubcategoryName', 'ProductSubcategoryAlternateKey','EnglishProductSubcategoryName', 'ProductCategoryKey').withColumnRenamed("EnglishProductSubcategoryName","ProductSubcategoryName")
    print("Transforming src_DimSubProductCategory")
    # save dataframe to table
    revised.write.format("jdbc").option("url", target_url).option(
        "driver", target_driver
    ).option("dbtable", "stg_DimSubProductCategory").mode("overwrite").save()
    print("Done")

def tranform_prd(): 
    """
    Merge tables product table on product category tables
    """
    print("Loading tables")
    # load all three tables
    p = (
            etl.read.format("jdbc")
            .option("url", target_url)\
            .option("driver", target_driver)
            .option("user", uid)
            .option("password", pwd)
            .option("dbtable", "stg_DimProduct")
            .load()
        )
    pc = (
            etl.read.format("jdbc")
            .option("url", target_url)
            .option("driver", target_driver)
            .option("user", uid)
            .option("password", pwd)
            .option("dbtable", "stg_DimProductCategory")
            .load()
        )

    ps = (
            etl.read.format("jdbc")
            .option("url", target_url)\
            .option("driver", target_driver)
            .option("user", uid)
            .option("password", pwd)
            .option("dbtable", "stg_DimProductSubcategory")
            .load()
        )
    # create enfine
    engine = create_engine(f'postgresql://{uid}:{pwd}@{server}:5432/AdventureWorks')
    print("Tranforming: merging dat")
    new = p.to_pandas_on_spark().merge(ps. to_pandas_on_spark(), on='ProductSubcategoryKey').merge(pc.to_pandas_on_spark(), on='ProductCategoryKey')
    print("done")
    # save dataframe to table
    # new.to_sql(f'prd_DimProductCategory', engine, if_exists='replace', index=False)
    new.write
        
