import os
from pyspark.sql.functions import *
from aws_postgres import load, transform
import json
from config.spark_conf import etl


with open("config/config.json") as content:
    config = json.load(content)


# Credentials and db details
pwd = os.environ["PGPASS"]
uid = os.environ["PGUID"]
server = config["server"]
src_db = config["src_db"]
target_db = config["target_db"]
src_driver = config["src_driver"]
target_driver = config["target_driver"]


# source connection
src_url = f"jdbc:sqlserver://{server}:1433;databaseName={src_db};user={uid};password={pwd};encrypt=true;trustServerCertificate=true;"
# target connection
target_url = f"jdbc:postgresql://{server}:5432/{target_db}?user={uid}&password={pwd}"


def extract():
    """
    Extract data from source
    """
    SQL = """select  t.name as table_name from sys.tables t 
        where t.name in ('DimProduct','DimProductSubcategory','DimProductCategory','DimSalesTerritory','FactInternetSales') """

    try:
        data = (
            etl.read.format("jdbc")
            .options(driver=src_driver, user=uid, password=pwd, url=src_url, query=SQL)
            .load()
)
        # get table names
        data_collect = data.collect()
        # looping thorough each row of the dataframe
        for row in data_collect:
            # while looping through each
            # row printing the data of table_name
            print(row["table_name"])
            tbl_name = row["table_name"]
            df = (
                etl.read.format("jdbc")
                .option("driver", src_driver)
                .option("user", uid)
                .option("password", pwd)
                .option("url", src_url)\
                .option("dbtable", f"dbo.{tbl_name}") \
                .load()
            )
            # print(df.show(10))
            load.postgres(df, tbl_name, target_url, target_driver, pwd, uid)
            # aws.load_aws(df, tbl_name)
            print("Data loaded successfully")
    except Exception as e:
        print("Data extraction error: " + str(e))


if __name__ == "__main__":
    print("extracting")
    extract()

    # transform.tranform_prd()
    # transform.transform_ProductCategory()
    # transform.transform_SubProductCategory()
    # transform.tranform_prd()
