import json
import boto3, io
import logging



def aws(df, tbl,access_key, secret_key):
    """
    Load data to aws
    """

    try:
        rows_imported = 0
        print(f"importing rows {rows_imported + df.count()}... for table {tbl}")
        # save to s3
        upload_file_bucket = "my-s3-bucket-9090009"
        upload_file_key = "public/" + str(tbl) + f"/{str(tbl)}"
        filepath = upload_file_key + ".csv"
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",
        )
        with io.StringIO() as csv_buffer:
            df.toPandas().to_csv(csv_buffer)

            response = s3_client.put_object(
                Bucket=upload_file_bucket, Key=filepath, Body=csv_buffer.getvalue()
            )

            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                logging.debug(f"Successful S3 put_object response. Status - {status}")
            else:
                logging.debug(f"Unsuccessful S3 put_object response. Status - {status}")
                print("Data imported successful")
    except Exception as e:
        print("Data load error: " + str(e))


def postgres(df, tbl,uid, pwd, target_url, target_driver):
    """
    Load data to postgres
    """
    try:
        print(
            f"importing {df.count()}... for table {tbl}"
        )
        df.write.mode("overwrite").format("jdbc").option("url", target_url).option(
            "user", uid
        ).option("password", pwd).option("driver", target_driver).option(
            "dbtable", "src_" + tbl)
        print("Data imported successful")
    except Exception as e:
        print("Data load error: " + str(e))

