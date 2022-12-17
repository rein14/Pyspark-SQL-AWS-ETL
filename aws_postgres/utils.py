from dataclasses import dataclass
from typing import Optional
import json

with open("./config/config.json") as file:
    config = json.load(file)


@dataclass
class AwsConnection:
    aws_access_key: Optional[str]
    aws_secret_key: Optional[str]
    region_name: Optional[str]


def get_aws() -> AwsConnection:
    return AwsConnection(
        aws_access_key = config["access_key"],
        aws_secret_key = config["secret_access"],
        region_name = config["region_name"]
    )
